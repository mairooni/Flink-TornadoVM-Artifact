/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.asm.AsmClassLoader;
import org.apache.flink.api.asm.MiddleReduce;
import org.apache.flink.api.asm.TornadoReduce;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.hash.InPlaceMutableHashTable;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.apache.flink.runtime.tornadovm.DataTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.examineTypeInfoForFlinkUDFs;

/**
 * Chained version of ReduceCombineDriver.
 */
public class ChainedReduceCombineDriver<T> extends ChainedDriver<T, T> {

	private static final Logger LOG = LoggerFactory.getLogger(ChainedReduceCombineDriver.class);

	/** Fix length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;


	private AbstractInvokable parent;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;

	private ReduceFunction<T> reducer;

	private DriverStrategy strategy;

	private InMemorySorter<T> sorter;

	private QuickSort sortAlgo = new QuickSort();

	private InPlaceMutableHashTable<T> table;

	private InPlaceMutableHashTable<T>.ReduceFacade reduceFacade;

	private List<MemorySegment> memory;

	private volatile boolean running;

	private boolean tornado = Boolean.parseBoolean(System.getProperty("tornado", "false"));
	private boolean flinkReductions = Boolean.parseBoolean(System.getProperty("flinkReductions", "false"));
	// ------------------------------------------------------------------------

	@Override
	public Function getStub() {
		return reducer;
	}

	@Override
	public String getTaskName() {
		return taskName;
	}

	@Override
	public void setup(AbstractInvokable parent) {
		this.parent = parent;
		running = true;

		strategy = config.getDriverStrategy();

		reducer = BatchTask.instantiateUserCode(config, userCodeClassLoader, ReduceFunction.class);
		FunctionUtils.setFunctionRuntimeContext(reducer, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = config.getStubParameters();
		BatchTask.openUserCode(reducer, stubConfig);

		// instantiate the serializer / comparator
		serializer = config.<T>getInputSerializer(0, userCodeClassLoader).getSerializer();
		comparator = config.<T>getDriverComparator(0, userCodeClassLoader).createComparator();

		MemoryManager memManager = parent.getEnvironment().getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(config.getRelativeMemoryDriver());
		memory = memManager.allocatePages(parent, numMemoryPages);

		LOG.debug("ChainedReduceCombineDriver object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");

		switch (strategy) {
			case SORTED_PARTIAL_REDUCE:
				// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
				if (comparator.supportsSerializationWithKeyNormalization() &&
					serializer.getLength() > 0 && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING) {
					sorter = new FixedLengthRecordSorter<T>(serializer, comparator.duplicate(), memory);
				} else {
					sorter = new NormalizedKeySorter<T>(serializer, comparator.duplicate(), memory);
				}
				break;
			case HASHED_PARTIAL_REDUCE:
				table = new InPlaceMutableHashTable<T>(serializer, comparator, memory);
				table.open();
				reduceFacade = table.new ReduceFacade(reducer, outputCollector, objectReuseEnabled);
				break;
		}
	}

	@Override
	public void collect(T record) {
		try {
			// if we are running Flink-Tornado KMeans, the record value will be null in order to "fake" execution
			if (tornado && !flinkReductions) {
				try {
					switch (strategy) {
						case SORTED_PARTIAL_REDUCE:
							collectSortedTornadoVM(record);
							break;
						case HASHED_PARTIAL_REDUCE:
							collectHashed(null);
							break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				switch (strategy) {
					case SORTED_PARTIAL_REDUCE:
						collectSorted(record);
						break;
					case HASHED_PARTIAL_REDUCE:
						collectHashed(record);
						break;
				}
			}
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(taskName, ex);
		}
	}

	private void collectSortedTornadoVM(T record) throws Exception {
		// do the actual sorting, combining, and data writing
		if (!sorter.write(record)) {
			// it didn't succeed; sorter is full

			// do the actual sorting, combining, and data writing
			//sortAndCombineTornadoVM();
			sortAlgo.sort(sorter);
			sorter.reset();

			// write the value again
			if (!sorter.write(record)) {
				throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
			}
		}
	}

	private void collectSorted(T record) throws Exception {
		// try writing to the sorter first
		if (!sorter.write(record)) {
			// it didn't succeed; sorter is full

			// do the actual sorting, combining, and data writing
			sortAndCombine();
			sorter.reset();

			// write the value again
			if (!sorter.write(record)) {
				throw new IOException("Cannot write record to fresh sort buffer. Record too large.");
			}
		}
	}

	private void collectHashed(T record) throws Exception {
		try {
			reduceFacade.updateTableEntryWithReduce(record);
		} catch (EOFException ex) {
			// the table has run out of memory
			reduceFacade.emitAndReset();
			// try again
			reduceFacade.updateTableEntryWithReduce(record);
		}
	}

	private void sortAndCombineTornadoVM() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			//sortAlgo.sort(sorter);

			final TypeSerializer<T> serializer = this.serializer;
			final MutableObjectIterator<T> input = sorter.getIterator();

			//long compStart = System.currentTimeMillis();
			byte[] b;
			int key;
			FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
			AccelerationData acres = new AccelerationData();
			int returnSize = 0;

			byte[] typeB = new byte[0];
			typeB = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTypeInfo(typeB);
			ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
			ObjectInputStream ins = new ObjectInputStream(bObj);
			HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
			ins.close();
			bObj.close();

			for (String name : typeInf.keySet()) {
				if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
					returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
					break;
				}
			}

//			for (String name : AbstractInvokable.typeInfo.keySet()) {
//				if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
//					returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
//					break;
//				}
//			}

			boolean padding = false;
			int actualSize = 0;
			int numOfFields = 0;

			for (int size : fct0.getFieldSizes()) {
				actualSize += size;
				numOfFields++;
			}

			for (int i = 0; i < fct0.getFieldSizes().size(); i++) {
				for (int j = i + 1; j < fct0.getFieldSizes().size(); j++) {
					if (!fct0.getFieldSizes().get(i).equals(fct0.getFieldSizes().get(j))) {
						padding = true;
						break;
					}
				}
			}

			//long compEnd = System.currentTimeMillis();
			//System.out.println("**** Function GROUPBY set compiler info and extract typeinfo: " + (compEnd - compStart));
			//long dataStart = System.currentTimeMillis();
			if (input instanceof NormalizedKeySorter.NormalizedKeySorterIterator) {
				b = ((NormalizedKeySorter.NormalizedKeySorterIterator) input).getBytes(actualSize);
				key = ((NormalizedKeySorter) sorter).getTupleKey();
			} else {
				return;
			}
			//long dataEnd = System.currentTimeMillis();
			//System.out.println("**** Function GROUPBY get data from sorter: " + (dataEnd - dataStart));
			// Tornado format of data
			//long tornadoDataStart = System.currentTimeMillis();
			DataTransformation dtrans = new DataTransformation();
			AccelerationData acdata = dtrans.generateInputAccelerationData(b, serializer, null, false, false, false, false);
			//long tornadoDataEnd = System.currentTimeMillis();
			byte[] bytes = acdata.getRawData();
			//System.out.println("**** Function GROUPBY make data Tornado-Friendly: " + (tornadoDataEnd - tornadoDataStart));
			//long groupsStart = System.currentTimeMillis();

			int sizeOfTuple;
			int keyBytes;
			if (padding) {
				sizeOfTuple = 8 * numOfFields;
				keyBytes = 8;
			} else {
				sizeOfTuple = actualSize;
				keyBytes = fct0.getFieldSizes().get(key);
			}

			int	keyOffset = key * (sizeOfTuple - keyBytes); // key * (previous field sizes)

			ArrayList<int[]> groupsB = new ArrayList<>();
			int j = 0;


			while (true) {
				if (groupsB.isEmpty()) {
					BinaryGroupIndexBytes(bytes, 0, 0, bytes.length - sizeOfTuple, sizeOfTuple, keyBytes, keyOffset, groupsB);
				} else {
					int[] indexes = groupsB.get(j);
					BinaryGroupIndexBytes(bytes, indexes[1] + sizeOfTuple, indexes[1] + sizeOfTuple, bytes.length - sizeOfTuple, sizeOfTuple, keyBytes, keyOffset, groupsB);
					j++;
				}
				if (groupsB.get(j)[1] + sizeOfTuple == bytes.length) break;
			}

			//long groupsEnd = System.currentTimeMillis();
			//System.out.println("**** Function GROUPBY identify groups: " + (groupsEnd - groupsStart));
			long totalRedTime = 0;
			long totalPR = 0;
			long totalSplit = 0;
			byte[] totalBytes = new byte[groupsB.size() * sizeOfTuple];
			int currpos = 0;
			for (int[] pos : groupsB) {
				// for each group
				// get the array
				//long splitStart = System.currentTimeMillis();
				byte[] group = Arrays.copyOfRange(bytes, pos[0], pos[1] + sizeOfTuple);
				int numOfElements = group.length / sizeOfTuple;
				if (!isPowerOfTwo(numOfElements)) {
					double log = Math.log(numOfElements) / Math.log(2);
					int exp = (int) log;
					numOfElements = (int) Math.pow(2, exp + 1);
				}
				//long splitEnd = System.currentTimeMillis();
				//totalSplit += (splitEnd - splitStart);
				// reduction
				int size; // = inputByteData.length;
				if (numOfElements <= 256) {
					size = 2;
				} else {
					size = numOfElements / 256 + 1;
				}
				size *= 4;

				byte[] outBytes = new byte[size * returnSize];
				String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);//System.getProperty("user.dir");
				//System.out.println(path + "/build-target/pre-compiled/prebuilt-centroidAccum.cl");
				//FlinkData f = new FlinkData(true, group, outBytes);
				Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
				byte[] def = new byte[0];
				byte[] bytesRsk = conf.getSkeletonReduceBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleReduce mdr = loader.loadClassRed("org.apache.flink.api.asm.ReduceASMSkeleton", bytesRsk);
				TornadoReduce rsk = new TornadoReduce(mdr);
				//TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(2); //TornadoRuntime.getTornadoRuntime().getDefaultDevice();
				// @formatter:off
//				TaskSchedule ts = new TaskSchedule("s2")
//					.flinkInfo(f)
//					.prebuiltTask("t2",
//						"reduce",
//						"./pre-compiled/prebuilt-centroidAccum.cl",
//						//"./pre-compiled/prebuilt-centroidAccum.cl",
//						new Object[] { group, outBytes },
//						new Access[] { Access.READ, Access.WRITE },
//						defaultDevice,
//						new int[] { numOfElements })
//					.streamOut(outBytes);
				Tuple3[] in = new Tuple3[numOfElements];
				Tuple3[] out = new Tuple3[1];
				FlinkData f = new FlinkData(group, outBytes);
				WorkerGrid worker = new WorkerGrid1D(numOfElements);
				GridScheduler gridScheduler = new GridScheduler("s2.t2", worker);

				TaskSchedule ts = new TaskSchedule("s2")
					.flinkCompilerData(fct0)
					.flinkInfo(f)
					.task("t2", rsk::reduce, in, out)
					.streamOut(outBytes);
					//.execute();

				//long startRed = System.currentTimeMillis();
				worker.setGlobalWork(numOfElements, 1, 1);
				ts.execute(gridScheduler);
				//ts.execute();
				//long endRed = System.currentTimeMillis();
				//totalRedTime += (endRed - startRed);

				//long PRstart = System.currentTimeMillis();
				byte[] total = new byte[returnSize];
				for (int i = 0; i < keyBytes; i++) {
					total[i] = outBytes[i];
				}
				for (int i = 1; i < 4; i++) {
					addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true, padding);
				}
				//long PRend = System.currentTimeMillis();
				//totalPR += (PRend - PRstart);
				// collect
				int q = 0;
				for (int k = currpos; k < sizeOfTuple + currpos; k++) {
					totalBytes[k] = total[q];
					q++;
				}
				currpos += sizeOfTuple;

			}

			materializeTornadoBuffers("org/apache/flink/api/java/tuple/Tuple3", fct0, totalBytes);
			//System.out.println("**** Function CentroidAccumulator split dataset: " + totalSplit);
			//System.out.println("**** Function CentroidAccumulator execution time: " + totalRedTime);
			//System.out.println("**** Function CentroidAccumulator collect Partial Results: " + totalPR);

		}
	}

	public static boolean isPowerOfTwo(int inputSize) {
		boolean isPow2 = (int) (Math.ceil((Math.log(inputSize) / Math.log(2)))) == (int) (Math.floor(((Math.log(inputSize) / Math.log(2)))));
		return isPow2;
	}

	void materializeTornadoBuffers (String type, FlinkCompilerInfo fct, byte[] bytes) {
		byte[] out = bytes;

		if (type.equals("java/lang/Integer")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Integer in = bf.getInt(i);
				outputCollector.collect((T) in);
				//records.add((OT) in);
			}
		} else if (type.equals("java/lang/Double")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Double d = bf.getDouble(i);
				outputCollector.collect((T) d);
				//records.add((OT) d);
			}
		} else if (type.equals("java/lang/Float")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Float f = bf.getFloat(i);
				outputCollector.collect((T) f);
				//records.add((OT) f);
			}
		} else if (type.equals("java/lang/Long")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Long l = bf.getLong(i);
				outputCollector.collect((T) l);
				//records.add((OT) l);
			}
		} else if (type.equals("org/apache/flink/api/java/tuple/Tuple2")) {
			int sizeOftuple = 0;
			boolean padding = false;
			for (int i = 0; i < fct.getFieldSizesRet().size(); i++) {
				for (int j = i + 1; j < fct.getFieldSizesRet().size(); j++) {
					if (fct.getFieldSizesRet().get(i) != fct.getFieldSizesRet().get(j)) {
						padding = true;
						break;
					}
				}
			}
			int actualSize = 0;
			for (int fieldSize : fct.getFieldSizesRet()) {
				actualSize += fieldSize;
			}
			if (padding) {
				sizeOftuple = fct.getFieldSizesRet().size()*8;
			} else {
				sizeOftuple = actualSize;
			}
			DataTransformation dtrans = new DataTransformation();
			for (int i = 0; i < out.length; i+=sizeOftuple) {
				byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
				if (outputCollector instanceof CountingCollector) {
					((CountingCollector) outputCollector).collect(recordBytes);
				}
			}
		} else if (fct.getFieldSizesRet().size() == 3) {
			int sizeOftuple = 0;
			boolean padding = false;
			for (int i = 0; i < fct.getFieldSizesRet().size(); i++) {
				for (int j = i + 1; j < fct.getFieldSizesRet().size(); j++) {
					if (fct.getFieldSizesRet().get(i) != fct.getFieldSizesRet().get(j)) {
						padding = true;
						break;
					}
				}
			}
			int actualSize = 0;
			for (int fieldSize : fct.getFieldSizesRet()) {
				actualSize += fieldSize;
			}
			if (padding) {
				sizeOftuple = fct.getFieldSizesRet().size()*8;
			} else {
				sizeOftuple = actualSize;
			}

			if (outputCollector instanceof CountingCollector) {
				if (((CountingCollector) outputCollector).getCollector() instanceof ChainedReduceCombineDriver) {
					DataTransformation<T> dtrans = new DataTransformation();
					ArrayList<T> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					for (T record : records) {
						(((CountingCollector<T>) outputCollector).getCollector()).collect(record);
					}
				} else {
					DataTransformation dtrans = new DataTransformation();
					for (int i = 0; i < out.length; i += sizeOftuple) {
						byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
						((CountingCollector) outputCollector).collect(recordBytes);
					}
				}
			}
		} else if (fct.getFieldSizesRet().size() == 4) {
			long total = 0;
			int sizeOftuple = 0;
			boolean padding = false;
			for (int i = 0; i < fct.getFieldSizesRet().size(); i++) {
				for (int j = i + 1; j < fct.getFieldSizesRet().size(); j++) {
					if (!fct.getFieldSizesRet().get(i).equals(fct.getFieldSizesRet().get(j))) {
						padding = true;
						break;
					}
				}
			}
			int actualSize = 0;
			for (int fieldSize : fct.getFieldSizesRet()) {
				actualSize += fieldSize;
			}
			if (padding) {
				sizeOftuple = fct.getFieldSizesRet().size()*8;
			} else {
				sizeOftuple = actualSize;
			}

			if (outputCollector instanceof CountingCollector) {
				if (((CountingCollector) outputCollector).getCollector() instanceof ChainedReduceCombineDriver) {
					DataTransformation<T> dtrans = new DataTransformation();
					ArrayList<T> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					for (T record : records) {
						(((CountingCollector<T>) outputCollector).getCollector()).collect(record);
					}
					//((ChainedReduceCombineDriver) ((CountingCollector<OT>) outputCollector).getCollector()).collect(acres, fct, padding);
				} else {
					DataTransformation dtrans = new DataTransformation();
					for (int i = 0; i < out.length; i += sizeOftuple) {
						//long breakRecordsStart = System.currentTimeMillis();
						byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
						//long breakRecordsEnd = System.currentTimeMillis();
						((CountingCollector) outputCollector).collect(recordBytes);
						//total += (breakRecordsEnd - breakRecordsStart);
					}
				}
			}
			//System.out.println("**** Function CentroidAccumulator break data into records: " + total);
		}
	}
	private void sortAndCombine() throws Exception {
		final InMemorySorter<T> sorter = this.sorter;

		if (!sorter.isEmpty()) {
			sortAlgo.sort(sorter);

			final TypeSerializer<T> serializer = this.serializer;
			final TypeComparator<T> comparator = this.comparator;
			final ReduceFunction<T> function = this.reducer;
			final Collector<T> output = this.outputCollector;
			final MutableObjectIterator<T> input = sorter.getIterator();

			if (objectReuseEnabled) {
				// We only need two objects. The first reference stores results and is
				// eventually collected. New values are read into the second.
				//
				// The output value must have the same key fields as the input values.

				T reuse1 = input.next();
				T reuse2 = serializer.createInstance();

				T value = reuse1;

				// iterate over key groups
				while (running && value != null) {
					comparator.setReference(value);

					// iterate within a key group
					while ((reuse2 = input.next(reuse2)) != null) {
						if (comparator.equalToReference(reuse2)) {
							// same group, reduce
							value = function.reduce(value, reuse2);

							// we must never read into the object returned
							// by the user, so swap the reuse objects
							if (value == reuse2) {
								T tmp = reuse1;
								reuse1 = reuse2;
								reuse2 = tmp;
							}
						} else {
							// new key group
							break;
						}
					}
					output.collect(value);

					// swap the value from the new key group into the first object
					T tmp = reuse1;
					reuse1 = reuse2;
					reuse2 = tmp;

					value = reuse1;
				}
			} else {
				T value = input.next();
				long total = 0;
				long start, end;
				// iterate over key groups
				while (running && value != null) {
					comparator.setReference(value);
					T res = value;
					//start = System.currentTimeMillis();
					// iterate within a key group
					while ((value = input.next()) != null) {
						if (comparator.equalToReference(value)) {
							// same group, reduce
							res = function.reduce(res, value);
						} else {
							// new key group
							break;
						}
					}
				//	end = System.currentTimeMillis();
				//	total = total + (end - start);
					output.collect(res);
				}
				//System.out.println("**** Function GROUPBY Flink Reduction: " + total);
			}
		}
	}

	// Add partial results of reduction
	public static void addPRField(byte[] partialResults, byte[] total, int fieldNo, ArrayList<Integer> fieldSizes, ArrayList<String> fieldTypes, int totalSize, boolean changeEndianess, boolean padding) {
		int offset = 0;
		if (fieldNo != 0) {
			for (int i = 0; i < fieldNo; i++) {
				if (padding) {
					offset += 8;
				} else {
					offset += fieldSizes.get(i);
				}
			}
		}

		int totalNumInt = 0;
		double totalNumDouble = 0;
		float totalNumFloat = 0;
		long totalNumLong = 0;

		int fieldSize = fieldSizes.get(fieldNo);
		int j = 0;
		for (int i = offset; i < partialResults.length; i+=totalSize) {
			//if (offset + i >= partialResults.length) break;

			if (fieldTypes.get(fieldNo).equals("int")) {
				byte[] b = new byte[4];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess4(b);
				totalNumInt += ByteBuffer.wrap(b).getInt();
			} else if (fieldTypes.get(fieldNo).equals("double")) {
				byte[] b = new byte[8];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess8(b);
				totalNumDouble += ByteBuffer.wrap(b).getDouble();
			} else if (fieldTypes.get(fieldNo).equals("float")) {
				byte[] b = new byte[4];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess4(b);
				totalNumFloat += ByteBuffer.wrap(b).getFloat();
			} else if (fieldTypes.get(fieldNo).equals("long")) {
				byte[] b = new byte[8];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess8(b);
				totalNumLong += ByteBuffer.wrap(b).getLong();
			}
			j++;
		}

		if (fieldTypes.get(fieldNo).equals("int")) {
			byte[] bytesOfField = new byte[4];
			ByteBuffer.wrap(bytesOfField).putInt(totalNumInt);
			if (changeEndianess) {
				changeOutputEndianess4(bytesOfField);
			}
			for (int i = 0; i < 4; i++) {
				total[i + offset] = bytesOfField[i];
			}
		} else if (fieldTypes.get(fieldNo).equals("double")) {
			byte[] bytesOfField = new byte[8];
			ByteBuffer.wrap(bytesOfField).putDouble(totalNumDouble);
			if (changeEndianess) {
				changeOutputEndianess8(bytesOfField);
			}
			for (int i = 0; i < 8; i++) {
				total[i + offset] = bytesOfField[i];
			}
		} else if (fieldTypes.get(fieldNo).equals("float")) {
			byte[] bytesOfField = new byte[4];
			ByteBuffer.wrap(bytesOfField).putFloat(totalNumFloat);
			if (changeEndianess) {
				changeOutputEndianess4(bytesOfField);
			}
			for (int i = 0; i < 4; i++) {
				total[i + offset] = bytesOfField[i];
			}
		} else if (fieldTypes.get(fieldNo).equals("long")) {
			byte[] bytesOfField = new byte[8];
			ByteBuffer.wrap(bytesOfField).putLong(totalNumLong);
			if (changeEndianess) {
				changeOutputEndianess8(bytesOfField);
			}
			for (int i = 0; i < 8; i++) {
				total[i + offset] = bytesOfField[i];
			}
		}

	}

	public static byte[] changeOutputEndianess4(byte[] output) {
		byte tmp;
		for (int i = 0; i < output.length; i += 4) {
			// swap 0 and 3
			tmp = output[i];
			output[i] = output[i + 3];
			output[i + 3] = tmp;
			// swap 1 and 2
			tmp = output[i + 1];
			output[i + 1] = output[i + 2];
			output[i + 2] = tmp;
		}
		return output;
	}

	public static byte[] changeOutputEndianess8(byte[] output) {
		byte tmp;
		for (int i = 0; i < output.length; i += 8) {
			// swap 0 and 7
			tmp = output[i];
			output[i] = output[i + 7];
			output[i + 7] = tmp;
			// swap 1 and 6
			tmp = output[i + 1];
			output[i + 1] = output[i + 6];
			output[i + 6] = tmp;
			// swap 2 and 5
			tmp = output[i + 2];
			output[i + 2] = output[i + 5];
			output[i + 5] = tmp;
			// swap 3 and 4
			tmp = output[i + 3];
			output[i + 3] = output[i + 4];
			output[i + 4] = tmp;
		}
		return output;
	}

	@Override
	public void close() {
		// send the final batch
		if (tornado && !flinkReductions) {
			try {
				switch (strategy) {
					case SORTED_PARTIAL_REDUCE:
						sortAlgo.sort(sorter);
						sortAndCombineTornadoVM();
						break;
					case HASHED_PARTIAL_REDUCE:
						reduceFacade.emit();
						break;
				}
			} catch (Exception ex2) {
				throw new ExceptionInChainedStubException(taskName, ex2);
			}
		} else {
			try {
				switch (strategy) {
					case SORTED_PARTIAL_REDUCE:
						sortAndCombine();
						break;
					case HASHED_PARTIAL_REDUCE:
						reduceFacade.emit();
						break;
				}
			} catch (Exception ex2) {
				throw new ExceptionInChainedStubException(taskName, ex2);
			}
		}

		outputCollector.close();
		dispose(false);
	}

	private static boolean compareBytes (byte[] array, int pos1, int pos2, int fieldLength, int keyOffset) {
		boolean eq = true;
		int i = 0;
		while (eq && i < fieldLength) {
			if (array[pos1 + keyOffset + i] == array[pos2 + keyOffset + i]) i++;
			else eq = false;
		}
		return eq;
	}

	private static boolean BinaryGroupIndexBytes(byte[] array, int start, int storedStart, int end, int tupleSize, int keyLength, int keyOffset, ArrayList<int[]> groups) {
		if (start <= end) {
			int middle = (start / tupleSize + (end - start) / (tupleSize * 2)) * tupleSize;

			if (middle == end) {
				if (compareBytes(array, start, middle, keyLength, keyOffset)) { // compare array[start] == array[middle]
					int[] positions = new int[2];
					if (storedStart != start) {
						positions[0] = storedStart;
					} else {
						positions[0] = start;
					}
					positions[1] = middle;
					groups.add(positions);
					return true;
				} else {
					int[] positions = new int[2];
					positions[0] = middle;
					positions[1] = middle;
					groups.add(positions);
					return true;
				}
			}

			if (!compareBytes(array, start, middle, keyLength, keyOffset)) { // compare array[start] != array[middle]
				return BinaryGroupIndexBytes(array, start, storedStart, middle - tupleSize, tupleSize, keyLength, keyOffset, groups);
			} else {
				if (!compareBytes(array, start, middle + tupleSize, keyLength, keyOffset)) { // compare array[start] != array[middle + 1]
					int[] positions = new int[2];
					if (storedStart != start) {
						positions[0] = storedStart;
					} else {
						positions[0] = start;
					}
					positions[1] = middle;
					groups.add(positions);
					return true;
				} else {
					if (start != storedStart) {
						return BinaryGroupIndexBytes(array, middle + tupleSize, storedStart, end, tupleSize, keyLength, keyOffset, groups);
					} else {
						return BinaryGroupIndexBytes(array, middle + tupleSize, start, end, tupleSize, keyLength, keyOffset, groups);
					}
				}
			}
		} else {
			return false;
		}
	}

	@Override
	public void closeTask() throws Exception {
		if (running) {
			BatchTask.closeUserCode(reducer);
		}
	}

	@Override
	public void cancelTask() {
		running = false;
		dispose(true);
	}

	private void dispose(boolean ignoreException) {
		try {
			if (sorter != null) {
				sorter.dispose();
			}
			if (table != null) {
				table.close();
			}
		} catch (Exception e) {
			// May happen during concurrent modification.
			if (!ignoreException) {
				throw e;
			}
		} finally {
			parent.getEnvironment().getMemoryManager().release(memory);
		}
	}
}
