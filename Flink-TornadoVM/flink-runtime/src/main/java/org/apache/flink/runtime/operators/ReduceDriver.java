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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.chaining.ChainedAllReduceDriver;
import org.apache.flink.runtime.operators.chaining.ChainedMapDriver;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.apache.flink.runtime.tornadovm.DataTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.*;

/**
 * Reduce driver which is executed by a Task Manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The ReduceDriver creates an iterator over all records from its input. The iterator returns all records grouped by their
 * key. The elements are handed pairwise to the <code>reduce()</code> method of the ReduceFunction.
 * </p>
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
public class ReduceDriver<T> implements Driver<ReduceFunction<T>, T> {
	private static final Logger LOG = LoggerFactory.getLogger(ReduceDriver.class);
	private TaskContext<ReduceFunction<T>, T> taskContext;
	private MutableObjectIterator<T> input;

	private TypeSerializer<T> serializer;

	private TypeComparator<T> comparator;
	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	private boolean tornado; // = Boolean.parseBoolean(System.getProperty("tornado", "false"));
	private boolean flinkReductions;
	// ------------------------------------------------------------------------

	@Override
	public void setup(TaskContext<ReduceFunction<T>, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<ReduceFunction<T>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<ReduceFunction<T>> clazz = (Class<ReduceFunction<T>>) (Class<?>) ReduceFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 1;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.SORTED_REDUCE) {
			throw new Exception("Unrecognized driver strategy for Reduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.<T>getInputSerializer(0).getSerializer();
		this.comparator = this.taskContext.getDriverComparator(0);
		this.input = this.taskContext.getInput(0);

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("Reducer preprocessing done. Running Reducer code."));
		}

		final Counter numRecordsIn = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		final Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		// cache references on the stack
		final MutableObjectIterator<T> input = this.input;
		final TypeSerializer<T> serializer = this.serializer;
		final TypeComparator<T> comparator = this.comparator;
		final ReduceFunction<T> function = this.taskContext.getStub();
		final Collector<T> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		tornado = Boolean.parseBoolean(System.getProperty("tornado", "false"));
		flinkReductions = Boolean.parseBoolean(System.getProperty("flinkReductions", "false"));
		// ----------------
		if (tornado && !flinkReductions) {
			byte[] b;
			if (input instanceof NormalizedKeySorter.NormalizedKeySorterIterator) {
			//	long compStart = System.currentTimeMillis();
				FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
				AccelerationData acdata = new AccelerationData();
				int returnSize = 0;
				byte[] typeB = new byte[0];
				typeB = this.taskContext.getTaskConfig().getConfiguration().getTypeInfo(typeB);
				ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
				ObjectInputStream ins = new ObjectInputStream(bObj);
				HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
				ins.close();
				bObj.close();

				for (String name : typeInf.keySet()) {
					if (name.contains(function.getClass().toString().replace("class ", ""))) {
						returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acdata);
						break;
					}
				}

				int actualSize = 0;
				int numOfFields = 0;
				boolean padding = false;

				int key = ((TupleComparator) comparator).keyPositions[0];

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

			//	long compEnd = System.currentTimeMillis();
				//System.out.println("**** Function ReduceDriver set compiler info and extract typeinfo: " + (compEnd - compStart));
			//	long dataStart = System.currentTimeMillis();
				b = ((NormalizedKeySorter.NormalizedKeySorterIterator) input).getBytes(actualSize);
			//	System.out.println(b.length);
			//	long dataEnd = System.currentTimeMillis();
				//System.out.println("**** Function ReduceDriver get data from sorter: " + (dataEnd - dataStart));

			//	long tornadoDataStart = System.currentTimeMillis();
				DataTransformation dtrans = new DataTransformation();
				AccelerationData acres = dtrans.generateInputAccelerationData(b, serializer, null, false, false, false, false);
				long tornadoDataEnd = System.currentTimeMillis();
				byte[] bytes = acres.getRawData();
				//System.out.println("**** Function ReduceDriver make data Tornado-Friendly: " + (tornadoDataEnd - tornadoDataStart));
				long groupsStart = System.currentTimeMillis();

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
				//System.out.println("**** Function ReduceDriver identify groups: " + (groupsEnd - groupsStart));

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
					if (numOfElements == 1 || groupsB.size() == 1) {
						int q = 0;
						for (int k = currpos; k < currpos + sizeOfTuple; k++) {
							totalBytes[k] = group[q];
							q++;
						}
						currpos += sizeOfTuple;
						//acres.setRawData(group);
					} else {
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
						//String path = System.getProperty("user.dir");
						String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
						FlinkData f = new FlinkData(true, group, outBytes);
						TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
						// @formatter:off
						TaskSchedule ts = new TaskSchedule("s8")
							.flinkInfo(f)
							.prebuiltTask("t8",
								"reduce",
								path + "/pre-compiled/prebuilt-centroidAccum.cl",
							//	"./pre-compiled/prebuilt-centroidAccum.cl",
								new Object[]{group, outBytes},
								new Access[]{Access.READ, Access.WRITE},
								defaultDevice,
								new int[]{numOfElements})
							.streamOut(outBytes);

						//long startRed = System.currentTimeMillis();
						ts.execute();
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
						int q = 0;
						for (int k = currpos; k < sizeOfTuple + currpos; k++) {
							totalBytes[k] = total[q];
							q++;
						}
						currpos += sizeOfTuple;
						//acres.setRawData(total);
					}
				}
				acres.setRawData(totalBytes);
				acres.setInputSize(totalBytes.length / sizeOfTuple);
				// collect
				if (this.taskContext.getOutputCollector() instanceof ChainedAllReduceDriver) {
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) this.taskContext.getOutputCollector();
					// not acres
					chred.collect(acres);
				} else if (this.taskContext.getOutputCollector() instanceof ChainedMapDriver) {
					ChainedMapDriver chmad = (ChainedMapDriver) this.taskContext.getOutputCollector();
					// not acres
					chmad.collect(acres);
				}
				//System.out.println("**** Function CentroidAccumulator split dataset: " + totalSplit);
				//System.out.println("**** Function CentroidAccumulator execution time: " + totalRedTime);
				//System.out.println("**** Function CentroidAccumulator collect Partial Results: " + totalPR);
			} else {
				// In this case the data is not grouped...here we would have the cases with the skeletons
			}
		} else {

			// ---------------
			if (objectReuseEnabled) {
				// We only need two objects. The first reference stores results and is
				// eventually collected. New values are read into the second.
				//
				// The output value must have the same key fields as the input values.

				T reuse1 = input.next();
				T reuse2 = serializer.createInstance();

				T value = reuse1;

				// iterate over key groups
				while (this.running && value != null) {
					numRecordsIn.inc();
					comparator.setReference(value);

					// iterate within a key group
					while ((reuse2 = input.next(reuse2)) != null) {
						numRecordsIn.inc();
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
				// iterate over key groups
				long total = 0;
				long start, end;
				while (this.running && value != null) {
					numRecordsIn.inc();
					comparator.setReference(value);
					T res = value;
					//start = System.currentTimeMillis();
					// iterate within a key group
					while ((value = input.next()) != null) {
						numRecordsIn.inc();
						if (comparator.equalToReference(value)) {
							// same group, reduce
							res = function.reduce(res, value);
						} else {
							// new key group
							break;
						}
					}
					//end = System.currentTimeMillis();
					//total = total + (end - start);
					output.collect(res);
				}
				//System.out.println("**** Function CentroidAccumulator Flink Reduction: " + total);
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

	public static boolean isPowerOfTwo(int inputSize) {
		boolean isPow2 = (int) (Math.ceil((Math.log(inputSize) / Math.log(2)))) == (int) (Math.floor(((Math.log(inputSize) / Math.log(2)))));
		return isPow2;
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}
