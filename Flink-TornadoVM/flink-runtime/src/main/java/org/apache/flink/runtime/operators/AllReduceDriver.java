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

import org.apache.flink.api.asm.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.flinktornadolog.FlinkTornadoVMLogger;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.chaining.ChainedAllReduceDriver;
import org.apache.flink.runtime.operators.chaining.ChainedMapDriver;
import org.apache.flink.runtime.operators.chaining.ChainedReduceCombineDriver;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.apache.flink.runtime.tornadovm.DataTransformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.MutableObjectIterator;
import scala.Array;
import uk.ac.manchester.tornado.api.*;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.runtime.tasks.PrebuiltTask;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UnknownFormatFlagsException;
import java.util.stream.IntStream;

//import static org.apache.flink.api.asm.ExamineUDF.inOwner;
//import static org.apache.flink.api.asm.ExamineUDF.outOwner;
import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.examineTypeInfoForFlinkUDFs;

/**
 * Reduce task which is executed by a Task Manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceFunction
 * implementation.
 * <p>
 * The AllReduceDriver creates an iterator over all records from its input.
 * The elements are handed pairwise to the <code>reduce()</code> method of the ReduceFunction.
 * 
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
public class AllReduceDriver<T> implements Driver<ReduceFunction<T>, T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(AllReduceDriver.class);

	private TaskContext<ReduceFunction<T>, T> taskContext;
	
	private MutableObjectIterator<T> input;

	private TypeSerializer<T> serializer;
	
	private boolean running;

	private boolean objectReuseEnabled = false;
	private boolean tornado = Boolean.parseBoolean(System.getProperty("tornado", "false"));

	public static final boolean BREAKDOWN = Boolean.parseBoolean(System.getProperties().getProperty("flinktornado.breakdown", "false"));

	// ------------------------------------------------------------------------

	private TaskSchedule[] taskSchedules = new TaskSchedule[4];
	private double[] inputDataReduce;
	private double[] output;

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
		return 0;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		final TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.ALL_REDUCE) {
			throw new Exception("Unrecognized driver strategy for AllReduce driver: " + config.getDriverStrategy().name());
		}
		
		TypeSerializerFactory<T> serializerFactory = this.taskContext.getInputSerializer(0);
		this.serializer = serializerFactory.getSerializer();
		this.input = this.taskContext.getInput(0);

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("AllReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}


	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllReduce preprocessing done. Running Reducer code."));
		}

		final Counter numRecordsIn = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		final Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

		final ReduceFunction<T> stub = this.taskContext.getStub();
		final MutableObjectIterator<T> input = this.input;
		final TypeSerializer<T> serializer = this.serializer;
		final Collector<T> collector = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

		// ==========================================================
		// TornadoVM-Flink
		// This is only a proof of concept just to demonstrate how
		// TornadoVM and Flink could be connected. Since this launches
		// a pre-compiled kernel, this should not be the path to go.
		// This driver performs reductions. Once Full reductions are
		// working in Flink-TornadoVM, the following code can take
		// advantage of the TornadoVM JIT compiler to perform full
		// reductions on GPU, so the driver will be more generic.
		// ==========================================================
		boolean tornadoConfFlag = this.taskContext.getTaskConfig().getConfiguration().getTornadoVMFlag();
		if (tornado || tornadoConfFlag) {
			long startTornado = System.currentTimeMillis();
			String className = stub.getClass().getSimpleName();
			int numTornadoFunction = getReductionType(className);
			String path = "./";//System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
			boolean compiledReductions = true;
			if (compiledReductions) {
				int numOfElements = 0;
				byte[] inputByteData = null;
				AccelerationData acdata = null;
				AccelerationData acres = new AccelerationData();

				//ArrayList dataset = new ArrayList();

				if (input instanceof ReaderIterator) {

					DataTransformation dtrans = new DataTransformation();
					//long dataStart = System.currentTimeMillis();
					byte[] inBytes = ((ReaderIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ ALLREDUCE READITER BUFFER NULL");
						}
						return;
					}

					acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<T>getInputSerializer(0).getSerializer(), null, false, false, true, false);
				}

				inputByteData = acdata.getRawData();
				numOfElements = acdata.getInputSize();
				numRecordsIn.inc(numOfElements);
				String reduceUserClassName = stub.getClass().toString().replace("class ", "").replace(".", "/");
				// TODO: Change to getting byte array from configuration and loading it
				Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesReduce.getTypes(reduceUserClassName);
				String inOwner = inOutTypes.f0;
				String outOwner = inOutTypes.f1;
				byte[] def = new byte[0];
				byte[] bytesRsk = this.taskContext.getTaskConfig().getConfiguration().getSkeletonReduceBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleReduce mdr = loader.loadClassRed("org.apache.flink.api.asm.ReduceASMSkeleton", bytesRsk);
				TornadoReduce rsk = new TornadoReduce(mdr);
				//TornadoReduce rsk = AcceleratedFlinkUserFunction.getTornadoReduce();

				FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
				if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
					int returnSize = 0;
					acres = new AccelerationData();

					fct0 = new FlinkCompilerInfo();
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						int ref = name.indexOf("@");
						String classname = name.substring(0, ref);
						if (classname.equals(stub.getClass().toString().replace("class ", ""))) {
							//System.out.println("-> name: " + name + " mapper: " + mapper.getClass().toString().replace("class ", ""));
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					if (!isPowerOfTwo(numOfElements)) {
						double log = Math.log(numOfElements) / Math.log(2);
						int exp = (int) log;
						numOfElements = (int) Math.pow(2, exp + 1);
					}

					int size; //= inputByteData.length;
					if (numOfElements <= 256) {
						size = 2;
					} else {
						size = numOfElements / 256 + 1;
					}
					size *= 4;

					ArrayList<Integer> fieldSizes = fct0.getFieldSizes();
					int totalTupleSize = fct0.getReturnArrayFieldTotalBytes();
					int arrayPos = fct0.getReturnTupleArrayFieldNo();
					for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
						if (i != arrayPos) {
							if (fct0.getDifferentTypesRet()) {
								totalTupleSize += 8;
								fieldSizes.set(i, 8);
							} else {
								totalTupleSize += fct0.getFieldSizesRet().get(i);
							}
						} else {
							fieldSizes.set(arrayPos, fct0.getReturnArrayFieldTotalBytes());
						}
					}
					int arraySize = fct0.getArraySize();
//					int tsize = 0;
//					int arrayPos = -1;
//					byte[] outBytes;
//					if (fct0.getReturnArrayField()) {
//						tsize = fct0.getReturnArrayFieldTotalBytes();
//						arrayPos = fct0.getReturnTupleArrayFieldNo();
//						for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//							if (i != arrayPos) {
//								if (fct0.getDifferentTypesRet()) {
//									tsize += 8;
//								} else {
//									tsize += fct0.getFieldSizesRet().get(i);
//								}
//							}
//						}
//						outBytes = new byte[tsize];
//					} else {
//						outBytes = new byte[returnSize];
//					}
//					int totalTupleSize = outBytes.length;
					byte[] outBytes = new byte[size * totalTupleSize];
					//byte[] outBytes = new byte[size * returnSize];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					//FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
					fct0.setArrayField(true);
					Tuple2[] in = new Tuple2[1];
					Tuple2[] out = new Tuple2[1];

					WorkerGrid worker = new WorkerGrid1D(numOfElements);
					GridScheduler gridScheduler = new GridScheduler("s2.t2", worker);

					TaskSchedule ts = new TaskSchedule("s2")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t2", rsk::reduce, in, out)
						.streamOut(outBytes);
						//.execute();

					worker.setGlobalWork(numOfElements, 1, 1);
					long start, end;
					if (BREAKDOWN) {
						start = System.currentTimeMillis();
						ts.execute(gridScheduler);
						end = System.currentTimeMillis();
						System.out.println();
						FlinkTornadoVMLogger.printBreakDown("**** ALLRED Function UpdateAccumulator (RED-PowerOfTwo) execution time: " + (end - start));
					} else {
						ts.execute(gridScheduler);
					}

//					for (int i = 0; i < outBytes.length; i++) {
//						System.out.print(outBytes[i] + " ");
//					}
//					System.out.println("\n");
					byte[] total = new byte[totalTupleSize];
					if (BREAKDOWN) {
						long PRstart = System.currentTimeMillis();
						addPRField(outBytes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
						Array.copy(outBytes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
						long PRend = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** ALLRED Function UpdateAccumulator (RED-PowerOfTwo) collect partial results: " + (PRend - PRstart));
					} else {
						addPRField(outBytes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
						Array.copy(outBytes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
					}
					acres.setRawData(total);
					acres.setInputSize(1);
					acres.setReturnSize(1);
					if (fct0.getReturnArrayField()) {
						acres.hasArrayField();
						acres.setLengthOfArrayField(arraySize);
						acres.setArrayFieldNo(0);
						int totalB = fct0.getReturnArrayFieldTotalBytes();
						for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
							if (i != arrayPos) {
								totalB += fct0.getFieldSizesRet().get(i);
							}
						}
						acres.setTotalBytes(totalB);
						acres.setRecordSize(totalTupleSize);
					}
//					byte[] total = outBytes; //new byte[totalTupleSize];
//
//					acres.setRawData(total);
//					acres.setInputSize(1);
//					acres.setReturnSize(1);
//					if (fct0.getReturnArrayField()) {
//						acres.hasArrayField();
//						acres.setLengthOfArrayField(83);
//						acres.setArrayFieldNo(0);
//						int totalB = fct0.getReturnArrayFieldTotalBytes();
//						for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//							if (i != arrayPos) {
//								totalB += fct0.getFieldSizesRet().get(i);
//							}
//						}
//						acres.setTotalBytes(totalB);
//						acres.setRecordSize(totalTupleSize);
//					}
				} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
					//long infoStart = System.currentTimeMillis();
					int returnSize = 0;

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(stub.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					int size; //= inputByteData.length;
					if (numOfElements <= 256) {
						size = 2;
					} else {
						size = numOfElements / 256 + 1;
					}
					size *= 4;
					byte[] outBytes = new byte[size * returnSize];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					//FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					Tuple4[] in = new Tuple4[1];
					Tuple4[] out = new Tuple4[1];

					WorkerGrid worker = new WorkerGrid1D(numOfElements);
					TaskSchedule ts;
					byte[] def2 = new byte[0];
					byte[] bytesRsk2 = this.taskContext.getTaskConfig().getConfiguration().getSkeletonReduceBytes2(def2);

					if (bytesRsk2.length != 0) {
						GridScheduler gridScheduler = new GridScheduler("s5.t5", worker);
						AsmClassLoader loader2 = new AsmClassLoader();
						// either load class using classloader or retrieve loaded class
						MiddleReduce2 mdr2 = loader2.loadClassRed2("org.apache.flink.api.asm.ReduceASMSkeleton2", bytesRsk2);
						TornadoReduce2 rsk2 = new TornadoReduce2(mdr2);
						ts = new TaskSchedule("s5")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t5", rsk2::reduce, in, out)
							.streamOut(outBytes);
						worker.setGlobalWork(numOfElements, 1, 1);
						if (BREAKDOWN) {
							long start = System.currentTimeMillis();
							ts.execute(gridScheduler);
							long end = System.currentTimeMillis();
							FlinkTornadoVMLogger.printBreakDown("**** Function Evaluate execution time: " + (end - start));;
						} else {
							ts.execute(gridScheduler);
						}

						byte[] total = new byte[returnSize];
						if (BREAKDOWN) {
							long PRstart = System.currentTimeMillis();
							for (int i = 0; i < 4; i++) {
								addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
							}
							long PRend = System.currentTimeMillis();
							FlinkTornadoVMLogger.printBreakDown("**** Function Evaluate collect partial results: " + (PRend - PRstart));
						} else {
							for (int i = 0; i < 4; i++) {
								addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
							}
						}
						acres = new AccelerationData(total, 1, returnSize);
					} else {
						GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);

						ts = new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", rsk::reduce, in, out)
							.streamOut(outBytes);
						//.execute();

						worker.setGlobalWork(numOfElements, 1, 1);
						ts.execute(gridScheduler);

						byte[] total = new byte[returnSize];
						//long PRstart = System.currentTimeMillis();
						Array.copy(outBytes, 0, total, 0, 8);
						getPRIoT(outBytes, total, 1, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true, numTornadoFunction, numOfElements);
						Array.copy(outBytes, 16, total, 16, 8);
						acres = new AccelerationData(total, 1, returnSize);
					}
				}
				if (this.taskContext.getOutputCollector() instanceof ChainedAllReduceDriver) {
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) this.taskContext.getOutputCollector();
					((CountingCollector) collector).numRecordsOut.inc(acres.getInputSize());
					chred.collect(acres);
				} else if (this.taskContext.getOutputCollector() instanceof ChainedMapDriver) {
					ChainedMapDriver chmad = (ChainedMapDriver) this.taskContext.getOutputCollector();
					((CountingCollector) collector).numRecordsOut.inc(acres.getInputSize());
					chmad.collect(acres);
				} else {
					materializeTornadoBuffers(acres, outOwner, fct0);
				}
			} else if (numTornadoFunction > 0) {
				// SPARKWORKS
				T val1;
				if ((val1 = input.next()) == null) {
					return;
				}
				numRecordsIn.inc();
				T value2 = serializer.createInstance();

				tornadoVMCleanUp();
				// We know which expression to replace. Once the Full JIT compiler
				// is fixed, the whole marshalling has to be generalized

				long sMar = System.currentTimeMillis();
				T value3 = value2;
				ArrayList<Tuple4> arr = new ArrayList<>();
				arr.add((Tuple4) val1);
				while ((value3 = input.next(value3)) != null) {
					arr.add((Tuple4) value3);
				}

				int inputSize = arr.size();
				double log = Math.log(inputSize) / Math.log(2);
				int exp = (int) log;
				if (Math.log(inputSize) % Math.log(2) != 0) {
					inputSize = (int) Math.pow(2, exp + 1);
				} else {
					inputSize = (int) Math.pow(2, exp);
				}

				// We know which expression to replace. Once the Full JIT compiler
				// is fixed, the whole marshalling has to be generalized

				inputDataReduce = new double[inputSize];
				//inputDataReduce[0] = (double) ((Tuple4) arr.get(0)).f1;

				{
					int i = 0;
					for (Tuple4 t4 : arr) {
						inputDataReduce[i++] = (double) ((Tuple4) t4).f1;
					}
				}
				long eMar = System.currentTimeMillis();

				// GPU groups = 256 threads by default
				int size = inputDataReduce.length / 256;
				Random r = new Random(System.currentTimeMillis());
				output = new double[size];

				TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
				FlinkData f = new FlinkData(true, inputDataReduce, output);
				WorkerGrid worker1D = new WorkerGrid1D(inputDataReduce.length);
				worker1D.setLocalWork(256, 1, 1);

				String taskName = "t" + r.nextLong();
				GridScheduler gridScheduler = new GridScheduler("reduce-doubles" + numTornadoFunction + "." + taskName, worker1D);

				//if (taskSchedules[numTornadoFunction - 1] == null) {
				String fileOCL = getPreCompiledFunction(numTornadoFunction);
				// We cache the expression
				TaskSchedule ts = new TaskSchedule("reduce-doubles" + numTornadoFunction)
					//.flinkInfo(f)
					.streamIn(inputDataReduce, output)
					.prebuiltTask(taskName,
						"reduceDouble_" + numTornadoFunction,
						//"reduce",
						fileOCL,
						new Object[]{ inputDataReduce, output },
						new Access[]{ Access.READ, Access.WRITE },
						defaultDevice,
						new int[]{ inputDataReduce.length })
					.streamOut(output);
				taskSchedules[numTornadoFunction - 1] = ts;
				//}
				long start = System.currentTimeMillis();
				taskSchedules[numTornadoFunction - 1].execute(gridScheduler);
				long end = System.currentTimeMillis();

				// Final reduction
				int fieldNum = 1;
				int tupleNumber = 4;
				for (int k = 0; k < size; k++) {
					int idx = tupleNumber * k + fieldNum;
					if (idx < size) {
						switch (numTornadoFunction) {
							case 1:
								output[0] = Math.min(output[0], output[idx]);
								break;
							case 2:
								output[0] = Math.max(output[0], output[idx]);
								break;
							case 3:
							case 4:
								output[0] += output[idx];
								break;
						}
					}
				}
				if (numTornadoFunction == 4) {
					output[0] /= inputSize;
				}
				//long end2 = System.currentTimeMillis();
				//System.out.println("TotalTime-Parallel(ms): " + (end-start));
				//System.out.println("TotalTime-TOTALJOB(ms): " + (end2-startTornado));
				//System.out.println("Marshalling(ms)-------: " + (eMar-sMar));
				return;
			} else {
				int numOfElements = 0;
				byte[] inputByteData = null;
				AccelerationData acdata = null;
				AccelerationData acres = new AccelerationData();

				//ArrayList dataset = new ArrayList();

				if (input instanceof ReaderIterator) {

					DataTransformation dtrans = new DataTransformation();
					//long dataStart = System.currentTimeMillis();
					byte[] inBytes = ((ReaderIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ ALLREDUCE READITER BUFFER NULL");
						}
						return;
					}

					acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<T>getInputSerializer(0).getSerializer(), null, false, false, true, false);
//					T testRecord;
//					ArrayList dataset = new ArrayList();
//					if ((testRecord = input.next()) == null) {
//						return;
//					}
//					dataset.add(testRecord);
//					T record = this.taskContext.<T>getInputSerializer(0).getSerializer().createInstance();
//
//					while (this.running && ((record = input.next(record)) != null)) {
//						dataset.add(record);
//					}
//					acdata = dtrans.generateBroadcastedInputAccelerationData(dataset, this.taskContext.<T>getInputSerializer(0).getSerializer());
					//long dataEnd = System.currentTimeMillis();
					//	System.out.println("==== INPUT " + acdata.getInputSize());
					//System.out.println("**** Function " + function + " get input dataset and make it Tornado-friendly: " + (dataEnd - dataStart));
				}

				inputByteData = acdata.getRawData();
				numOfElements = acdata.getInputSize();
				numRecordsIn.inc(numOfElements);
				String reduceUserClassName = stub.getClass().toString().replace("class ", "").replace(".", "/");
				// TODO: Change to getting byte array from configuration and loading it
				Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesReduce.getTypes(reduceUserClassName);
				String inOwner = inOutTypes.f0;
				String outOwner = inOutTypes.f1;
				byte[] def = new byte[0];
				byte[] bytesRsk = this.taskContext.getTaskConfig().getConfiguration().getSkeletonReduceBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleReduce mdr = loader.loadClassRed("org.apache.flink.api.asm.ReduceASMSkeleton", bytesRsk);
				TornadoReduce rsk = new TornadoReduce(mdr);
				//TornadoReduce rsk = AcceleratedFlinkUserFunction.getTornadoReduce();

				FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
				if (inOwner.equals("java/lang/Integer")) {
					int[] in = new int[numOfElements];

					// Based on the output data type create the output array and the Task Schedule
					// Currently, the input and output data can only be of type Integer, Double, Float or Long.
					// Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
					// since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.

					int[] out = new int[1];
					byte[] outBytes = new byte[4];
					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", rsk::reduce, in, out)
						.streamOut(out)
						.execute();

//					System.out.println("RESULTS: ");
//					for (int i = 0; i < outBytes.length; i++) {
//						System.out.print(outBytes[i] + " ");
//					}
//					System.out.println("\n");
					acres = new AccelerationData(outBytes, 1, 4);
				} else if (inOwner.equals("java/lang/Double")) {
					double[] in = new double[numOfElements];
					double[] out = new double[1];
					byte[] outBytes = new byte[8];
					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", rsk::reduce, in, out)
						.streamOut(out)
						.execute();

					acres = new AccelerationData(outBytes, 1, 8);
				} else if (inOwner.equals("java/lang/Float")) {
					float[] in = new float[numOfElements];

					float[] out = new float[1];
					byte[] outBytes = new byte[4];
					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", rsk::reduce, in, out)
						.streamOut(out)
						.execute();

					acres = new AccelerationData(outBytes, 1, 4);
				} else if (inOwner.equals("java/lang/Long")) {
					long[] in = new long[numOfElements];
//					int size;
//					if (numOfElements <= 256) {
//						size = 2;
//					} else {
//						size = numOfElements / 256 + 1;
//					}
//					size *= 4;
					long[] out = new long[1];
					byte[] outBytes = new byte[8];
					FlinkData f = new FlinkData(rsk, inputByteData, outBytes, true);

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", rsk::reduce, in, out)
						.streamOut(out)
						.execute();

//					long sum = 0;
//					for (int i = 0; i < outBytes.length; i+=8) {
//						byte[] rec = Arrays.copyOfRange(outBytes, i, i+8);
//						long fl = ByteBuffer.wrap(rec).getLong();
//						sum += fl;
//					}
//
//					byte[] res = new byte[8];
//					ByteBuffer.wrap(res).putLong(sum);
					for (int i = 0; i < outBytes.length; i += 8) {
						changeOutputEndianess8Par(outBytes, i);
					}
					acres = new AccelerationData(outBytes, 1, 8);

					//acres = new AccelerationData(res, 1, 8);
				} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
					//long infoStart = System.currentTimeMillis();
					int returnSize = 0;

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(stub.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					int size; //= inputByteData.length;
					if (numOfElements <= 256) {
						size = 2;
					} else {
						size = numOfElements / 256 + 1;
					}
					size *= 4;
//
//
//
					ArrayList<Integer> fieldSizes = fct0.getFieldSizes();
					int totalTupleSize = fct0.getReturnArrayFieldTotalBytes();
					int arrayPos = fct0.getReturnTupleArrayFieldNo();
					for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
						if (i != arrayPos) {
							if (fct0.getDifferentTypesRet()) {
								totalTupleSize += 8;
								fieldSizes.set(i, 8);
							} else {
								totalTupleSize += fct0.getFieldSizesRet().get(i);
							}
						} else {
							fieldSizes.set(arrayPos, fct0.getReturnArrayFieldTotalBytes());
						}
					}
//
					byte[] outBytes = new byte[size * totalTupleSize];
//
//					System.out.println("=== Input Bytes ===");
//					for (int i = 0; i < inputByteData.length; i++) {
//						System.out.print(inputByteData[i] + " ");
//					}
//					System.out.println();
//
					// map correctness is not dependent on input size
					byte[] tupleSize = new byte[4]; //numOfElements;
					ByteBuffer.wrap(tupleSize).putInt(numOfElements);
					changeOutputEndianess4(tupleSize);
					int arraySize = fct0.getArraySize();
					byte[] arraySizeInBytes = new byte[4];
					ByteBuffer.wrap(arraySizeInBytes).putInt(arraySize);
					changeOutputEndianess4(arraySizeInBytes);

					//long infoEnd = System.currentTimeMillis();
					//System.out.println("**** ALLRED Function UpdateAccumulator (MAP) collect information: " + (infoEnd - infoStart));
					FlinkData f = new FlinkData(true, inputByteData, tupleSize, arraySizeInBytes, outBytes);
					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
					// @formatter:off
					TaskSchedule ts = new TaskSchedule("s1")
						.flinkInfo(f)
						.prebuiltTask("t1",
							"mapOperations",
							path + "/pre-compiled/prebuilt-exus-map-reduction.cl",
							new Object[] { inputByteData, tupleSize, arraySizeInBytes, outBytes },
							new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
							defaultDevice,
							// numofthreads == arraysize of tuplefield
							new int[] { arraySize })
						.streamOut(outBytes);

					//long start = System.currentTimeMillis();
					ts.execute();
					//long end = System.currentTimeMillis();
					//System.out.println("**** ALLRED Function UpdateAccumulator (MAP) execution time: " + (end - start));

					if (isPowerOfTwo(numOfElements)) {

						int size2; // = inputByteData.length;
						if (numOfElements <= 256) {
							size2 = 2;
						} else {
							size2 = numOfElements / 256 + 1;
						}
						size2 *= 4;

						byte[] outBytes2 = new byte[size2 * totalTupleSize];


						FlinkData f2 = new FlinkData(true, inputByteData, outBytes, arraySizeInBytes, outBytes2);

						// @formatter:off
						TaskSchedule ts2 = new TaskSchedule("s2")
							.flinkInfo(f2)
							.prebuiltTask("t2",
								"reduce",
								path + "/pre-compiled/prebuilt-exus-reduction-UpdateAccum.cl",
								new Object[] { inputByteData, outBytes, arraySizeInBytes, outBytes2 },
								new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
								defaultDevice,
								new int[] { numOfElements })
							.streamOut(outBytes2);

						//long startRed = System.currentTimeMillis();
						ts2.execute();
						//long endRed = System.currentTimeMillis();
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-PowerOfTwo) execution time: " + (endRed - startRed));

//						System.out.println("=== Reduction bytes ===");
//						for (int i = 0; i < outBytes2.length; i++) {
//							System.out.print(outBytes2[i] + " ");
//						}
//						System.out.println();

						byte[] total = new byte[totalTupleSize];

						//long PRstart = System.currentTimeMillis();
						addPRField(outBytes2, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
						Array.copy(outBytes2, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
						//long PRend = System.currentTimeMillis();
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-PowerOfTwo) collect partial results: " + (PRend - PRstart));
						//changeOutputEndianess8Partial(total, 72);
//						System.out.println("=== Reduction TOTAL bytes ===");
//						for (int i = 0; i < total.length; i++) {
//							System.out.print(total[i] + " ");
//						}
//						System.out.println();

						acres.setRawData(total);
						acres.setInputSize(1);
						acres.setReturnSize(1);
						if (fct0.getReturnArrayField()) {
							acres.hasArrayField();
							acres.setLengthOfArrayField(arraySize);
							acres.setArrayFieldNo(0);
							int totalB = fct0.getReturnArrayFieldTotalBytes();
							for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
								if (i != arrayPos) {
									totalB += fct0.getFieldSizesRet().get(i);
								}
							}
							acres.setTotalBytes(totalB);
							acres.setRecordSize(totalTupleSize);
						}
					} else {
//						System.out.println("=== Total Input Bytes ===");
//						for (int i = 0; i < inputByteData.length; i++) {
//							System.out.print(inputByteData[i] + " ");
//						}
//						System.out.println();
						//long powerStart = System.currentTimeMillis();
						ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
						int powOf2 = (int) largestPowerOfTwo(numOfElements);
						//System.out.println("Largest power of two: " + powOf2);
						powerOfTwoDatasetSizes.add(powOf2);

						int remainder = numOfElements - powOf2;
						if (isPowerOfTwo(remainder)) {
							powerOfTwoDatasetSizes.add(remainder);
						} else {
							int pow2Size;
							while (!isPowerOfTwo(remainder)) {
								pow2Size = (int) largestPowerOfTwo(remainder);
								powerOfTwoDatasetSizes.add(pow2Size);
								remainder = remainder - pow2Size;
								if (isPowerOfTwo(remainder)) {
									powerOfTwoDatasetSizes.add(remainder);
								}
							}
						}
						//long powerEnd = System.currentTimeMillis();
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-Non-PowerOfTwo) calculate power-of-2 sizes: " + (powerEnd - powerStart));
//						System.out.println("Powers of 2:");
//						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
//							System.out.println(powerOfTwoDatasetSizes.get(i));
//						}
						ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();

						int bytesUsed = 0;
						// for each data set perform the reduction
						long totalRedTime = 0;
						long totalPR = 0;
						long totalSplit = 0;
						for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {

							//long splitStart = System.currentTimeMillis();
							int inputSize = powerOfTwoDatasetSizes.get(w);

							byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * totalTupleSize)); //new byte[inputSize];
							bytesUsed += (inputSize * totalTupleSize);
							//long splitEnd = System.currentTimeMillis();
							//totalSplit += (splitEnd - splitStart);

							int size2;
							if (inputSize <= 256) {
								size2 = 2;
							} else {
								size2 = inputSize / 256 + 1;
							}
							size2 *= 4;

							byte[] outBytes2 = new byte[size2 * totalTupleSize];

							FlinkData f2 = new FlinkData(true, inBytes, outBytes, arraySizeInBytes, outBytes2);

							// @formatter:off
							TaskSchedule ts2 = new TaskSchedule("s2")
								.flinkInfo(f2)
								.prebuiltTask("t2",
									"reduce",
									path + "/pre-compiled/prebuilt-exus-reduction-UpdateAccum.cl",
									new Object[] { inBytes, outBytes, arraySizeInBytes, outBytes2 },
									new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
									defaultDevice,
									new int[] { inputSize })
								.streamOut(outBytes2);
							//long startRed = System.currentTimeMillis();
							ts2.execute();
							long endRed = System.currentTimeMillis();
							//totalRedTime += (endRed - startRed);

//							System.out.println("=== Reduction bytes ===");
//							for (int i = 0; i < outBytes2.length; i++) {
//								System.out.print(outBytes2[i] + " ");
//							}
//							System.out.println();

							//long PRstart = System.currentTimeMillis();
							byte[] totalPartial = new byte[totalTupleSize];

							addPRField(outBytes2, totalPartial, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
							Array.copy(outBytes2, 0, totalPartial, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
							//long PRend = System.currentTimeMillis();
							//totalPR += (PRend - PRstart);
							//changeOutputEndianess8Partial(total, 72);
//							System.out.println("=== Reduction PARTIAL TOTAL bytes ===");
//							for (int i = 0; i < totalPartial.length; i++) {
//								System.out.print(totalPartial[i] + " ");
//							}
//							System.out.println();

							collectPowerOfTwoRedRes.add(totalPartial);
						}
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-Non-PowerOfTwo) split dataset: " + totalSplit);
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-Non-PowerOfTwo) execution time: " + totalRedTime);
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-Non-PowerOfTwo) collect Partial Results: " + totalPR);

						//long finalPRStart = System.currentTimeMillis();
						int concatSize = 0;
						for (byte[] partialRes : collectPowerOfTwoRedRes) {
							concatSize += partialRes.length;
						}

						byte[] concatRes = null;
						int destPos = 0;

						for (byte[] partialRes : collectPowerOfTwoRedRes) {
							if (concatRes == null) {
								// write first array
								concatRes = Arrays.copyOf(partialRes, concatSize);
								destPos += partialRes.length;
							} else {
								System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
								destPos += partialRes.length;
							}
						}

						byte[] total = new byte[totalTupleSize];

//						System.out.println("Concatenated results: ");
//						for (int i = 0; i < concatRes.length; i++) {
//							System.out.print(concatRes[i] + " ");
//						}
//						System.out.println("\n");

						addPRField(concatRes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
						Array.copy(concatRes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
						//long finalPREnd = System.currentTimeMillis();
						//System.out.println("**** ALLRED Function UpdateAccumulator (RED-Non-PowerOfTwo) collect partial results of PowerOfTwo executions: " + (finalPREnd - finalPRStart));

						// collect acdata for next operation
//						System.out.println("=== Reduction TOTAL bytes ===");
//						for (int i = 0; i < total.length; i++) {
//							System.out.print(total[i] + " ");
//						}
//						System.out.println();

						acres.setRawData(total);
						acres.setInputSize(1);
						acres.setReturnSize(1);
						if (fct0.getReturnArrayField()) {
							acres.hasArrayField();
							acres.setLengthOfArrayField(fct0.getArraySize());
							acres.setArrayFieldNo(0);
							int totalB = fct0.getReturnArrayFieldTotalBytes();
							for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
								if (i != arrayPos) {
									totalB += fct0.getFieldSizesRet().get(i);
								}
							}
							acres.setTotalBytes(totalB);
							acres.setRecordSize(totalTupleSize);
						}

					}
				} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {

					int returnSize = 0;

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(stub.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					if (isPowerOfTwo(numOfElements)) {
						int size; // = inputByteData.length;
						if (numOfElements <= 256) {
							size = 2;
						} else {
							size = numOfElements / 256 + 1;
						}
						size *= 4;

						byte[] outBytes = new byte[size * returnSize];

						FlinkData f = new FlinkData(true, inputByteData, outBytes);
						TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
						// @formatter:off
						TaskSchedule ts = new TaskSchedule("s5")
							.flinkInfo(f)
							.prebuiltTask("t5",
								"reduce",
								path + "/pre-compiled/prebuilt-exus-reduction-ints.cl",
								new Object[] { inputByteData, outBytes },
								new Access[] { Access.READ, Access.WRITE },
								defaultDevice,
								new int[] { numOfElements })
							.streamOut(outBytes);

						//long start = System.currentTimeMillis();
						ts.execute();
						//long end = System.currentTimeMillis();
					//	System.out.println("**** Function Evaluate execution time: " + (end - start));



						// -----------------------

						//					System.out.println("=== Reduction bytes ===");
						//					for (int i = 0; i < outBytes.length; i++) {
						//						System.out.print(outBytes[i] + " ");
						//					}
						//					System.out.println();

						byte[] total = new byte[returnSize];
						//long PRstart = System.currentTimeMillis();
						for (int i = 0; i < 4; i++) {
							addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
						}
						//long PRend = System.currentTimeMillis();
					//	System.out.println("**** Function Evaluate collect partial results: " + (PRend - PRstart));

						//					System.out.println("=== Reduction TOTAL bytes ===");
						//					for (int i = 0; i < total.length; i++) {
						//						System.out.print(total[i] + " ");
						//					}
						//					System.out.println();

						acres = new AccelerationData(total, 1, returnSize);

					} else {
						//long powerStart = System.currentTimeMillis();
						ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
						int powOf2 = (int) largestPowerOfTwo(numOfElements);
						//System.out.println("Largest power of two: " + powOf2);
						powerOfTwoDatasetSizes.add(powOf2);

						int remainder = numOfElements - powOf2;
						if (isPowerOfTwo(remainder)) {
							powerOfTwoDatasetSizes.add(remainder);
						} else {
							int pow2Size;
							while (!isPowerOfTwo(remainder)) {
								pow2Size = (int) largestPowerOfTwo(remainder);
								powerOfTwoDatasetSizes.add(pow2Size);
								remainder = remainder - pow2Size;
								if (isPowerOfTwo(remainder)) {
									powerOfTwoDatasetSizes.add(remainder);
								}
							}
						}

						//long powerEnd = System.currentTimeMillis();
					//	System.out.println("**** Function Evaluate (Non-PowerOfTwo) calculate power-of-2 sizes: " + (powerEnd - powerStart));
//						System.out.println("Powers of 2:");
//						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
//							System.out.println(powerOfTwoDatasetSizes.get(i));
//						}
						ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();

						int bytesUsed = 0;

						// for each data set perform the reduction
						long totalRedTime = 0;
						long totalPRTime = 0;
						long totalSplit = 0;
						for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {
							//long splitStart = System.currentTimeMillis();
							int inputSize = powerOfTwoDatasetSizes.get(w);

							byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * returnSize)); //new byte[inputSize];
							bytesUsed += (inputSize * returnSize);
							//long splitEnd = System.currentTimeMillis();
							//totalSplit += (splitEnd - splitStart);

							int size; // = inputByteData.length;
							if (inputSize <= 256) {
								size = 2;
							} else {
								size = inputSize / 256 + 1;
							}
							size *= 4;

							byte[] outBytes = new byte[size * returnSize];

							FlinkData f = new FlinkData(true, inBytes, outBytes);
							TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
							// @formatter:off
							TaskSchedule ts = new TaskSchedule("s5")
								.flinkInfo(f)
								.prebuiltTask("t5",
									"reduce",
									path + "/pre-compiled/prebuilt-exus-reduction-ints.cl",
									new Object[]{inputByteData, outBytes},
									new Access[]{Access.READ, Access.WRITE},
									defaultDevice,
									new int[]{inputSize})
								.streamOut(outBytes);

							//long start = System.currentTimeMillis();
							ts.execute();
							//long end = System.currentTimeMillis();
							//totalRedTime += (end - start);
							// -----------------------

							//					System.out.println("=== Reduction bytes ===");
							//					for (int i = 0; i < outBytes.length; i++) {
							//						System.out.print(outBytes[i] + " ");
							//					}
							//					System.out.println();

							byte[] totalPR = new byte[returnSize];
							//long PRStart = System.currentTimeMillis();
							for (int i = 0; i < 4; i++) {
								addPRField(outBytes, totalPR, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
							}
							//long PREnd = System.currentTimeMillis();
							//totalPRTime += (PREnd - PRStart);
							collectPowerOfTwoRedRes.add(totalPR);
						}
					//	System.out.println("**** Function Evaluate (Non-PowerOfTwo) split dataset: " + totalSplit);
					//	System.out.println("**** Function Evaluate (Non-PowerOfTwo) execution time: " + totalRedTime);
					//	System.out.println("**** Function Evaluate (Non-PowerOfTwo) collect Partial Results: " + totalPRTime);

						//long finalPRStart = System.currentTimeMillis();
						int concatSize = 0;
						for (byte[] partialRes : collectPowerOfTwoRedRes) {
							concatSize += partialRes.length;
						}

						byte[] concatRes = null;
						int destPos = 0;

						for (byte[] partialRes : collectPowerOfTwoRedRes) {
							if (concatRes == null) {
								// write first array
								concatRes = Arrays.copyOf(partialRes, concatSize);
								destPos += partialRes.length;
							} else {
								System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
								destPos += partialRes.length;
							}
						}

						byte[] total = new byte[returnSize];
						for (int i = 0; i < 4; i++) {
							addPRField(concatRes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
						}
						//long finalPREnd = System.currentTimeMillis();
						//					System.out.println("=== Reduction TOTAL bytes ===");
						//					for (int i = 0; i < total.length; i++) {
						//						System.out.print(total[i] + " ");
						//					}
						//					System.out.println();
					//	System.out.println("**** Function Evaluate (Non-PowerOfTwo) collect partial results of PowerOfTwo executions: " + (finalPREnd - finalPRStart));
						acres = new AccelerationData(total, 1, returnSize);
					}

				}

				tornadoVMCleanUp();
							
				//System.out.println("!!! FORWARD DATA AllReduceDriver outputCollector: " + this.taskContext.getOutputCollector());
				if (this.taskContext.getOutputCollector() instanceof ChainedAllReduceDriver) {
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) this.taskContext.getOutputCollector();
					((CountingCollector) collector).numRecordsOut.inc(acres.getInputSize());
					chred.collect(acres);
				} else if (this.taskContext.getOutputCollector() instanceof ChainedMapDriver) {
					ChainedMapDriver chmad = (ChainedMapDriver) this.taskContext.getOutputCollector();
					((CountingCollector) collector).numRecordsOut.inc(acres.getInputSize());
					chmad.collect(acres);
				} else {
					materializeTornadoBuffers(acres, outOwner, fct0);
				}
			}
		} else {
			// ==========================================================
			T val1;
			if ((val1 = input.next()) == null) {
				return;
			}
			numRecordsIn.inc();

			if (objectReuseEnabled) {
				// We only need two objects. The first reference stores results and is
				// eventually collected. New values are read into the second.
				T val2 = serializer.createInstance();

				T value = val1;

				while (running && (val2 = input.next(val2)) != null) {
					numRecordsIn.inc();
					value = stub.reduce(value, val2);

					// we must never read into the object returned
					// by the user, so swap the reuse objects,
					if (value == val2) {
						T tmp = val1;
						val1 = val2;
						val2 = tmp;
					}
				}

				collector.collect(value);
			} else {
				T val2;
				while (running && (val2 = input.next()) != null) {
					numRecordsIn.inc();
					val1 = stub.reduce(val1, val2);
				}
				/*if (val1 instanceof Tuple2) {
					System.out.println("%% AllReduceDriver red result: " + val1);
					Tuple2<Tuple2<double[], Integer>, Integer> t = (Tuple2<Tuple2<double[], Integer>, Integer>) val1;
					System.out.print("[");
					for (int i = 0; i < t.f0.f0.length; i++) {
						System.out.print(t.f0.f0[i] + ", ");
					}
					System.out.println("], " + t.f0.f1 + ", " + t.f1);
				} */ 
				collector.collect(val1);
				
			}
		}
	}

	private byte[] getPRIoT(byte[] partialResults, byte[] total, int fieldNo, ArrayList<Integer> fieldSizes, ArrayList<String> fieldTypes, int totalSize, boolean changeEndianess, int numTornadoFunction, int numberOfElements) {
		int offset = 0;
		if (fieldNo != 0) {
			for (int i = 0; i < fieldNo; i++) {
				offset += fieldSizes.get(i);
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
				if (numTornadoFunction == 1) {
					// MIN
					totalNumInt = Math.min(totalNumInt, ByteBuffer.wrap(b).getInt());
				} else if (numTornadoFunction == 2) {
					// MAX
					totalNumInt = Math.max(totalNumInt, ByteBuffer.wrap(b).getInt());
				} else if (numTornadoFunction == 3 || numTornadoFunction == 4) {
					// SUM
					totalNumInt += ByteBuffer.wrap(b).getInt();
				}
			} else if (fieldTypes.get(fieldNo).equals("double")) {
				byte[] b = new byte[8];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess8(b);
				if (numTornadoFunction == 1) {
					// MIN
					totalNumDouble = Math.min(totalNumDouble, ByteBuffer.wrap(b).getDouble());
				} else if (numTornadoFunction == 2) {
					// MAX
					totalNumDouble = Math.max(totalNumDouble, ByteBuffer.wrap(b).getDouble());
				} else if (numTornadoFunction == 3 || numTornadoFunction == 4) {
					// SUM
					totalNumDouble += ByteBuffer.wrap(b).getDouble();
				}
			} else if (fieldTypes.get(fieldNo).equals("float")) {
				byte[] b = new byte[4];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess4(b);
				if (numTornadoFunction == 1) {
					// MIN
					totalNumFloat = Math.min(totalNumFloat, ByteBuffer.wrap(b).getFloat());
				} else if (numTornadoFunction == 2) {
					// MAX
					totalNumFloat = Math.max(totalNumFloat, ByteBuffer.wrap(b).getFloat());
				} else if (numTornadoFunction == 3 || numTornadoFunction == 4) {
					// SUM
					totalNumFloat += ByteBuffer.wrap(b).getFloat();
				}
			} else if (fieldTypes.get(fieldNo).equals("long")) {
				byte[] b = new byte[8];
				ByteBuffer.wrap(partialResults, (offset + totalSize*j), fieldSize).get(b);
				changeOutputEndianess8(b);
				if (numTornadoFunction == 1) {
					// MIN
					totalNumLong = Math.min(totalNumLong, ByteBuffer.wrap(b).getLong());
				} else if (numTornadoFunction == 2) {
					// MAX
					totalNumLong = Math.max(totalNumLong, ByteBuffer.wrap(b).getLong());
				} else if (numTornadoFunction == 3 || numTornadoFunction == 4) {
					// SUM
					totalNumLong += ByteBuffer.wrap(b).getLong();
				}
			}
			j++;
		}
		if (numTornadoFunction == 4) {
			if (fieldTypes.get(fieldNo).equals("int")) {
				totalNumInt /= numberOfElements;
			} else if (fieldTypes.get(fieldNo).equals("double")) {
				totalNumDouble /= numberOfElements;
			} else if (fieldTypes.get(fieldNo).equals("float")) {
				totalNumFloat /= numberOfElements;
			} else if (fieldTypes.get(fieldNo).equals("long")) {
				totalNumLong /= numberOfElements;
			}
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

		return total;
	}

	private String getPreCompiledFunction(int numFunction) {
		String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);

		switch (numFunction) {
			case 1:
				return path + "/pre-compiled/simple-double-reduce-min.cl";
			case 2:
				return path + "/pre-compiled/simple-double-reduce-max.cl";
			case 3:
				return path + "/pre-compiled/simple-double-reduce-sum.cl";
			case 4:
				return path + "/pre-compiled/simple-double-reduce-avg.cl";
			default:
				throw new RuntimeException("[ERROR] Function not recognised");
		}
	}

	private int getReductionType(String className) {
		switch (className) {
			case "ReduceMin":
				return 1;
			case "ReduceMax":
				return 2;
			case "ReduceSum":
				return 3;
			case "ReduceAvg":
				return 4;
		}
		return 0;
	}

	void materializeTornadoBuffers (AccelerationData acres, String type, FlinkCompilerInfo fct) {
		byte[] out = acres.getRawData();
		//System.out.println(">> ALLRED materialize");
		if (acres.getArrayField()) {
			//System.out.println(">>ALL RED materialize: has array field");
			long total = 0;
			int numOfBytes = 0;
			int lengthOfArrayField = acres.getLengthOfArrayField();
			int arrayFieldNo = acres.getArrayFieldNo();
			int totalBytes = acres.getTotalBytes();
			int recordSize = acres.getRecordSize();
			ArrayList<Integer> tupleReturnSizes = acres.getReturnFieldSizes();
			boolean diffReturnSize = acres.isDifferentReturnTupleFields();
			//long endianessStart = System.currentTimeMillis();
			changeEndianess(true, out, recordSize, diffReturnSize, tupleReturnSizes);
			//long endianessEnd = System.currentTimeMillis();
			//System.out.println("**** Function " + this.mapper + " change endianess of result buffer: " + (endianessEnd - endianessStart));
			while (numOfBytes < out.length) {
				//long breakFieldsStart = System.currentTimeMillis();
				byte[] recordBytes = getReturnRecordArrayField(out, numOfBytes, tupleReturnSizes,lengthOfArrayField,arrayFieldNo, totalBytes, diffReturnSize);
				//long breakFieldsEnd = System.currentTimeMillis();
				if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
				//	System.out.println("ALLREDUCE WRITE RECORD IN COUNTINGCOLLECTOR");
					((CountingCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				} else if (this.taskContext.getOutputCollector() instanceof WorksetUpdateOutputCollector) {
				//	System.out.println("ALLREDUCE WRITE RECORD IN WORKSETUPDATEOUTPUTCOLLECTOR");
					((WorksetUpdateOutputCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				} else if (this.taskContext.getOutputCollector() instanceof OutputCollector) {
				//	System.out.println("ALLREDUCE WRITE RECORD IN OUTPUTCOLLECTOR");
					((OutputCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				}
				numOfBytes += recordSize;
				//total += (breakFieldsEnd - breakFieldsStart);
			}
			//System.out.println("**** Function " + this.mapper + " break data into records: " + total);
			return;
		}

		if (type.equals("java/lang/Integer")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Integer in = bf.getInt(i);
				this.taskContext.getOutputCollector().collect((T) in);
				//records.add((OT) in);
			}
		} else if (type.equals("java/lang/Double")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Double d = bf.getDouble(i);
				this.taskContext.getOutputCollector().collect((T) d);
				//records.add((OT) d);
			}
		} else if (type.equals("java/lang/Float")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Float f = bf.getFloat(i);
				this.taskContext.getOutputCollector().collect((T) f);
				//records.add((OT) f);
			}
		} else if (type.equals("java/lang/Long")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Long l = bf.getLong(i);
				this.taskContext.getOutputCollector().collect((T) l);
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
				if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
					((CountingCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
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

			if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
				if (((CountingCollector) this.taskContext.getOutputCollector()).getCollector() instanceof ChainedReduceCombineDriver) {
					DataTransformation<T> dtrans = new DataTransformation();
					//long materializeStart = System.currentTimeMillis();
					ArrayList<T> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					//long materializeEnd = System.currentTimeMillis();
					//System.out.println("**** Function " + this.mapper + " materialize data to be sent to groupBy: " + (materializeEnd - materializeStart));
					for (T record : records) {
						(((CountingCollector<T>) this.taskContext.getOutputCollector()).getCollector()).collect(record);
					}
				} else {
					DataTransformation dtrans = new DataTransformation();
					for (int i = 0; i < out.length; i += sizeOftuple) {
						byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
						((CountingCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
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

			// KMeans
			// TODO: include this option in the other cases
			if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
				if (((CountingCollector) this.taskContext.getOutputCollector()).getCollector() instanceof ChainedReduceCombineDriver) {
					DataTransformation<T> dtrans = new DataTransformation();
					ArrayList<T> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					for (T record : records) {
						(((CountingCollector<T>) this.taskContext.getOutputCollector()).getCollector()).collect(record);
					}
				} else {
					DataTransformation dtrans = new DataTransformation();
					for (int i = 0; i < out.length; i += sizeOftuple) {
						//long breakRecordsStart = System.currentTimeMillis();
						byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
						//long breakRecordsEnd = System.currentTimeMillis();
						((CountingCollector)this.taskContext.getOutputCollector()).collect(recordBytes);
						//total += (breakRecordsEnd - breakRecordsStart);
					}
				}
			} else if (this.taskContext.getOutputCollector() instanceof OutputCollector) {
				DataTransformation dtrans = new DataTransformation();
				for (int i = 0; i < out.length; i += sizeOftuple) {
					//long breakRecordsStart = System.currentTimeMillis();
					byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
					//long breakRecordsEnd = System.currentTimeMillis();
					((OutputCollector)this.taskContext.getOutputCollector()).collect(recordBytes);
					//total += (breakRecordsEnd - breakRecordsStart);
				}
			}
			//System.out.println("**** Function " + this.mapper + " break data into records: " + total);
		}
	}

	public byte[] changeEndianess(boolean arrayField, byte[] b, int recordSize, boolean diffReturnSize, ArrayList<Integer> tupleReturnSizes) {
		int off = 0;
		if (arrayField) {
			if (diffReturnSize) {
				for (int i = 0; i < b.length; i += 8) {
					changeOutputEndianess8Par(b, i);
				}
			} else {
				for (int i = 0; i < b.length; i += 4) {
					changeOutputEndianess4Par(b, i);
				}
			}
			return b;
		}
		if (diffReturnSize) {
			for (int i = 0; i < b.length; i += recordSize) {
				for (int j = 0; j < tupleReturnSizes.size(); j++) {
					changeOutputEndianess8Par(b, off + i);
					off += 8;
				}
				off = 0;
			}
		} else {
			for (int i = 0; i < b.length; i += recordSize) {
				for (int j = 0; j < tupleReturnSizes.size(); j++) {
					if (tupleReturnSizes.get(j) == 4) {
						changeOutputEndianess4Par(b, off + i);
						off += 4;
					} else if (tupleReturnSizes.get(j) == 8) {
						changeOutputEndianess8Par(b, off + i);
						off += 8;
					}
				}
				off = 0;
			}
		}
		return b;
	}

	public byte[] getReturnRecordArrayField(byte[] results, int from, ArrayList<Integer> tupleReturnSizes, int lengthOfArrayField, int arrayFieldNo, int tupleTotalBytes, boolean differentTypes) {
		int numOfFields = tupleReturnSizes.size();

		// plus 4 bytes for the array header
		byte[] record = new byte[tupleTotalBytes + 4];

		int k = 0;
		for (int i = 0; i < numOfFields; i++) {
			if (i == arrayFieldNo) {
				byte[] header = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(lengthOfArrayField).array();
				for (int w = 0; w < lengthOfArrayField; w++) {
					if (w == 0) {
						record[k] = header[0];
						record[k + 1] = header[1];
						record[k + 2] = header[2];
						record[k + 3] = header[3];
						k = k + 4;
					}

					if (tupleReturnSizes.get(i) == 4) {
						int start;
						if (differentTypes) {
							start = from + 4 + 8 * i + 8 * w;
						} else {
							// FIXME: check more thoroughly if correct
							start = from + 4 * i + 4 * w;
						}
						for (int j = start; j < start + 4; j++) {
							record[k] = results[j];
							k++;
						}
					} else {
						int start = from + 8 * i + 8 * w;

						for (int j = start; j < start + 8; j++) {
							record[k] = results[j];
							k++;
						}
					}
				}
			} else {
				if (tupleReturnSizes.get(i) == 4) {
					int start;
					if (differentTypes) {
						start = from + 4 + (8 * lengthOfArrayField) + (i - 1) * 8;
					} else {
						// FIXME: check more thoroughly if correct
						start = from + 4 * i * lengthOfArrayField;
					}
					for (int j = start; j < start + 4; j++) {
						record[k] = results[j];
						k++;
					}
				} else {
					int start = from + 8 * i * lengthOfArrayField;
					for (int j = start; j < start + 8; j++) {
						record[k] = results[j];
						k++;
					}
				}
			}
		}

		return record;
	}

	public long largestPowerOfTwo(long inputSize) {
		if (inputSize < 1) {
			System.out.println("INPUT SIZE < 1");
			return -1;
		}

		long temp = inputSize;
		if (inputSize % 2 == 0) {
			temp = inputSize - 1;
		}
		long power = (long) (Math.log(temp) / Math.log(2));
		long result = (long) Math.pow(2, power);
		return result;
	}

	public static boolean isPowerOfTwo(int inputSize) {
		boolean isPow2 = (int) (Math.ceil((Math.log(inputSize) / Math.log(2)))) == (int) (Math.floor(((Math.log(inputSize) / Math.log(2)))));
		return isPow2;
	}

	// Add partial results of reduction
	public static void addPRField(byte[] partialResults, byte[] total, int fieldNo, ArrayList<Integer> fieldSizes, ArrayList<String> fieldTypes, int totalSize, boolean changeEndianess) {
		int offset = 0;
		if (fieldNo != 0) {
			for (int i = 0; i < fieldNo; i++) {
				offset += fieldSizes.get(i);
			}
		}

		int totalNumInt = 0, totalNumDouble = 0, totalNumFloat = 0, totalNumLong = 0;

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

	public static void changeOutputEndianess4Par(byte[] output, int i) {
		byte tmp;
		// swap 0 and 3
		tmp = output[i];
		output[i] = output[i + 3];
		output[i + 3] = tmp;
		// swap 1 and 2
		tmp = output[i + 1];
		output[i + 1] = output[i + 2];
		output[i + 2] = tmp;
	}

	public static void changeOutputEndianess8Par(byte[] output, int i) {
		byte tmp;
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

	private void tornadoVMCleanUp() {
		for (int k = 0; k < TornadoRuntime.getTornadoRuntime().getNumDrivers(); k++) {
			final TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(k);
			for (int j = 0; j < driver.getDeviceCount(); j++) {
				driver.getDevice(j).reset();
			}
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {
		this.running = false;
	}
}
