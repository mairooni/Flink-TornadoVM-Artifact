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

import org.apache.flink.api.asm.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;

import org.apache.flink.flinktornadolog.FlinkTornadoVMLogger;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.apache.flink.runtime.tornadovm.DataTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;
import uk.ac.manchester.tornado.api.*;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

//import static org.apache.flink.api.asm.ExamineUDF.inOwner;
import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.*;

public class ChainedAllReduceDriver<IT> extends ChainedDriver<IT, IT> {
	private static final Logger LOG = LoggerFactory.getLogger(ChainedAllReduceDriver.class);

	// --------------------------------------------------------------------------------------------
	private ReduceFunction<IT> reducer;
	private TypeSerializer<IT> serializer;

	private IT base;

	private AccelerationData acres = new AccelerationData();

	private boolean tornadoEx;

	private byte[] inputByteData;

	private int numOfElements;

	private FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

	private boolean tornado = Boolean.parseBoolean(System.getProperty("tornado", "false"));
	private boolean precompiled = Boolean.parseBoolean(System.getProperty("precompiledKernel", "false"));
	public static final boolean BREAKDOWN = Boolean.parseBoolean(System.getProperties().getProperty("flinktornado.breakdown", "false"));
	private String inOwner;

	// --------------------------------------------------------------------------------------------
	@Override
	public void setup(AbstractInvokable parent) {
		final ReduceFunction<IT> red = BatchTask.instantiateUserCode(this.config, userCodeClassLoader, ReduceFunction.class);
		this.reducer = red;
		FunctionUtils.setFunctionRuntimeContext(red, getUdfRuntimeContext());

		TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ChainedAllReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.reducer, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		BatchTask.closeUserCode(this.reducer);
	}

	@Override
	public void cancelTask() {
		try {
			FunctionUtils.closeFunction(this.reducer);
		} catch (Throwable t) {
			// Ignore exception.
		}
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public Function getStub() {
		return this.reducer;
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------

	public void collect (AccelerationData acdata) {
		boolean tornadoConfFlag = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTornadoVMFlag();
		boolean flinkReductions = Boolean.parseBoolean(System.getProperty("flinkReductions", "false"));
		if ((tornado || tornadoConfFlag) && !flinkReductions) {
			//System.out.println("%%%%%%% CHAINEDREDUCE: " + tornadoConfFlag);
			String reduceUserClassName = this.reducer.getClass().toString().replace("class ", "").replace(".", "/");
			Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesReduce.getTypes(reduceUserClassName);
			inOwner = inOutTypes.f0;
			// TODO: Change to getting byte array from configuration and loading it
			//TornadoReduce rsk = AcceleratedFlinkUserFunction.getTornadoReduce();
			TornadoReduce rsk;
			Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
			if (BREAKDOWN) {
				long skelStart = System.currentTimeMillis();
				byte[] def = new byte[0];
				byte[] bytesRsk = conf.getSkeletonReduceBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleReduce mdr = loader.loadClassRed("org.apache.flink.api.asm.ReduceASMSkeleton", bytesRsk);
				rsk = new TornadoReduce(mdr);
				long skelEnd = System.currentTimeMillis();
				FlinkTornadoVMLogger.printBreakDown("**** Function " + reducer + " create ASM skeleton: " + (skelEnd - skelStart));
			} else {
				byte[] def = new byte[0];
				byte[] bytesRsk = conf.getSkeletonReduceBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleReduce mdr = loader.loadClassRed("org.apache.flink.api.asm.ReduceASMSkeleton", bytesRsk);
				rsk = new TornadoReduce(mdr);
			}
			inputByteData = acdata.getRawData();
			numOfElements = acdata.getInputSize();
			numRecordsIn.inc(numOfElements);
			//String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
			String path = "./";

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
				//fct0 = new FlinkCompilerInfo();

				double[] in = new double[numOfElements];
				double[] out = new double[1];

				int size; //= inputByteData.length;
				if (numOfElements <= 256) {
					size = 2;
				} else {
					size = numOfElements / 256 + 1;
				}
				size *= 4;
				byte[] outBytes = new byte[8*size];

				FlinkData f = new FlinkData(inputByteData, outBytes);
				new TaskSchedule("s0")
					.flinkCompilerData(fct0)
					.flinkInfo(f)
					.task("t0", rsk::reduce, in, out)
					.streamOut(outBytes)
					.execute();

//				System.out.println("== outBytes:");
//				for (int i = 0; i < outBytes.length; i++) {
//					System.out.print(outBytes[i] + " ");
//				}
//				System.out.println("\n");

				byte[] total = new byte[8];
				double totalNumDouble = 0.0;
				int j = 0;
				for (int i = 0; i < outBytes.length; i+=8) {
					byte[] b = new byte[8];
					ByteBuffer.wrap(outBytes, 8*j, 8).get(b);
					changeOutputEndianess8(b);
					totalNumDouble += ByteBuffer.wrap(b).getDouble();
					j++;
				}

				byte[] bytesOfField = new byte[8];
				ByteBuffer.wrap(bytesOfField).putDouble(totalNumDouble);

				//changeOutputEndianess8(bytesOfField);

				for (int i = 0; i < 8; i++) {
					total[i] = bytesOfField[i];
				}

//				for (int i = 0; i < outBytes.length; i += 8) {
//					changeOutputEndianess8Par(outBytes, i);
//				}

				acres = new AccelerationData(total, 1, 8);
			} else if (inOwner.equals("java/lang/Float")) {
				float[] in = new float[numOfElements];

				float[] out = new float[1];
				byte[] outBytes = new byte[4];
				//FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
				FlinkData f = new FlinkData(rsk, inputByteData, outBytes, true);

				new TaskSchedule("s0")
					.flinkCompilerData(fct0)
					.flinkInfo(f)
					.task("t0", rsk::reduce, in, out)
					.streamOut(out)
					.execute();

				for (int i = 0; i < outBytes.length; i += 4) {
					changeOutputEndianess4Par(outBytes, i);
				}
				acres = new AccelerationData(outBytes, 1, 4);
			} else if (inOwner.equals("java/lang/Long")) {
				if (precompiled) {
					int returnSize = 8;

					int size; // = inputByteData.length;
					if (numOfElements <= 256) {
						size = 2;
					} else {
						size = numOfElements / 256 + 1;
					}
					size *= 4;

					byte[] outBytes = new byte[size * returnSize];

					byte[] sizeb = new byte[4]; //numOfElements;
					ByteBuffer.wrap(sizeb).putInt(numOfElements);
					changeOutputEndianess4(sizeb);

					FlinkData f = new FlinkData(true, inputByteData, outBytes);
					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(2); //TornadoRuntime.getTornadoRuntime().getDefaultDevice();
					// @formatter:off
					TaskSchedule ts = new TaskSchedule("s1")
						.flinkInfo(f)
						.prebuiltTask("t1",
							"reduce",
							path + "/pre-compiled/pi-reduce.cl",
							new Object[]{inputByteData, outBytes},
							new Access[]{Access.READ, Access.WRITE},
							defaultDevice,
							new int[]{numOfElements})
						.streamOut(outBytes);

					//	long start = System.currentTimeMillis();
					ts.execute();
					long totalNumLong = 0;
					for (int i = 0; i < outBytes.length; i+=8) {
						byte[] b = new byte[8];
						ByteBuffer.wrap(outBytes, i, 8).get(b);
						changeOutputEndianess8(b);
						totalNumLong += ByteBuffer.wrap(b).getLong();
					}

					byte[] bytesOfField = new byte[8];
					ByteBuffer.wrap(bytesOfField).putLong(totalNumLong);

					//changeOutputEndianess8(bytesOfField);
					acres = new AccelerationData(bytesOfField, 1, 8);

				} else {
					long[] in = new long[numOfElements];
//				int size;
//				if (numOfElements <= 256) {
//					size = 2;
//				} else {
//					size = numOfElements / 256 + 1;
//				}
//				size *= 4;
					long[] out = new long[1];
					byte[] outBytes = new byte[8];
					FlinkData f = new FlinkData(rsk, inputByteData, outBytes, true);

					new TaskSchedule("s1")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t1", rsk::reduce, in, out)
						.streamOut(out)
						.execute();

//				long sum = 0;
//				for (int i = 0; i < outBytes.length; i+=8) {
//					byte[] rec = Arrays.copyOfRange(outBytes, i, i + 8);
//					byte[] rev = changeOutputEndianess8(rec);
//					long fl = ByteBuffer.wrap(rev).getLong();
//					sum += fl;
//				}
					for (int i = 0; i < outBytes.length; i += 8) {
						changeOutputEndianess8Par(outBytes, i);
					}
					//byte[] res = new byte[8];
					//ByteBuffer.wrap(res).putLong(sum);

					acres = new AccelerationData(outBytes, 1, 8);
				}
			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
				boolean compiledReduction = true;
				if (compiledReduction) {
					int returnSize = 0;
					acres = new AccelerationData();

					fct0 = new FlinkCompilerInfo();
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						int ref = name.indexOf("@");
						String classname = name.substring(0, ref);
						if (classname.equals(reducer.getClass().toString().replace("class ", ""))) {
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
					int arraySize = fct0.getArraySize();
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

					worker.setGlobalWork(numOfElements, 1, 1);
					long start, end;
					if (BREAKDOWN) {
						start = System.currentTimeMillis();
						ts.execute(gridScheduler);
						end = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** Function " + reducer + " execution time: " + (end - start));
					} else {
						ts.execute(gridScheduler);
					}

					//	.execute();
//					for (int i = 0; i < outBytes.length; i++) {
//						System.out.print(outBytes[i] + " ");
//					}
//					System.out.println("\n");
					byte[] total = new byte[totalTupleSize];


					if (BREAKDOWN) {
						long PRstart, PRend;
						PRstart = System.currentTimeMillis();
						addPRField(outBytes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
						Array.copy(outBytes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
						PRend = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** Function " + reducer + " collect partial results: " + (PRend - PRstart));
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
//						acres.setRecordSize(tsize);
//						//int numOfElementsRes = outBytes.length / tsize;
//						acres.setInputSize(1);
//						acres.setReturnSize(tsize);
//					} else {
//						acres = new AccelerationData();
//						int numOfElementsRes = 1;//outBytes.length / returnSize;
//						acres.setReturnSize(returnSize);
//						acres.setInputSize(numOfElementsRes);
//					}
//					acres.setRawData(outBytes);
				} else {
					//long infoStart = System.currentTimeMillis();
					int returnSize = 0;

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
//
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
					//System.out.println("**** Function UpdateAccumulator (MAP) collect information: " + (infoEnd - infoStart));
					FlinkData f = new FlinkData(true, inputByteData, tupleSize, arraySizeInBytes, outBytes);
					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(2);//TornadoRuntime.getTornadoRuntime().getDefaultDevice();
					// @formatter:off
					TaskSchedule ts = new TaskSchedule("s1")
						.flinkInfo(f)
						.prebuiltTask("t1",
							"mapOperations",
							path + "/pre-compiled/prebuilt-exus-map-reduction.cl",
							new Object[]{inputByteData, tupleSize, arraySizeInBytes, outBytes},
							new Access[]{Access.READ, Access.READ, Access.READ, Access.WRITE},
							defaultDevice,
							// numofthreads == arraysize of tuplefield
							new int[]{arraySize})
						.streamOut(outBytes);

					//long start = System.currentTimeMillis();
					ts.execute();
					//long end = System.currentTimeMillis();
					//System.out.println("**** Function UpdateAccumulator (MAP) execution time: " + (end - start));

					if (!isPowerOfTwo(numOfElements)) {
						double log = Math.log(numOfElements) / Math.log(2);
						int exp = (int) log;
						numOfElements = (int) Math.pow(2, exp + 1);
					}

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
							new Object[]{inputByteData, outBytes, arraySizeInBytes, outBytes2},
							new Access[]{Access.READ, Access.READ, Access.READ, Access.WRITE},
							defaultDevice,
							new int[]{numOfElements})
						.streamOut(outBytes2);

					//long startRed = System.currentTimeMillis();
					ts2.execute();
					//long endRed = System.currentTimeMillis();
					//System.out.println("**** Function UpdateAccumulator (RED-PowerOfTwo) execution time: " + (endRed - startRed));

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
					//System.out.println("**** Function UpdateAccumulator (RED-PowerOfTwo) collect partial results: " + (PRend - PRstart));
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
				}
				//}
//				} else {
////						System.out.println("=== Total Input Bytes ===");
////						for (int i = 0; i < inputByteData.length; i++) {
////							System.out.print(inputByteData[i] + " ");
////						}
////						System.out.println();
//					double log = Math.log(numOfElements) / Math.log(2);
//					int exp = (int) log;
//					int numberOfElements = (int) Math.pow(2, exp + 1);
//					long powerStart = System.currentTimeMillis();
//					ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
//					int powOf2 = (int) largestPowerOfTwo(numOfElements);
//					//System.out.println("Largest power of two: " + powOf2);
//					powerOfTwoDatasetSizes.add(powOf2);
//
//					int remainder = numOfElements - powOf2;
//					if (isPowerOfTwo(remainder)) {
//						powerOfTwoDatasetSizes.add(remainder);
//					} else {
//						int pow2Size;
//						while (!isPowerOfTwo(remainder)) {
//							pow2Size = (int) largestPowerOfTwo(remainder);
//							powerOfTwoDatasetSizes.add(pow2Size);
//							remainder = remainder - pow2Size;
//							if (isPowerOfTwo(remainder)) {
//								powerOfTwoDatasetSizes.add(remainder);
//							}
//						}
//					}
//					long powerEnd = System.currentTimeMillis();
//					System.out.println("**** Function UpdateAccumulator (RED-Non-PowerOfTwo) calculate power-of-2 sizes: " + (powerEnd - powerStart));
////						System.out.println("Powers of 2:");
////						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
////							System.out.println(powerOfTwoDatasetSizes.get(i));
////						}
//					ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();
//
//					int bytesUsed = 0;
//					// for each data set perform the reduction
//					long totalRedTime = 0;
//					long totalPR = 0;
//					long totalSplit = 0;
//					for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {
//
//						long splitStart = System.currentTimeMillis();
//						int inputSize = powerOfTwoDatasetSizes.get(w);
//
//						byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * totalTupleSize)); //new byte[inputSize];
//						bytesUsed += (inputSize * totalTupleSize);
//						long splitEnd = System.currentTimeMillis();
//						totalSplit += (splitEnd - splitStart);
//
//						int size2;
//						if (inputSize <= 256) {
//							size2 = 2;
//						} else {
//							size2 = inputSize / 256 + 1;
//						}
//						size2 *= 4;
//
//						byte[] outBytes2 = new byte[size2 * totalTupleSize];
//
//						FlinkData f2 = new FlinkData(true, inBytes, outBytes, arraySizeInBytes, outBytes2);
//
//						// @formatter:off
//						TaskSchedule ts2 = new TaskSchedule("s2")
//							.flinkInfo(f2)
//							.prebuiltTask("t2",
//								"reduce",
//								"./pre-compiled/prebuilt-exus-reduction-UpdateAccum-float.cl",
//								new Object[] { inBytes, outBytes, arraySizeInBytes, outBytes2 },
//								new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
//								defaultDevice,
//								new int[] { inputSize })
//							.streamOut(outBytes2);
//						long startRed = System.currentTimeMillis();
//						ts2.execute();
//						long endRed = System.currentTimeMillis();
//						totalRedTime += (endRed - startRed);
//
////							System.out.println("=== Reduction bytes ===");
////							for (int i = 0; i < outBytes2.length; i++) {
////								System.out.print(outBytes2[i] + " ");
////							}
////							System.out.println();
//
//						long PRstart = System.currentTimeMillis();
//						byte[] totalPartial = new byte[totalTupleSize];
//
//						addPRField(outBytes2, totalPartial, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
//						Array.copy(outBytes2, 0, totalPartial, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
//						long PRend = System.currentTimeMillis();
//						totalPR += (PRend - PRstart);
//						//changeOutputEndianess8Partial(total, 72);
////							System.out.println("=== Reduction PARTIAL TOTAL bytes ===");
////							for (int i = 0; i < totalPartial.length; i++) {
////								System.out.print(totalPartial[i] + " ");
////							}
////							System.out.println();
//
//						collectPowerOfTwoRedRes.add(totalPartial);
//					}
//					System.out.println("**** Function UpdateAccumulator (RED-Non-PowerOfTwo) split dataset: " + totalSplit);
//					System.out.println("**** Function UpdateAccumulator (RED-Non-PowerOfTwo) execution time: " + totalRedTime);
//					System.out.println("**** Function UpdateAccumulator (RED-Non-PowerOfTwo) collect Partial Results: " + totalPR);
//
//					long finalPRStart = System.currentTimeMillis();
//					int concatSize = 0;
//					for (byte[] partialRes : collectPowerOfTwoRedRes) {
//						concatSize += partialRes.length;
//					}
//
//					byte[] concatRes = null;
//					int destPos = 0;
//
//					for (byte[] partialRes : collectPowerOfTwoRedRes) {
//						if (concatRes == null) {
//							// write first array
//							concatRes = Arrays.copyOf(partialRes, concatSize);
//							destPos += partialRes.length;
//						} else {
//							System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
//							destPos += partialRes.length;
//						}
//					}
//
//					byte[] total = new byte[totalTupleSize];
//
////						System.out.println("Concatenated results: ");
////						for (int i = 0; i < concatRes.length; i++) {
////							System.out.print(concatRes[i] + " ");
////						}
////						System.out.println("\n");
//
//					addPRField(concatRes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
//					Array.copy(concatRes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
//					long finalPREnd = System.currentTimeMillis();
//					System.out.println("**** Function UpdateAccumulator (RED-Non-PowerOfTwo) collect partial results of PowerOfTwo executions: " + (finalPREnd - finalPRStart));
//
//					// collect acdata for next operation
////						System.out.println("=== Reduction TOTAL bytes ===");
////						for (int i = 0; i < total.length; i++) {
////							System.out.print(total[i] + " ");
////						}
////						System.out.println();
//
//					acres.setRawData(total);
//					acres.setInputSize(1);
//					acres.setReturnSize(1);
//					if (fct0.getReturnArrayField()) {
//						acres.hasArrayField();
//						acres.setLengthOfArrayField(fct0.getArraySize());
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
//
//				}
			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
				boolean prebuilt = false;
				if (prebuilt) {
					int returnSize = 0;
					acres = new AccelerationData();

					fct0 = new FlinkCompilerInfo();
					returnSize = 32;
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
					System.out.println("INPUT");
					for (int i = 0; i < inputByteData.length; i++) {
						System.out.print(inputByteData[i] + " ");
					}
					System.out.println("\n");
					byte[] outBytes = new byte[size * returnSize];
					//String path = System.getProperty("user.dir");
					//String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
					FlinkData f = new FlinkData(true, inputByteData, outBytes);
					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(2);//TornadoRuntime.getTornadoRuntime().getDefaultDevice();
					// @formatter:off
					TaskSchedule ts = new TaskSchedule("s8")
						.flinkInfo(f)
						.prebuiltTask("t8",
							"reduce",
							"./pre-compiled/prebuilt-centroidAccum.cl",
							//	"./pre-compiled/prebuilt-centroidAccum.cl",
							new Object[]{inputByteData, outBytes},
							new Access[]{Access.READ, Access.WRITE},
							defaultDevice,
							new int[]{numOfElements})
						.streamOut(outBytes);

					//long startRed = System.currentTimeMillis();
					ts.execute();
					System.out.println("== outBytes:");
					for (int i = 0; i < outBytes.length; i++) {
						System.out.print(outBytes[i] + " ");
					}
					System.out.println("\n");
					//long endRed = System.currentTimeMillis();
					//totalRedTime += (endRed - startRed);

					//long PRstart = System.currentTimeMillis();

				} else {
					int returnSize = 0;
					acres = new AccelerationData();

					fct0 = new FlinkCompilerInfo();
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						int ref = name.indexOf("@");
						String classname = name.substring(0, ref);
						if (classname.equals(reducer.getClass().toString().replace("class ", ""))) {
							//System.out.println("-> name: " + name + " mapper: " + mapper.getClass().toString().replace("class ", ""));
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
					System.out.println("Return size " + returnSize);
					byte[] outBytes = new byte[size * returnSize];
					//byte[] outBytes = new byte[returnSize * numOfElements];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					//FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					Tuple3[] in = new Tuple3[numOfElements];
					Tuple3[] out = new Tuple3[1];

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", rsk::reduce, in, out)
						.streamOut(outBytes)
						.execute();
//					System.out.println("== outBytes:");
//					for (int i = 0; i < outBytes.length; i++) {
//						System.out.print(outBytes[i] + " ");
//					}
//					System.out.println("\n");

					byte[] total = new byte[returnSize];
					//long PRstart = System.currentTimeMillis();
					for (int i = 0; i < 8; i++) {
						total[i] = outBytes[i];
					}
					for (int i = 1; i < 4; i++) {
						addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
					}
					System.out.println("== total:");
					for (int i = 0; i < total.length; i++) {
						System.out.print(total[i] + " ");
					}
					System.out.println("\n");
					acres = new AccelerationData(total, 1, returnSize);
				}

			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
				boolean compiledReduction = true;
				if (compiledReduction) {
					int returnSize = 0;
					acres = new AccelerationData();

					fct0 = new FlinkCompilerInfo();
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						int ref = name.indexOf("@");
						String classname = name.substring(0, ref);
						if (classname.equals(reducer.getClass().toString().replace("class ", ""))) {
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
					byte[] outBytes = new byte[size * returnSize];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					//FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

					Tuple4[] in = new Tuple4[1];
					Tuple4[] out = new Tuple4[1];
					TaskSchedule ts;
					byte[] def2 = new byte[0];
					byte[] bytesRsk2 = conf.getSkeletonReduceBytes2(def2);

					WorkerGrid worker = new WorkerGrid1D(numOfElements);

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
							FlinkTornadoVMLogger.printBreakDown("**** Function " +  reducer + " execution time: " + (end - start));
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
							FlinkTornadoVMLogger.printBreakDown("**** Function " + reducer + " collect partial results: " + (PRend - PRstart));
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
						worker.setGlobalWork(numOfElements, 1, 1);
						ts.execute(gridScheduler);

						byte[] total = new byte[returnSize];
						//long PRstart = System.currentTimeMillis();
						String className = reducer.getClass().getSimpleName();
						int numTornadoFunction = getReductionType(className);
						Array.copy(outBytes, 0, total, 0, 8);
						getPRIoT(outBytes, total, 1, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true, numTornadoFunction, numOfElements);
						Array.copy(outBytes, 16, total, 16, 8);
						acres = new AccelerationData(total, 1, returnSize);
					}


					//acres = new AccelerationData();
					//int numOfElementsRes = outBytes.length / returnSize;
					//acres.setRawData(outBytes);
					//acres.setInputSize(numOfElementsRes);
					//acres.setReturnSize(returnSize);
				} else {

					int returnSize = 0;

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					if (!isPowerOfTwo(numOfElements)) {
						double log = Math.log(numOfElements) / Math.log(2);
						int exp = (int) log;
						numOfElements = (int) Math.pow(2, exp + 1);
					}
					int size; // = inputByteData.length;
					if (numOfElements <= 256) {
						size = 2;
					} else {
						size = numOfElements / 256 + 1;
					}
					size *= 4;

					byte[] outBytes = new byte[size * returnSize];

					FlinkData f = new FlinkData(true, inputByteData, outBytes);
					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(2);//TornadoRuntime.getTornadoRuntime().getDefaultDevice();
					// @formatter:off
					TaskSchedule ts = new TaskSchedule("s5")
						.flinkInfo(f)
						.prebuiltTask("t5",
							"reduce",
							path + "/pre-compiled/prebuilt-exus-reduction-ints.cl",
							new Object[]{inputByteData, outBytes},
							new Access[]{Access.READ, Access.WRITE},
							defaultDevice,
							new int[]{numOfElements})
						.streamOut(outBytes);

					//	long start = System.currentTimeMillis();
					ts.execute();
					//	long end = System.currentTimeMillis();
					//System.out.println("**** Function Evaluate execution time: " + (end - start));


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
					//System.out.println("**** Function Evaluate collect partial results: " + (PRend - PRstart));

					//					System.out.println("=== Reduction TOTAL bytes ===");
					//					for (int i = 0; i < total.length; i++) {
					//						System.out.print(total[i] + " ");
					//					}
					//					System.out.println();

					acres = new AccelerationData(total, 1, returnSize);
				}

//				} else {
//					long powerStart = System.currentTimeMillis();
//					ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
//					int powOf2 = (int) largestPowerOfTwo(numOfElements);
//					//System.out.println("Largest power of two: " + powOf2);
//					powerOfTwoDatasetSizes.add(powOf2);
//
//					int remainder = numOfElements - powOf2;
//					if (isPowerOfTwo(remainder)) {
//						powerOfTwoDatasetSizes.add(remainder);
//					} else {
//						int pow2Size;
//						while (!isPowerOfTwo(remainder)) {
//							pow2Size = (int) largestPowerOfTwo(remainder);
//							powerOfTwoDatasetSizes.add(pow2Size);
//							remainder = remainder - pow2Size;
//							if (isPowerOfTwo(remainder)) {
//								powerOfTwoDatasetSizes.add(remainder);
//							}
//						}
//					}
//
//					long powerEnd = System.currentTimeMillis();
//					System.out.println("**** Function Evaluate (Non-PowerOfTwo) calculate power-of-2 sizes: " + (powerEnd - powerStart));
////						System.out.println("Powers of 2:");
////						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
////							System.out.println(powerOfTwoDatasetSizes.get(i));
////						}
//					ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();
//
//					int bytesUsed = 0;
//
//					// for each data set perform the reduction
//					long totalRedTime = 0;
//					long totalPRTime = 0;
//					long totalSplit = 0;
//					for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {
//						long splitStart = System.currentTimeMillis();
//						int inputSize = powerOfTwoDatasetSizes.get(w);
//
//						byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * returnSize)); //new byte[inputSize];
//						bytesUsed += (inputSize * returnSize);
//						long splitEnd = System.currentTimeMillis();
//						totalSplit += (splitEnd - splitStart);
//
//						int size; // = inputByteData.length;
//						if (inputSize <= 256) {
//							size = 2;
//						} else {
//							size = inputSize / 256 + 1;
//						}
//						size *= 4;
//
//						byte[] outBytes = new byte[size * returnSize];
//
//						FlinkData f = new FlinkData(true, inBytes, outBytes);
//						TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
//						// @formatter:off
//						TaskSchedule ts = new TaskSchedule("s5")
//							.flinkInfo(f)
//							.prebuiltTask("t5",
//								"reduce",
//								"./pre-compiled/prebuilt-exus-reduction-ints.cl",
//								new Object[]{inputByteData, outBytes},
//								new Access[]{Access.READ, Access.WRITE},
//								defaultDevice,
//								new int[]{inputSize})
//							.streamOut(outBytes);
//
//						long start = System.currentTimeMillis();
//						ts.execute();
//						long end = System.currentTimeMillis();
//						totalRedTime += (end - start);
//						// -----------------------
//
//						//					System.out.println("=== Reduction bytes ===");
//						//					for (int i = 0; i < outBytes.length; i++) {
//						//						System.out.print(outBytes[i] + " ");
//						//					}
//						//					System.out.println();
//
//						byte[] totalPR = new byte[returnSize];
//						long PRStart = System.currentTimeMillis();
//						for (int i = 0; i < 4; i++) {
//							addPRField(outBytes, totalPR, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
//						}
//						long PREnd = System.currentTimeMillis();
//						totalPRTime += (PREnd - PRStart);
//						collectPowerOfTwoRedRes.add(totalPR);
//					}
//					System.out.println("**** Function Evaluate (Non-PowerOfTwo) split dataset: " + totalSplit);
//					System.out.println("**** Function Evaluate (Non-PowerOfTwo) execution time: " + totalRedTime);
//					System.out.println("**** Function Evaluate (Non-PowerOfTwo) collect Partial Results: " + totalPRTime);
//
//					long finalPRStart = System.currentTimeMillis();
//					int concatSize = 0;
//					for (byte[] partialRes : collectPowerOfTwoRedRes) {
//						concatSize += partialRes.length;
//					}
//
//					byte[] concatRes = null;
//					int destPos = 0;
//
//					for (byte[] partialRes : collectPowerOfTwoRedRes) {
//						if (concatRes == null) {
//							// write first array
//							concatRes = Arrays.copyOf(partialRes, concatSize);
//							destPos += partialRes.length;
//						} else {
//							System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
//							destPos += partialRes.length;
//						}
//					}
//
//					byte[] total = new byte[returnSize];
//					for (int i = 0; i < 4; i++) {
//						addPRField(concatRes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
//					}
//					long finalPREnd = System.currentTimeMillis();
//					//					System.out.println("=== Reduction TOTAL bytes ===");
//					//					for (int i = 0; i < total.length; i++) {
//					//						System.out.print(total[i] + " ");
//					//					}
//					//					System.out.println();
//					System.out.println("**** Function Evaluate (Non-PowerOfTwo) collect partial results of PowerOfTwo executions: " + (finalPREnd - finalPRStart));
//					acres = new AccelerationData(total, 1, returnSize);
//				}

			}
			tornadoEx = true;
			tornadoVMCleanUp();
		} else {
			try {
				ArrayList<IT> data = new ArrayList<>();
				DataTransformation dtras = new DataTransformation();
				byte[] bdata = acdata.getRawData();
				FlinkCompilerInfo fct = new FlinkCompilerInfo();
				byte[] typeB = new byte[0];
				Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
				typeB = conf.getTypeInfo(typeB);
				ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
				ObjectInputStream ins = new ObjectInputStream(bObj);
				HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
				ins.close();
				bObj.close();

				for (String name : typeInf.keySet()) {
					int ref = name.indexOf("@");
					String classname = name.substring(0, ref);
					if (classname.equals(reducer.getClass().toString().replace("class ", ""))) {
						examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct, acres);
						inOwner = getType(typeInf.get(name)[0].toString());
						break;
					}
				}
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
				if (acdata.getArrayField()) {
					actualSize = acdata.getRecordSize();
					if (padding) {
						sizeOftuple = acdata.getRecordSize(); //fct.getFieldSizesRet().size() * 8;
					} else {
						sizeOftuple = acdata.getTotalBytes();
					}
				} else {
					for (int fieldSize : fct.getFieldSizesRet()) {
						actualSize += fieldSize;
					}
					if (padding) {
						sizeOftuple = fct.getFieldSizesRet().size() * 8;
					} else {
						sizeOftuple = actualSize;
					}
				}
				data = dtras.materializeInputRecords(inOwner, bdata, fct, padding, sizeOftuple, actualSize);
				for (IT record : data) {
					if (base == null) {
						base = serializer.copy(record);
					} else {
						base = objectReuseEnabled ? reducer.reduce(base, record) : serializer.copy(reducer.reduce(base, record));
					}
				}
			} catch (Exception e) {
				throw new ExceptionInChainedStubException(taskName, e);
			}
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

	private String getType(String type) {
		StringTokenizer multiTokenizer = new StringTokenizer(type, " <>");
		String formatedType = null;
		if (type.contains("Tuple")) {
			int i = 0;
			while (multiTokenizer.hasMoreTokens()) {
				String tok = multiTokenizer.nextToken();
				if (i == 1) {
					formatedType = "org/apache/flink/api/java/tuple/" + tok;
					break;
				}
				i++;
			}
		} else if (type.contains("BasicArray")) {
			int i = 0;
			while (multiTokenizer.hasMoreTokens()) {
				String tok = multiTokenizer.nextToken();
				if (i == 1) {
					formatedType = "java/lang/" + tok + "[]";
					break;
				}
				i++;
			}
		} else {
			formatedType = "java/lang/" + type;
		}
		return formatedType;

	}

	void materializeTornadoBuffers (AccelerationData acres, String type, FlinkCompilerInfo fct) {
		byte[] out = acres.getRawData();
		if (acres.getArrayField()) {
			int numOfBytes = 0;
			int lengthOfArrayField = acres.getLengthOfArrayField();
			int arrayFieldNo = acres.getArrayFieldNo();
			int totalBytes = acres.getTotalBytes();
			int recordSize = acres.getRecordSize();
			ArrayList<Integer> tupleReturnSizes = acres.getReturnFieldSizes();
			boolean diffReturnSize = acres.isDifferentReturnTupleFields();
			changeEndianess(true, out, recordSize, diffReturnSize, tupleReturnSizes);
			while (numOfBytes < out.length) {
				byte[] recordBytes = getReturnRecordArrayField(out, numOfBytes, tupleReturnSizes,lengthOfArrayField,arrayFieldNo, totalBytes, diffReturnSize);
				if (outputCollector instanceof CountingCollector) {
					((CountingCollector) outputCollector).collect(recordBytes);
				}
				numOfBytes += recordSize;
			}
			return;
		}

		if (type.equals("java/lang/Integer")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Integer in = bf.getInt(i);
				outputCollector.collect((IT) in);
				//records.add((OT) in);
			}
		} else if (type.equals("java/lang/Double")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Double d = bf.getDouble(i);
				outputCollector.collect((IT) d);
				//records.add((OT) d);
			}
		} else if (type.equals("java/lang/Float")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Float f = bf.getFloat(i);
				outputCollector.collect((IT) f);
				//records.add((OT) f);
			}
		} else if (type.equals("java/lang/Long")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Long l = bf.getLong(i);
				outputCollector.collect((IT) l);
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
		} else if (type.equals("org/apache/flink/api/java/tuple/Tuple3")) {
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
			for (int i = 0; i < out.length; i += sizeOftuple) {
				byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
				if (outputCollector instanceof CountingCollector) {
					((CountingCollector) outputCollector).collect(recordBytes);
				}
			}
		} else if (type.equals("org/apache/flink/api/java/tuple/Tuple4")) {
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
			for (int i = 0; i < out.length; i += sizeOftuple) {
				byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
				if (outputCollector instanceof CountingCollector) {
					((CountingCollector) outputCollector).collect(recordBytes);
				}
			}
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

	@Override
	public void collect(IT record) {
		numRecordsIn.inc();
		try {
			boolean tornadoConfFlag = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTornadoVMFlag();
			boolean flinkReductions = Boolean.parseBoolean(System.getProperty("flinkReductions", "false"));
			if ((tornado || tornadoConfFlag) && !flinkReductions) {
				System.out.println("[ERROR] CHAINEDALLREDUCEDRIVER: TORNADOVM CODE SHOULD NOT BE EXECUTED HERE");
//				//System.out.println("%%%%%%% CHAINEDREDUCE: " + tornadoConfFlag);
//				String reduceUserClassName = this.reducer.getClass().toString().replace("class ", "").replace(".", "/");
//				Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesReduce.getTypes(reduceUserClassName);
//				inOwner = inOutTypes.f0;
//				// TODO: Change to getting byte array from configuration and loading it
//				TornadoReduce rsk = AcceleratedFlinkUserFunction.getTornadoReduce();
//				Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
//				AccelerationData acdata = (AccelerationData) conf.getAccelerationData();
//				inputByteData = acdata.getRawData();
//				numOfElements = acdata.getInputSize();
//				if (inOwner.equals("java/lang/Integer")) {
//					int[] in = new int[numOfElements];
//
//					// Based on the output data type create the output array and the Task Schedule
//					// Currently, the input and output data can only be of type Integer, Double, Float or Long.
//					// Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
//					// since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
//
//					int[] out = new int[1];
//					byte[] outBytes = new byte[4];
//					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
//
//					new TaskSchedule("s0")
//						.flinkInfo(f)
//						.task("t0", rsk::reduce, in, out)
//						.streamOut(out)
//						.execute();
//
////					System.out.println("RESULTS: ");
////					for (int i = 0; i < outBytes.length; i++) {
////						System.out.print(outBytes[i] + " ");
////					}
////					System.out.println("\n");
//					AccelerationData acres = new AccelerationData(outBytes, 1, 4);
//					conf.setAccelerationData(acres);
//				} else if (inOwner.equals("java/lang/Double")) {
//					double[] in = new double[numOfElements];
//					double[] out = new double[1];
//					byte[] outBytes = new byte[8];
//					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
//
//					new TaskSchedule("s0")
//						.flinkInfo(f)
//						.task("t0", rsk::reduce, in, out)
//						.streamOut(out)
//						.execute();
//
//					AccelerationData acres = new AccelerationData(outBytes, 1, 8);
//					conf.setAccelerationData(acres);
//				} else if (inOwner.equals("java/lang/Float")) {
//					float[] in = new float[numOfElements];
//
//					float[] out = new float[1];
//					byte[] outBytes = new byte[4];
//					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
//
//					new TaskSchedule("s0")
//						.flinkInfo(f)
//						.task("t0", rsk::reduce, in, out)
//						.streamOut(out)
//						.execute();
//
//					AccelerationData acres = new AccelerationData(outBytes, 1, 4);
//					conf.setAccelerationData(acres);
//				} else if (inOwner.equals("java/lang/Long")) {
//					long[] in = new long[numOfElements];
//					long[] out = new long[1];
//					byte[] outBytes = new byte[8];
//					FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);
//
//					new TaskSchedule("s0")
//						.flinkInfo(f)
//						.task("t0", rsk::reduce, in, out)
//						.streamOut(out)
//						.execute();
//
//					AccelerationData acres = new AccelerationData(outBytes, 1, 8);
//					conf.setAccelerationData(acres);
//				} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
//					int returnSize = 0;
//					AccelerationData acres = new AccelerationData();
//
//					FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
//					for (String name : AbstractInvokable.typeInfo.keySet()) {
//						if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
//							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
//							break;
//						}
//					}
////
//					int size; //= inputByteData.length;
//					if (numOfElements <= 256) {
//						size = 2;
//					} else {
//						size = numOfElements / 256 + 1;
//					}
//					size *= 4;
////
////
////
//					ArrayList<Integer> fieldSizes = fct0.getFieldSizes();
//					int totalTupleSize = fct0.getReturnArrayFieldTotalBytes();
//					int arrayPos = fct0.getReturnTupleArrayFieldNo();
//					for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//						if (i != arrayPos) {
//							if (fct0.getDifferentTypesRet()) {
//								totalTupleSize += 8;
//								fieldSizes.set(i, 8);
//							} else {
//								totalTupleSize += fct0.getFieldSizesRet().get(i);
//							}
//						} else {
//							fieldSizes.set(arrayPos, fct0.getReturnArrayFieldTotalBytes());
//						}
//					}
////
//					byte[] outBytes = new byte[size * totalTupleSize];
////
////					System.out.println("=== Input Bytes ===");
////					for (int i = 0; i < inputByteData.length; i++) {
////						System.out.print(inputByteData[i] + " ");
////					}
////					System.out.println();
////
//					// map correctness is not dependent on input size
//					byte[] tupleSize = new byte[4]; //numOfElements;
//					ByteBuffer.wrap(tupleSize).putInt(numOfElements);
//					changeOutputEndianess4(tupleSize);
//					System.out.println("=== TupleSize Bytes ===");
//					for (int i = 0; i < tupleSize.length; i++) {
//						System.out.print(tupleSize[i] + " ");
//					}
//					System.out.println();
//					int arraySize = fct0.getArraySize();
//					System.out.println("arraySize: " + arraySize);
//					byte[] arraySizeInBytes = new byte[4];
//					ByteBuffer.wrap(arraySizeInBytes).putInt(arraySize);
//					changeOutputEndianess4(arraySizeInBytes);
//
////					System.out.println("Reduce input: ");
////					for (int i = 0; i < inputByteData.length; i++) {
////						System.out.print(inputByteData[i] + " ");
////					}
////					System.out.println("\n");
//
//					FlinkData f = new FlinkData(true, inputByteData, tupleSize, arraySizeInBytes, outBytes);
//					TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
//					// @formatter:off
//					new TaskSchedule("s0")
//						.flinkInfo(f)
//						.prebuiltTask("t0",
//							"mapOperations",
//							"./pre-compiled/prebuilt-exus-map-reduction-float.cl",
//							new Object[] { inputByteData, tupleSize, arraySizeInBytes, outBytes },
//							new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
//							defaultDevice,
//							// numofthreads == arraysize of tuplefield
//							new int[] { arraySize })
//						.streamOut(outBytes)
//						.execute();
////
////					System.out.println("=== Map Reduction Bytes ===");
////					for (int i = 0; i < outBytes.length; i++) {
////						System.out.print(outBytes[i] + " ");
////					}
////					System.out.println();
//
//					if (isPowerOfTwo(numOfElements)) {
//						System.out.println("SIZE POWER OF TWO!");
//
//						int size2; // = inputByteData.length;
//						if (numOfElements <= 256) {
//							size2 = 2;
//						} else {
//							size2 = numOfElements / 256 + 1;
//						}
//						size2 *= 4;
//
//						byte[] outBytes2 = new byte[size2 * totalTupleSize];
//
//
//						FlinkData f2 = new FlinkData(true, inputByteData, outBytes, arraySizeInBytes, outBytes2);
//
//						// @formatter:off
//						new TaskSchedule("s1")
//							.flinkInfo(f2)
//							.prebuiltTask("t1",
//								"reduce",
//								"./pre-compiled/prebuilt-exus-reduction-UpdateAccum-float.cl",
//								new Object[] { inputByteData, outBytes, arraySizeInBytes, outBytes2 },
//								new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
//								defaultDevice,
//								new int[] { numOfElements })
//							.streamOut(outBytes2)
//							.execute();
//
////						System.out.println("=== Reduction bytes ===");
////						for (int i = 0; i < outBytes2.length; i++) {
////							System.out.print(outBytes2[i] + " ");
////						}
////						System.out.println();
//
//						byte[] total = new byte[totalTupleSize];
//
//						addPRField(outBytes2, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
//						Array.copy(outBytes2, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
//						//changeOutputEndianess8Partial(total, 72);
////						System.out.println("=== Reduction TOTAL bytes ===");
////						for (int i = 0; i < total.length; i++) {
////							System.out.print(total[i] + " ");
////						}
////						System.out.println();
//
//						acres.setRawData(total);
//						acres.setInputSize(1);
//						acres.setReturnSize(1);
//						if (fct0.getReturnArrayField()) {
//							acres.hasArrayField();
//							acres.setLengthOfArrayField(arraySize);
//							acres.setArrayFieldNo(0);
//							int totalB = fct0.getReturnArrayFieldTotalBytes();
//							for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//								if (i != arrayPos) {
//									totalB += fct0.getFieldSizesRet().get(i);
//								}
//							}
//							acres.setTotalBytes(totalB);
//							acres.setRecordSize(totalTupleSize);
//						}
//						conf.setAccelerationData(acres);
//					} else {
////						System.out.println("=== Total Input Bytes ===");
////						for (int i = 0; i < inputByteData.length; i++) {
////							System.out.print(inputByteData[i] + " ");
////						}
////						System.out.println();
//						ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
//						int powOf2 = (int) largestPowerOfTwo(numOfElements);
//						//System.out.println("Largest power of two: " + powOf2);
//						powerOfTwoDatasetSizes.add(powOf2);
//
//						int remainder = numOfElements - powOf2;
//						if (isPowerOfTwo(remainder)) {
//								powerOfTwoDatasetSizes.add(remainder);
//						} else {
//							int pow2Size;
//							while (!isPowerOfTwo(remainder)) {
//								pow2Size = (int) largestPowerOfTwo(remainder);
//								powerOfTwoDatasetSizes.add(pow2Size);
//								remainder = remainder - pow2Size;
//								if (isPowerOfTwo(remainder)) {
//									powerOfTwoDatasetSizes.add(remainder);
//								}
//							}
//						}
////						System.out.println("Powers of 2:");
////						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
////							System.out.println(powerOfTwoDatasetSizes.get(i));
////						}
//						ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();
//
//						int bytesUsed = 0;
//						// for each data set perform the reduction
//						for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {
//
//							int inputSize = powerOfTwoDatasetSizes.get(w);
//
//							byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * totalTupleSize)); //new byte[inputSize];
//							bytesUsed += (inputSize * totalTupleSize);
//
//							int size2;
//							if (inputSize <= 256) {
//								size2 = 2;
//							} else {
//								size2 = inputSize / 256 + 1;
//							}
//							size2 *= 4;
//
//							byte[] outBytes2 = new byte[size2 * totalTupleSize];
//
//
//							FlinkData f2 = new FlinkData(true, inBytes, outBytes, arraySizeInBytes, outBytes2);
//
//							// @formatter:off
//							new TaskSchedule("s1")
//								.flinkInfo(f2)
//								.prebuiltTask("t1",
//									"reduce",
//									"./pre-compiled/prebuilt-exus-reduction-UpdateAccum.cl",
//									new Object[] { inBytes, outBytes, arraySizeInBytes, outBytes2 },
//									new Access[] { Access.READ, Access.READ, Access.READ, Access.WRITE },
//									defaultDevice,
//									new int[] { inputSize })
//								.streamOut(outBytes2)
//								.execute();
//
////							System.out.println("=== Reduction bytes ===");
////							for (int i = 0; i < outBytes2.length; i++) {
////								System.out.print(outBytes2[i] + " ");
////							}
////							System.out.println();
//
//
//							byte[] totalPartial = new byte[totalTupleSize];
//
//							addPRField(outBytes2, totalPartial, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
//							Array.copy(outBytes2, 0, totalPartial, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
//							//changeOutputEndianess8Partial(total, 72);
////							System.out.println("=== Reduction PARTIAL TOTAL bytes ===");
////							for (int i = 0; i < totalPartial.length; i++) {
////								System.out.print(totalPartial[i] + " ");
////							}
////							System.out.println();
//
//							collectPowerOfTwoRedRes.add(totalPartial);
//						}
//
//						int concatSize = 0;
//						for (byte[] partialRes : collectPowerOfTwoRedRes) {
//							concatSize += partialRes.length;
//						}
//
//						byte[] concatRes = null;
//						int destPos = 0;
//
//						for (byte[] partialRes : collectPowerOfTwoRedRes) {
//							if (concatRes == null) {
//								// write first array
//								concatRes = Arrays.copyOf(partialRes, concatSize);
//								destPos += partialRes.length;
//							} else {
//								System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
//								destPos += partialRes.length;
//							}
//						}
//
//						byte[] total = new byte[totalTupleSize];
//
////						System.out.println("Concatenated results: ");
////						for (int i = 0; i < concatRes.length; i++) {
////							System.out.print(concatRes[i] + " ");
////						}
////						System.out.println("\n");
//
//						addPRField(concatRes, total, 2, fieldSizes, fct0.getFieldTypesRet(), totalTupleSize, true);
//						Array.copy(concatRes, 0, total, 0, (fct0.getReturnArrayFieldTotalBytes() + 8));
//
//						// collect acdata for next operation
////						System.out.println("=== Reduction TOTAL bytes ===");
////						for (int i = 0; i < total.length; i++) {
////							System.out.print(total[i] + " ");
////						}
////						System.out.println();
//
//						acres.setRawData(total);
//						acres.setInputSize(1);
//						acres.setReturnSize(1);
//						if (fct0.getReturnArrayField()) {
//							acres.hasArrayField();
//							acres.setLengthOfArrayField(fct0.getArraySize());
//							acres.setArrayFieldNo(0);
//							int totalB = fct0.getReturnArrayFieldTotalBytes();
//							for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//								if (i != arrayPos) {
//									totalB += fct0.getFieldSizesRet().get(i);
//								}
//							}
//							acres.setTotalBytes(totalB);
//							acres.setRecordSize(totalTupleSize);
//						}
//						conf.setAccelerationData(acres);
//
//					}
//				} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
//
//					int returnSize = 0;
//					FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
//					AccelerationData acres = new AccelerationData();
//
//					for (String name : AbstractInvokable.typeInfo.keySet()) {
//						if (name.contains(reducer.getClass().toString().replace("class ", ""))) {
//							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
//							break;
//						}
//					}
//
//					if (isPowerOfTwo(numOfElements)) {
//						int size; // = inputByteData.length;
//						if (numOfElements <= 256) {
//							size = 2;
//						} else {
//							size = numOfElements / 256 + 1;
//						}
//						size *= 4;
//
//						byte[] outBytes = new byte[size * returnSize];
//
//						FlinkData f = new FlinkData(true, inputByteData, outBytes);
//						TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
//						// @formatter:off
//						new TaskSchedule("s0")
//							.flinkInfo(f)
//							.prebuiltTask("t0",
//								"reduce",
//								"./pre-compiled/prebuilt-exus-reduction-ints.cl",
//								new Object[] { inputByteData, outBytes },
//								new Access[] { Access.READ, Access.WRITE },
//								defaultDevice,
//								new int[] { numOfElements })
//							.streamOut(outBytes)
//							.execute();
//
//						// -----------------------
//
//	//					System.out.println("=== Reduction bytes ===");
//	//					for (int i = 0; i < outBytes.length; i++) {
//	//						System.out.print(outBytes[i] + " ");
//	//					}
//	//					System.out.println();
//
//						byte[] total = new byte[returnSize];
//
//						for (int i = 0; i < 4; i++) {
//							addPRField(outBytes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
//						}
//
//	//					System.out.println("=== Reduction TOTAL bytes ===");
//	//					for (int i = 0; i < total.length; i++) {
//	//						System.out.print(total[i] + " ");
//	//					}
//	//					System.out.println();
//
//						AccelerationData res = new AccelerationData(total, 1, returnSize);
//						conf.setAccelerationData(res);
//
//					} else {
//						ArrayList<Integer> powerOfTwoDatasetSizes = new ArrayList<>();
//						int powOf2 = (int) largestPowerOfTwo(numOfElements);
//						//System.out.println("Largest power of two: " + powOf2);
//						powerOfTwoDatasetSizes.add(powOf2);
//
//						int remainder = numOfElements - powOf2;
//						if (isPowerOfTwo(remainder)) {
//							powerOfTwoDatasetSizes.add(remainder);
//						} else {
//							int pow2Size;
//							while (!isPowerOfTwo(remainder)) {
//								pow2Size = (int) largestPowerOfTwo(remainder);
//								powerOfTwoDatasetSizes.add(pow2Size);
//								remainder = remainder - pow2Size;
//								if (isPowerOfTwo(remainder)) {
//									powerOfTwoDatasetSizes.add(remainder);
//								}
//							}
//						}
////						System.out.println("Powers of 2:");
////						for (int i = 0; i < powerOfTwoDatasetSizes.size(); i++) {
////							System.out.println(powerOfTwoDatasetSizes.get(i));
////						}
//						ArrayList<byte[]> collectPowerOfTwoRedRes = new ArrayList<>();
//
//						int bytesUsed = 0;
//
//						// for each data set perform the reduction
//						for (int w = 0; w < powerOfTwoDatasetSizes.size(); w++) {
//
//							int inputSize = powerOfTwoDatasetSizes.get(w);
//
//							byte[] inBytes = Arrays.copyOfRange(inputByteData, bytesUsed, bytesUsed + (inputSize * returnSize)); //new byte[inputSize];
//							bytesUsed += (inputSize * returnSize);
//
//							int size; // = inputByteData.length;
//							if (inputSize <= 256) {
//								size = 2;
//							} else {
//								size = inputSize / 256 + 1;
//							}
//							size *= 4;
//
//							byte[] outBytes = new byte[size * returnSize];
//
//							FlinkData f = new FlinkData(true, inBytes, outBytes);
//							TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
//							// @formatter:off
//							new TaskSchedule("s0")
//								.flinkInfo(f)
//								.prebuiltTask("t0",
//									"reduce",
//									"./pre-compiled/prebuilt-exus-reduction-ints.cl",
//									new Object[]{inputByteData, outBytes},
//									new Access[]{Access.READ, Access.WRITE},
//									defaultDevice,
//									new int[]{inputSize})
//								.streamOut(outBytes)
//								.execute();
//
//							// -----------------------
//
//		//					System.out.println("=== Reduction bytes ===");
//		//					for (int i = 0; i < outBytes.length; i++) {
//		//						System.out.print(outBytes[i] + " ");
//		//					}
//		//					System.out.println();
//
//							byte[] totalPR = new byte[returnSize];
//
//							for (int i = 0; i < 4; i++) {
//								addPRField(outBytes, totalPR, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
//							}
//
//							collectPowerOfTwoRedRes.add(totalPR);
//						}
//
//						int concatSize = 0;
//						for (byte[] partialRes : collectPowerOfTwoRedRes) {
//							concatSize += partialRes.length;
//						}
//
//						byte[] concatRes = null;
//						int destPos = 0;
//
//						for (byte[] partialRes : collectPowerOfTwoRedRes) {
//							if (concatRes == null) {
//								// write first array
//								concatRes = Arrays.copyOf(partialRes, concatSize);
//								destPos += partialRes.length;
//							} else {
//								System.arraycopy(partialRes, 0, concatRes, destPos, partialRes.length);
//								destPos += partialRes.length;
//							}
//						}
//
//						byte[] total = new byte[returnSize];
//						for (int i = 0; i < 4; i++) {
//							addPRField(concatRes, total, i, fct0.getFieldSizesRet(), fct0.getFieldTypesRet(), returnSize, true);
//						}
//
//	//					System.out.println("=== Reduction TOTAL bytes ===");
//	//					for (int i = 0; i < total.length; i++) {
//	//						System.out.print(total[i] + " ");
//	//					}
//	//					System.out.println();
//
//						AccelerationData res = new AccelerationData(total, 1, returnSize);
//						conf.setAccelerationData(res);
//					}
//
//				}
//				this.outputCollector.collect(null);
			} else {
				if (base == null) {
					base = serializer.copy(record);
					if (reducer.toString().contains("TestPiEstimationFlinkGPU")) {
						base = reducer.reduce(base, null);
					}
				} else {
					base = objectReuseEnabled ? reducer.reduce(base, record) : serializer.copy(reducer.reduce(base, record));
				}
			}
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(taskName, e);
		}
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
		boolean diffFieldSizes = false;
		for (int i = 0; i < fieldSizes.size(); i++) {
			for (int j = 0; j < fieldSizes.size(); j++) {
				if (fieldSizes.get(i) != fieldSizes.get(j)) {
					//System.out.println("Diff sizes!");
					diffFieldSizes = true;
					break;
				}
			}
		}
		if (fieldNo != 0) {
			for (int i = 0; i < fieldNo; i++) {
				if (diffFieldSizes) {
					offset += 8;
				} else {
					offset += fieldSizes.get(i);
				}
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

	public static void addPRArray(byte[] partialResultsMap, byte[] totalMap, int fieldNo, ArrayList<Integer> fieldSizes, ArrayList<String> fieldTypes, int totalSize, int arraySize, boolean changeEndianess) {
		int offset = 0;
		if (fieldNo != 0) {
			for (int i = 0; i < fieldNo; i++) {
				offset += fieldSizes.get(i);
			}
		}

		//int totalNumInt = 0, totalNumDouble = 0, totalNumFloat = 0, totalNumLong = 0;
		int[] totalNumInt = new int[arraySize];
		double[] totalNumDouble = new double[arraySize];
		float[] totalNumFloat = new float[arraySize];
		long[] totalNumLong = new long[arraySize];

		int fieldSize = fieldSizes.get(fieldNo);

		for (int i = 0; i < arraySize; i++) {
			int k = 0;
			for (int j = 0; j < partialResultsMap.length; j+=totalSize) {
				if (fieldTypes.get(fieldNo).equals("int")) {
					byte[] b = new byte[4];
					ByteBuffer.wrap(partialResultsMap, (offset + totalSize*k + i), fieldSize).get(b);
					changeOutputEndianess4(b);
					totalNumInt[i] += ByteBuffer.wrap(b).getInt();
				} else if (fieldTypes.get(fieldNo).equals("double")) {
					byte[] b = new byte[8];
					ByteBuffer.wrap(partialResultsMap, (offset + totalSize*k + i*8), 8).get(b);
					changeOutputEndianess8(b);
					totalNumDouble[i] += ByteBuffer.wrap(b).getDouble();
				} else if (fieldTypes.get(fieldNo).equals("float")) {
					byte[] b = new byte[4];
					ByteBuffer.wrap(partialResultsMap, (offset + totalSize*k + i), fieldSize).get(b);
					changeOutputEndianess4(b);
					totalNumFloat[i] += ByteBuffer.wrap(b).getFloat();
				} else if (fieldTypes.get(fieldNo).equals("long")) {
					byte[] b = new byte[8];
					ByteBuffer.wrap(partialResultsMap, (offset + totalSize*k + i), fieldSize).get(b);
					changeOutputEndianess8(b);
					totalNumLong[i] += ByteBuffer.wrap(b).getLong();
				}
				k++;
			}
		}

		for (int i = 0; i < arraySize; i++) {
			//for (int j = offset; j < partialResultsMap.length; j+=totalSize) {
				if (fieldTypes.get(fieldNo).equals("int")) {
					byte[] bytesOfField = new byte[4];
					ByteBuffer.wrap(bytesOfField).putInt(totalNumInt[i]);
					if (changeEndianess) {
						changeOutputEndianess4(bytesOfField);
					}
					for (int k = 0; k < 4; k++) {
						totalMap[k + offset + i*8] = bytesOfField[k];
					}
				} else if (fieldTypes.get(fieldNo).equals("double")) {
					byte[] bytesOfField = new byte[8];
					ByteBuffer.wrap(bytesOfField).putDouble(totalNumDouble[i]);
					if (changeEndianess) {
						changeOutputEndianess8(bytesOfField);
					}
					for (int k = 0; k < 8; k++) {
						totalMap[k + offset + i*8] = bytesOfField[k];
					}
				} else if (fieldTypes.get(fieldNo).equals("float")) {
					byte[] bytesOfField = new byte[4];
					ByteBuffer.wrap(bytesOfField).putFloat(totalNumFloat[i]);
					if (changeEndianess) {
						changeOutputEndianess4(bytesOfField);
					}
					for (int k = 0; k < 4; k++) {
						totalMap[k + offset + i*8] = bytesOfField[i];
					}
				} else if (fieldTypes.get(fieldNo).equals("long")) {
					byte[] bytesOfField = new byte[8];
					ByteBuffer.wrap(bytesOfField).putLong(totalNumLong[i]);
					if (changeEndianess) {
						changeOutputEndianess8(bytesOfField);
					}
					for (int k = 0; k < 8; k++) {
						totalMap[k + offset + i*8] = bytesOfField[i];
					}
				}
			//}
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

	@Override
	public void close() {
		if (tornadoEx) {
			if (this.getOutputCollector() instanceof ChainedAllReduceDriver) {
				ChainedAllReduceDriver chred = (ChainedAllReduceDriver) getOutputCollector();
				((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
				chred.collect(acres);
			} else if (this.getOutputCollector() instanceof ChainedMapDriver) {
				ChainedMapDriver chmad = (ChainedMapDriver) getOutputCollector();
				((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
				chmad.collect(acres);
			} else if (this.getOutputCollector() instanceof CountingCollector) {
				CountingCollector ccol = (CountingCollector) this.getOutputCollector();
				ccol.numRecordsOut.inc(acres.getInputSize());
				if (ccol.getCollector() instanceof ChainedAllReduceDriver) {
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) ccol.getCollector();
					chred.collect(acres);
				} else if (ccol.getCollector() instanceof ChainedMapDriver) {
					ChainedMapDriver chmad = (ChainedMapDriver) ccol.getCollector();
					chmad.collect(acres);
				} else {
					materializeTornadoBuffers(acres, inOwner, fct0);
				}
		    } else {
				materializeTornadoBuffers(acres, inOwner, fct0);
			}
		} else {
			try {
				if (base != null) {
					this.outputCollector.collect(base);
					base = null;
				}
			} catch (Exception e) {
				throw new ExceptionInChainedStubException(this.taskName, e);
			}
		}
		this.outputCollector.close();
	}

	private void tornadoVMCleanUp() {
		for (int k = 0; k < TornadoRuntime.getTornadoRuntime().getNumDrivers(); k++) {
			final TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(k);
			for (int j = 0; j < driver.getDeviceCount(); j++) {
				driver.getDevice(j).reset();
			}
		}
	}

}
