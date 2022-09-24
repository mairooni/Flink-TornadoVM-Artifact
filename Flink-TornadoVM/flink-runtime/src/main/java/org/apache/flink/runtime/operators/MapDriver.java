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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.flinktornadolog.FlinkTornadoVMLogger;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.chaining.ChainedAllReduceDriver;
import org.apache.flink.runtime.operators.chaining.ChainedMapDriver;
import org.apache.flink.runtime.operators.chaining.ChainedReduceCombineDriver;
import org.apache.flink.runtime.operators.resettable.SpillingResettableMutableObjectIterator;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.apache.flink.runtime.tornadovm.DataTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.tornado.api.*;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.runtime.common.Tornado;
import uk.ac.manchester.tornado.runtime.common.TornadoLogger;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.*;

/**
 * Map task which is executed by a Task Manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapFunction.
 * 
 * @see org.apache.flink.api.common.functions.MapFunction
 * 
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapDriver<IT, OT> implements Driver<MapFunction<IT, OT>, OT> {
	
	private TaskContext<MapFunction<IT, OT>, OT> taskContext;
	
	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	private boolean tornado = Boolean.parseBoolean(System.getProperty("tornado", "false"));

	protected final Logger LOG = LoggerFactory.getLogger(getClass());
	private final FlinkTornadoVMLogger FlinkTornadoVMLog = new FlinkTornadoVMLogger();

	public static final boolean BREAKDOWN = Boolean.parseBoolean(System.getProperties().getProperty("flinktornado.breakdown", "false"));

	@Override
	public void setup(TaskContext<MapFunction<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;

		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<MapFunction<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<MapFunction<IT, OT>> clazz = (Class<MapFunction<IT, OT>>) (Class<?>) MapFunction.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public void prepare() {
		// nothing, since a mapper does not need any preparation
	}

	@Override
	public void run() throws Exception {
		final Counter numRecordsIn = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
		final Counter numRecordsOut = this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
		// cache references on the stack
		final MutableObjectIterator<IT> input = this.taskContext.getInput(0);
		final MapFunction<IT, OT> function = this.taskContext.getStub();
		final Collector<OT> output = new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
		int arrayLength = 0;
		boolean tornadoConfFlag = this.taskContext.getTaskConfig().getConfiguration().getTornadoVMFlag();
		if (tornado || tornadoConfFlag) {
			//System.out.println("%%%%%%% MAP: " + tornadoConfFlag);
			// examine udf
//			TaskConfig taskConf = this.taskContext.getTaskConfig();
//			Configuration conf = taskConf.getConfiguration();
			String mapUserClassName = function.getClass().toString().replace("class ", "").replace(".", "/");
			//Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesMap.getTypes(mapUserClassName);
			//String inOwner = inOutTypes.f0;
			//String outOwner = inOutTypes.f1;
			byte[] typeBy = new byte[0];
			typeBy = this.taskContext.getTaskConfig().getConfiguration().getTypeInfo(typeBy);
			ByteArrayInputStream bO = new ByteArrayInputStream(typeBy);  
			ObjectInputStream instream = new ObjectInputStream(bO);
			HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) instream.readObject();
			instream.close();
			bO.close();
			String inOwner = null;
			String outOwner = null;
			for (String name : typeInf.keySet()) {
				int ref = name.indexOf("@");
				String clname = name.substring(0, ref);
				if (clname.equals(function.getClass().toString().replace("class ", ""))) {
					inOwner = getType(typeInf.get(name)[0].toString());
					outOwner = getType(typeInf.get(name)[1].toString());
					break;
				}
																		     }
			int numOfElements = 0;
			byte[] inputByteData = null;
			byte[] broadcastedBytes = null;
			int broadcastedSize = 0;
			boolean broadcastedDataset = false;
			AccelerationData acdata = null;
			AccelerationData acdata2 = null;
			AccelerationData acres = null;
			int broadCollectionSize = 0;

			TypeInformation broadcastedTypeInfo = null;

			ArrayList dataset = new ArrayList();

			if (input instanceof ReaderIterator) {
				DataTransformation dtrans = new DataTransformation();

				if (function instanceof RichFunction) {
					if (BREAKDOWN) {
						long broadStart = System.currentTimeMillis();
						RichFunction rich = (RichFunction) function;
						List broad = rich.getRuntimeContext().getBroadcastVariable(taskContext.getTaskConfig().getBroadcastInputName(0));
						BatchTask bt = (BatchTask) taskContext;
						TypeSerializer ser = bt.broadcastInputSerializers[0].getSerializer();
						broadCollectionSize = broad.size();
						acdata2 = dtrans.generateBroadcastedInputAccelerationData((ArrayList) broad, ser);
						broadcastedDataset = true;
						broadcastedTypeInfo = TypeExtractor.getForObject(broad.get(0));
						long broadEnd = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " get broadcasted dataset and make it Tornado-friendly: " + (broadEnd - broadStart));
					} else {
						RichFunction rich = (RichFunction) function;
						List broad = rich.getRuntimeContext().getBroadcastVariable(taskContext.getTaskConfig().getBroadcastInputName(0));
						BatchTask bt = (BatchTask) taskContext;
						TypeSerializer ser = bt.broadcastInputSerializers[0].getSerializer();
						broadCollectionSize = broad.size();
						acdata2 = dtrans.generateBroadcastedInputAccelerationData((ArrayList) broad, ser);
						broadcastedDataset = true;
						broadcastedTypeInfo = TypeExtractor.getForObject(broad.get(0));
					}
				}

				if (BREAKDOWN) {
					long dataStart = System.currentTimeMillis();

					byte[] inBytes = ((ReaderIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ READITER BUFFER NULL");
						}
						return;
					}
					acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, false, true, false);

					long dataEnd = System.currentTimeMillis();
					FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " get input dataset and make it Tornado-friendly: " + (dataEnd - dataStart));
				} else {
					byte[] inBytes = ((ReaderIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ READITER BUFFER NULL");
						}
						return;
					}
					acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, false, true, false);
				}

			} else if (input instanceof SpillingResettableMutableObjectIterator) {
				DataTransformation dtrans = new DataTransformation();
				if (function instanceof RichFunction) {
					if (BREAKDOWN) {
						long broadStart = System.currentTimeMillis();
						RichFunction rich = (RichFunction) function;
						List broad = rich.getRuntimeContext().getBroadcastVariable(taskContext.getTaskConfig().getBroadcastInputName(0));
						BatchTask bt = (BatchTask) taskContext;
						TypeSerializer ser = bt.broadcastInputSerializers[0].getSerializer();
						broadCollectionSize = broad.size();
						acdata2 = dtrans.generateBroadcastedInputAccelerationData((ArrayList) broad, ser);
						broadcastedDataset = true;
						broadcastedTypeInfo = TypeExtractor.getForObject(broad.get(0));
						long broadEnd = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " get broadcasted dataset and make it Tornado-friendly: " + (broadEnd - broadStart));
					} else {
						RichFunction rich = (RichFunction) function;
						List broad = rich.getRuntimeContext().getBroadcastVariable(taskContext.getTaskConfig().getBroadcastInputName(0));
						BatchTask bt = (BatchTask) taskContext;
						TypeSerializer ser = bt.broadcastInputSerializers[0].getSerializer();
						broadCollectionSize = broad.size();
						acdata2 = dtrans.generateBroadcastedInputAccelerationData((ArrayList) broad, ser);
						broadcastedDataset = true;
						broadcastedTypeInfo = TypeExtractor.getForObject(broad.get(0));
					}
				}

				if (BREAKDOWN) {
					long dataStart = System.currentTimeMillis();
					byte[] inBytes = ((SpillingResettableMutableObjectIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ SPILLING BUFFER NULL");
						}
						return;
					}
					if (((SpillingResettableMutableObjectIterator) input).inView == null) {
						acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, false, true, true);
						((SpillingResettableMutableObjectIterator) input).buffer.write(acdata.getRawData());
						((SpillingResettableMutableObjectIterator) input).tornadoDataSize = acdata.getRawData().length;
					} else {
						acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, true, true, true);
					}
					long dataEnd = System.currentTimeMillis();
					FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " get input dataset and make it Tornado-friendly: " + (dataEnd - dataStart));
				} else {
					byte[] inBytes = ((SpillingResettableMutableObjectIterator)input).getDataInBytes();
					if (inBytes == null || inBytes.length == 0) {
						if (inBytes == null) {
							//System.out.println("^^^ SPILLING BUFFER NULL");
						}
						return;
					}
					if (((SpillingResettableMutableObjectIterator) input).inView == null) {
						acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, false, true, true);
						((SpillingResettableMutableObjectIterator) input).buffer.write(acdata.getRawData());
						((SpillingResettableMutableObjectIterator) input).tornadoDataSize = acdata.getRawData().length;
					} else {
						acdata = dtrans.generateInputAccelerationData(inBytes, this.taskContext.<IT>getInputSerializer(0).getSerializer(), null, false, true, true, true);
					}
				}
			}

			inputByteData = acdata.getRawData();
			numOfElements = acdata.getInputSize();

			numRecordsIn.inc(numOfElements);

			if (acdata2!= null) {
				broadcastedBytes = acdata2.getRawData();
				broadcastedSize = acdata2.getInputSize();
				if (acdata.getArrayField()) {
					arrayLength = acdata.getLengthOfArrayField();
				}
				if (acdata2.getArrayField() && acdata.getArrayField()) {
					if (acdata.getLengthOfArrayField() != acdata2.getLengthOfArrayField()) {
						System.out.println("WARNING: ARRAYS OF BROADCASTED AND REGULAR INPUT SHOULD HAVE THE SAME LENGTH");
					}
				} else if (acdata2.getArrayField()) {
					arrayLength = acdata2.getLengthOfArrayField();
				}
			} else {
				arrayLength = acdata.getLengthOfArrayField();
			}
			TornadoMap msk = null;
			if (BREAKDOWN) {
				long skeletonStart = System.currentTimeMillis();
				// get asm skeleton in byte form
				byte[] def = new byte[0];
				byte[] bytesMsk = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleMap md = loader.loadClass("org.apache.flink.api.asm.MapASMSkeleton", bytesMsk);
				msk = new TornadoMap(md);
				long skeletonEnd = System.currentTimeMillis();
				FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " retrieve ASM Skeleton: " + (skeletonEnd - skeletonStart));
			} else {
				// get asm skeleton in byte form
				byte[] def = new byte[0];
				byte[] bytesMsk = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleMap md = loader.loadClass("org.apache.flink.api.asm.MapASMSkeleton", bytesMsk);
				msk = new TornadoMap(md);
			}
			FlinkCompilerInfo fct0 = null;
			if (inOwner.equals("java/lang/Integer")) {
				// Based on the output data type create the output array and the Task Schedule
				// Currently, the input and output data can only be of type Integer, Double, Float or Long.
				// Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
				// since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
				if (outOwner.equals("java/lang/Integer")) {
					int[] in = new int[numOfElements];
					int[] out = new int[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];

					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					// the results in bytes
					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Double")) {
					int[] in = new int[numOfElements];
					double[] out = new double[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];

					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				} else if (outOwner.equals("java/lang/Float")) {
					int[] in = new int[numOfElements];
					float[] out = new float[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];

					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Long")) {
					int[] in = new int[numOfElements];
					long[] out = new long[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();
					int returnSize = 0;

					if (broadcastedDataset) {
						setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
						fct0.setBroadcastedDataset(true);
						fct0.setBroadCollectionSize(broadCollectionSize);
						fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
						if (broadcastedTypeInfo.toString().contains("Tuple2")) {
							for (String name : AbstractInvokable.typeInfo.keySet()) {
								int ref = name.indexOf("@");
								String classname = name.substring(0, ref);
								if (classname.equals(function.getClass().toString().replace("class ", ""))) {
									returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
									break;
								}
							}
							int[] in = new int[1];
							Tuple2[] in2 = new Tuple2[1];
							Tuple2[] out = new Tuple2[1];
							byte[] outBytes = new byte[numOfElements * returnSize];
							fct0.setBroadcastedSize(broadcastedSize);
							FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
							//long endCompInfo = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));
							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);

							TaskSchedule ts = new TaskSchedule("s0")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t0", msk::map, in, in2, out)
								.streamOut(outBytes);

							worker.setGlobalWork(numOfElements, 1, 1);
							worker.setLocalWork(64, 1, 1);

							//long start = System.currentTimeMillis();
							ts.execute(gridScheduler);
							//long end = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " execution time: " + (end - start));
							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
						}
					}
				}
			} else if (inOwner.equals("java/lang/Double")) {
				double[] in = new double[numOfElements];

				if (outOwner.equals("java/lang/Integer")) {
					int[] out = new int[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Double")) {
					double[] out = new double[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				} else if (outOwner.equals("java/lang/Float")) {
					float[] out = new float[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Long")) {
					long[] out = new long[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				}
			} else if (inOwner.equals("java/lang/Float")) {
				float[] in = new float[numOfElements];

				if (outOwner.equals("java/lang/Integer")) {
					int[] out = new int[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Double")) {
					double[] out = new double[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				} else if (outOwner.equals("java/lang/Float")) {
					float[] out = new float[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Long")) {
					long[] out = new long[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				}

			} else if (inOwner.equals("java/lang/Long")) {
				long[] in = new long[numOfElements];

				if (outOwner.equals("java/lang/Integer")) {
					int[] out = new int[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Double")) {
					double[] out = new double[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				} else if (outOwner.equals("java/lang/Float")) {
					float[] out = new float[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres = new AccelerationData(outT, numOfElementsRes, 4);
				} else if (outOwner.equals("java/lang/Long")) {
					long[] out = new long[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres = new AccelerationData(outT, numOfElementsRes, 8);
				}
			} else if (inOwner.equals("java/lang/Float[]")) {
				int rowSize = acdata.getRowSize();
				int columnSize = acdata.getColumnSize();
				Float[][] in = new Float[1][1];
				if (outOwner.equals("java/lang/Float[]")) {
					Float[][] out = new Float[1][1];
					fct0 = new FlinkCompilerInfo();
					fct0.setIsMatrix();
					if (broadcastedDataset) {
						int rowSize2 = acdata2.getRowSize();
						int columnSize2 = acdata2.getColumnSize();
						setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
						fct0.setBroadCollectionSize(broadCollectionSize);
						fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
						fct0.setBroadcastedDataset(true);
						fct0.setMatrixType(float.class);
						fct0.setMatrixTypeSize(4);
						fct0.setColumnSizeMatrix2(columnSize2);
						fct0.setRowSizeMatrix1(rowSize);
						fct0.setRowSizeMatrix2(rowSize2);
						fct0.setColumnSizeMatrix1(columnSize);
						byte[] outBytes = new byte[rowSize*columnSize2* 4];
						FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
						Float[][] br = new Float[1][1];

						int taskNumber = ((BatchTask)taskContext).getEnvironment().getTaskInfo().getIndexOfThisSubtask();
						TornadoDevice device;
						//if (TornadoRuntime.getTornadoRuntime().getDriver(0).getDeviceCount() > taskNumber) {
						//	device =TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(taskNumber);
						//} else {
						//device =TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(0);
						//}

						WorkerGrid worker = new WorkerGrid1D(rowSize);
						GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);

						TaskSchedule ts = new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk::map, in, br, out)
							.streamOut(outBytes);

						//ts.mapAllTo(device);

						worker.setGlobalWork(rowSize, 1, 1);
						worker.setLocalWork(1, 1, 1);
						//long execStart = System.currentTimeMillis();
						ts.execute(gridScheduler);
						//long execEnd = System.currentTimeMillis();
						//System.out.println("**** Function " + function + " Tornado Execution time: " + (execEnd - execStart));
						//long endianessStart = System.currentTimeMillis();
						byte[] outT = changeOutputEndianess4(outBytes);
					//	long endianessEnd = System.currentTimeMillis();

						//System.out.println("**** Function " + function + " change output endianess: " + (endianessEnd - endianessStart));

						int numOfElementsRes = outT.length / 4;
						acres = new AccelerationData(outT, numOfElementsRes, 4);

					}
				}
			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {

				if (outOwner.equals("java/lang/Integer")) {
					Tuple2[] in = new Tuple2[numOfElements];
					int[] out = new int[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(int.class);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(4);
				} else if (outOwner.equals("java/lang/Double")) {
					Tuple2[] in = new Tuple2[numOfElements];
					double[] out = new double[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					fct0 = new FlinkCompilerInfo();

					fct0.setStoreJavaKind(double.class);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					FlinkData f = new FlinkData(inputByteData, outBytes);
					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();
					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(8);

				} else if (outOwner.equals("java/lang/Float")) {
					Tuple2[] in = new Tuple2[numOfElements];
					float[] out = new float[numOfElements];
					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(float.class);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(4);
				} else if (outOwner.equals("java/lang/Long")) {
					Tuple2[] in = new Tuple2[numOfElements];
					long[] out = new long[numOfElements];
					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(long.class);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(8);
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
					int returnSize = 0;
					long startCompInfo = 0, endCompInfo;
					if (BREAKDOWN) {
						startCompInfo = System.currentTimeMillis();
					}
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();
					for (String name : typeInf.keySet()) {
						int ref = name.indexOf("@");
						String classname = name.substring(0, ref);
						if (classname.equals(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
							break;
						}
					}
				//	returnSize = examineTypeInfoForFlinkUDFs(inOwner, outOwner, fct0, acres);
					if (broadcastedDataset) {
						setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
						fct0.setBroadcastedDataset(true);
						fct0.setBroadCollectionSize(broadCollectionSize);
						fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
						if (broadcastedTypeInfo.toString().contains("Tuple3")) {
							// kmeans
							Tuple2[] in = new Tuple2[1];
							Tuple3[] in2 = new Tuple3[1];
							Tuple2[] out = new Tuple2[1];
							byte[] outBytes = new byte[numOfElements * returnSize];
							fct0.setBroadcastedSize(broadcastedSize);
							FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
						//	long endCompInfo = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));

							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);
							TaskSchedule ts = new TaskSchedule("s0")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t0", msk::map, in, in2, out)
								.streamOut(outBytes);

							worker.setGlobalWork(numOfElements, 1, 1);
						//	long start = System.currentTimeMillis();
							ts.execute(gridScheduler);
						//	long end = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " execution time: " + (end - start));
							//byte[] outT = f.getByteResults();
//							System.out.println("---- SELECTNEAREST OUTPUT BYTES ----");
//							for (int i = 0; i < outBytes.length; i++) {
//								System.out.print(outBytes[i] + " ");
//							}
//							System.out.println();
							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
							//broadcastedInputConf.firstIterationCompleted();
						} else if (broadcastedTypeInfo.toString().contains("Tuple2")) {
							// code for exus here;
							Tuple2[] in = new Tuple2[1];
							Tuple2[] in2 = new Tuple2[1];
							Tuple2[] out = new Tuple2[1];

							int size = 0;
							int arrayPos = -1;
							byte[] outBytes;
							if (fct0.getReturnArrayField()) {
								size = fct0.getReturnArrayFieldTotalBytes();
								arrayPos = fct0.getReturnTupleArrayFieldNo();
								for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
									if (i != arrayPos) {
										if (fct0.getDifferentTypesRet()) {
											size += 8;
										} else {
											size += fct0.getFieldSizesRet().get(i);
										}
									}
								}
								outBytes = new byte[numOfElements * size];
							} else {
								outBytes = new byte[numOfElements * returnSize];
							}
							fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
							fct0.setBroadcastedSize(broadcastedSize);
							FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
							if (BREAKDOWN) {
								endCompInfo = System.currentTimeMillis();
								FlinkTornadoVMLog.printBreakDown("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));
							}
							//long endCompInfo = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));
							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);
							TaskSchedule ts = new TaskSchedule("s0")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t0", msk::map, in, in2, out)
								.streamOut(outBytes);
							worker.setGlobalWork(numOfElements, 1, 1);
							long start, end;
							if (BREAKDOWN) {
								start = System.currentTimeMillis();
								ts.execute(gridScheduler);
								end = System.currentTimeMillis();
								FlinkTornadoVMLog.printBreakDown("**** Function " + function + " execution time: " + (end - start));
							} else {
								ts.execute(gridScheduler);
							}
							//FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " execution time: " + (end - start));
							//System.out.println("**** Function " + function + " execution time: " + (end - start));
							int numOfElementsRes;
							acres.setRawData(outBytes);
							/*System.out.println("MAPDRIVER OUT: ");
							for (int i = 0; i < outBytes.length; i++) {
							    System.out.print(outBytes[i] + " ");
							}
							System.out.println("\n");*/
							if (fct0.getReturnArrayField()) {
								acres.hasArrayField();
								acres.setLengthOfArrayField(arrayLength);
								acres.setArrayFieldNo(0);
								int totalB = fct0.getReturnArrayFieldTotalBytes();
								for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
									if (i != arrayPos) {
										totalB += fct0.getFieldSizesRet().get(i);
									}
								}
								acres.setTotalBytes(totalB);
								acres.setRecordSize(size);
								numOfElementsRes = outBytes.length / size;
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(size);
							} else {
								numOfElementsRes = outBytes.length / returnSize;
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(returnSize);
							}
						}
					} else {
						//byte[] outBytes = new byte[numOfElements * returnSize];
						//FlinkData f = new FlinkData(inputByteData, outBytes);
						byte[] outBytes;
						int size = 0;
						int arrayPos = -1;
						if (fct0.getReturnArrayField()) {
						    size = fct0.getReturnArrayFieldTotalBytes();
						    arrayPos = fct0.getReturnTupleArrayFieldNo();
						    for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
							if (i != arrayPos) {
							    if (fct0.getDifferentTypesRet()) {
								size += 8;
							    } else {
								size += fct0.getFieldSizesRet().get(i);
							    }
							}
						    }
						    outBytes = new byte[numOfElements * size];
						} else {
						    outBytes = new byte[numOfElements * returnSize];
						}

//						System.out.println("Map INPUT");
//						for (int i = 0; i < inputByteData.length; i++) {
//						    System.out.print(inputByteData[i] + " ");
//						}
//						System.out.println("\n");
						Tuple2[] in = new Tuple2[1];
						Tuple2[] out = new Tuple2[1];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						//TornadoMap4 msk4 = new transformUDF4(mapUserClassName);
						// TODO: Change to getting byte array from configuration and loading it
						//TornadoMap4 msk4 = AcceleratedFlinkUserFunction.getTornadoMap4();
						// get asm skeleton in byte form
						WorkerGrid worker = new WorkerGrid1D(numOfElements);
						GridScheduler gridScheduler = new GridScheduler("s3.t3", worker);
						byte[] def3 = new byte[0];
						byte[] bytesMsk3 = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes3(def3);
						TaskSchedule ts;
						if (bytesMsk3.length != 0) {
							AsmClassLoader loader3 = new AsmClassLoader();
							// either load class using classloader or retrieve loaded class
							MiddleMap3 md3 = loader3.loadClass3("org.apache.flink.api.asm.MapASMSkeleton3", bytesMsk3);
							TornadoMap3 msk3 = new TornadoMap3(md3);
							ts = new TaskSchedule("s3")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t3", msk3::map, in, out)
								.streamOut(outBytes);
							//	.execute();
						} else {
							ts = new TaskSchedule("s3")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t3", msk::map, in, out)
								.streamOut(outBytes);
							//	.execute();
						}
						worker.setGlobalWork(numOfElements, 1, 1);
					//	long start = System.currentTimeMillis();
						ts.execute(gridScheduler);
					//	long end = System.currentTimeMillis();
					   //     System.out.println("**** Function " + function + " execution time: " + (end - start));
						//int numOfElementsRes = outBytes.length / returnSize;
						//acres.setRawData(outBytes);
						//acres.setInputSize(numOfElementsRes);
						//acres.setReturnSize(returnSize);
						int numOfElementsRes;

						acres.setRawData(outBytes);
						if (fct0.getReturnArrayField()) {
							acres.hasArrayField();
							acres.setLengthOfArrayField(acdata.getLengthOfArrayField());
							acres.setArrayFieldNo(acdata.getArrayFieldNo());
							int totalB = fct0.getReturnArrayFieldTotalBytes();
							for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
							    if (i != arrayPos) {
								totalB += fct0.getFieldSizesRet().get(i);
							    }
							}
							acres.setTotalBytes(totalB);
							acres.setRecordSize(size);
							numOfElementsRes = outBytes.length / size;
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(size);
						} else {
						       numOfElementsRes = outBytes.length / returnSize;
						       acres.setInputSize(numOfElementsRes);
						       acres.setReturnSize(returnSize);
						}
					}

				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
					Tuple2[] in = new Tuple2[numOfElements];
					Tuple3[] out = new Tuple3[numOfElements];
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();

					int returnSize = 0;
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					// TODO: Change to getting byte array from configuration and loading i

					byte[] outBytes = new byte[numOfElements * returnSize];

					FlinkData f = new FlinkData(inputByteData, outBytes);

					byte[] def2 = new byte[0];
					byte[] bytesMsk2 = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes2(def2);
					if (bytesMsk2.length != 0) {
						AsmClassLoader loader2 = new AsmClassLoader();
						// either load class using classloader or retrieve loaded class
						MiddleMap2 md2 = loader2.loadClass2("org.apache.flink.api.asm.MapASMSkeleton2", bytesMsk2);
						TornadoMap2 msk2 = new TornadoMap2(md2);

						new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk2::map, in, out)
							.streamOut(outBytes)
							.execute();
					} else {
						new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk::map, in, out)
							.streamOut(outBytes)
							.execute();
					}

					//byte[] outT = f.getByteResults();

					//TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(deviceIndex).reset();

					int numOfElementsRes = outBytes.length / returnSize;
					acres.setRawData(outBytes);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(returnSize);
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {

					int returnSize = 0;
					long startCompInfo = 0, endCompInfo;
					if (BREAKDOWN) {
						startCompInfo = System.currentTimeMillis();
					}
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					if (broadcastedDataset) {
						setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
						fct0.setBroadcastedDataset(true);
						fct0.setBroadCollectionSize(broadCollectionSize);
						fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
						if (broadcastedTypeInfo.toString().contains("Tuple2")) {
							// code for exus here;
							int size = 0;
							int arrayPos = -1;
							byte[] outBytes;
							if (fct0.getReturnArrayField()) {
								size = fct0.getReturnArrayFieldTotalBytes();
								arrayPos = fct0.getReturnTupleArrayFieldNo();
								for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
									if (i != arrayPos) {
										if (fct0.getDifferentTypesRet()) {
											size += 8;
										} else {
											size += fct0.getFieldSizesRet().get(i);
										}
									}
								}
								outBytes = new byte[numOfElements * size];
							} else {
								outBytes = new byte[numOfElements * returnSize];
							}

							fct0.setBroadcastedSize(broadcastedSize);
							FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
							if (BREAKDOWN) {
								endCompInfo = System.currentTimeMillis();
								FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));
							}
							//System.out.println("EXUS");
							// TODO: Change to getting byte array from configuration and loading it
							//TornadoMap3 msk3 = AcceleratedFlinkUserFunction.getTornadoMap3();
							TaskSchedule ts;
							byte[] def4 = new byte[0];
							byte[] bytesMsk4 = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes4(def4);

							if (bytesMsk4.length != 0) {
								AsmClassLoader loader4 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap4 md4 = loader4.loadClass4("org.apache.flink.api.asm.MapASMSkeleton4", bytesMsk4);
								TornadoMap4 msk4 = new TornadoMap4(md4);
								Tuple2[] in = new Tuple2[1];
								Tuple4[] out = new Tuple4[1];
								Tuple2[] in2 = new Tuple2[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s4.t4", worker);

								ts = new TaskSchedule("s4")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t4", msk4::map, in, in2, out)
									.streamOut(outBytes);
								worker.setGlobalWork(numOfElements, 1, 1);
								if (BREAKDOWN) {
									long start = System.currentTimeMillis();
									ts.execute(gridScheduler);
									long end = System.currentTimeMillis();
									FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " execution time: " + (end - start));
								} else {
									ts.execute(gridScheduler);
								}
							} else {
								Tuple2[] in = new Tuple2[numOfElements];
								Tuple4[] out = new Tuple4[numOfElements];
								Tuple2[] in2 = new Tuple2[broadcastedSize];

								ts = new TaskSchedule("s4")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t4", msk::map, in, in2, out)
									.streamOut(outBytes);

								if (BREAKDOWN) {
									long start = System.currentTimeMillis();
									ts.execute();
									long end = System.currentTimeMillis();
									FlinkTornadoVMLogger.printBreakDown("**** Function " + function + " execution time: " + (end - start));
								} else {
									ts.execute();
								}
							}

							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
							if (fct0.getReturnArrayField()) {
								acres.hasArrayField();
								acres.setLengthOfArrayField(arrayLength);
								acres.setArrayFieldNo(0);
								int totalB = fct0.getReturnArrayFieldTotalBytes();
								for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
									if (i != arrayPos) {
										totalB += fct0.getFieldSizesRet().get(i);
									}
								}
								acres.setTotalBytes(totalB);
								acres.setRecordSize(size);
							}
						}
					}
				}

			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {

				if (outOwner.equals("java/lang/Integer")) {
					Tuple3[] in = new Tuple3[numOfElements];
					int[] out = new int[numOfElements];
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(int.class);

					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();
					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(4);
				} else if (outOwner.equals("java/lang/Double")) {
					Tuple3[] in = new Tuple3[numOfElements];
					double[] out = new double[numOfElements];
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(double.class);

					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();
					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(8);
				} else if (outOwner.equals("java/lang/Float")) {
					Tuple3[] in = new Tuple3[numOfElements];
					float[] out = new float[numOfElements];
					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(long.class);

					byte[] outBytes = new byte[numOfElements * 4];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess4(outBytes);

					int numOfElementsRes = outT.length / 4;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(4);
				} else if (outOwner.equals("java/lang/Long")) {
					Tuple3[] in = new Tuple3[numOfElements];
					long[] out = new long[numOfElements];

					fct0 = new FlinkCompilerInfo();
					fct0.setStoreJavaKind(long.class);

					byte[] outBytes = new byte[numOfElements * 8];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					acres = new AccelerationData();

					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();

					byte[] outT = changeOutputEndianess8(outBytes);

					int numOfElementsRes = outT.length / 8;
					acres.setRawData(outT);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(8);
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple2")) {
					Tuple3[] in = new Tuple3[1];
					Tuple2[] out = new Tuple2[1];
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();
					int returnSize = 0;
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					byte[] outBytes = new byte[numOfElements * returnSize];

					if (broadcastedDataset) {
						setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
						fct0.setBroadcastedDataset(true);
						fct0.setBroadCollectionSize(broadCollectionSize);
						fct0.setCollectionName(taskContext.getTaskConfig().getBroadcastInputName(0));
						if (broadcastedTypeInfo.toString().contains("Integer")) {
							// code for dft here;
							//Tuple2[] in2 = new Tuple2[broadcastedSize];
							Integer[] in2 = new Integer[1];

							//byte[] outBytes;

							fct0.setBroadcastedSize(broadcastedSize);
							FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
						//	long endCompInfo = System.currentTimeMillis();
							//System.out.println("**** Function " + function + " set compiler info: " + (endCompInfo - startCompInfo));
							//System.out.println("DFT");

							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s4.t4", worker);
							TaskSchedule ts = new TaskSchedule("s4")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t4", msk::map, in, in2, out)
								.streamOut(outBytes);
						//	long start = System.currentTimeMillis();
							worker.setGlobalWork(numOfElements, 1, 1);
							worker.setLocalWork(64, 1, 1);
							ts.execute(gridScheduler);
						//	long end = System.currentTimeMillis();
						//	System.out.println("**** Function " + function + " execution time: " + (end - start));
						}
						//byte[] outT = f.getByteResults();
						int numOfElementsRes = outBytes.length / returnSize;
						acres.setRawData(outBytes);
						acres.setInputSize(numOfElementsRes);
						acres.setReturnSize(returnSize);
					}
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
					Tuple3[] in = new Tuple3[numOfElements];
					Tuple3[] out = new Tuple3[numOfElements];
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();

					int returnSize = 0;
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}
					byte[] outBytes = new byte[numOfElements * returnSize];
					// TODO: Change to getting byte array from configuration and loading it
					//TornadoMap3 msk3 = AcceleratedFlinkUserFunction.getTornadoMap3();

					FlinkData f = new FlinkData(inputByteData, outBytes);

					byte[] def3 = new byte[0];
					byte[] bytesMsk3 = this.taskContext.getTaskConfig().getConfiguration().getSkeletonMapBytes3(def3);

					if (bytesMsk3.length != 0) {
						AsmClassLoader loader3 = new AsmClassLoader();
						// either load class using classloader or retrieve loaded class
						MiddleMap3 md3 = loader3.loadClass3("org.apache.flink.api.asm.MapASMSkeleton3", bytesMsk3);
						TornadoMap3 msk3 = new TornadoMap3(md3);

						new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk3::map, in, out)
							.streamOut(outBytes)
							.execute();
					} else {
						new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk::map, in, out)
							.streamOut(outBytes)
							.execute();
					}

					//byte[] outT = f.getByteResults();
					int numOfElementsRes = outBytes.length / returnSize;
					acres.setRawData(outBytes);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(returnSize);
				}

			} else if (inOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
				Tuple4[] in = new Tuple4[numOfElements];

				if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
					fct0 = new FlinkCompilerInfo();

					Tuple3[] out = new Tuple3[numOfElements];
					acres = new AccelerationData();
					int returnSize = 0;
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					byte[] outBytes = new byte[numOfElements * returnSize];
					FlinkData f = new FlinkData(inputByteData, outBytes);

					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();
					int numOfElementsRes = outBytes.length / returnSize;
					acres.setRawData(outBytes);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(returnSize);
				} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
					Tuple4[] out = new Tuple4[numOfElements];
					fct0 = new FlinkCompilerInfo();
					acres = new AccelerationData();

					int returnSize = 0;
					for (String name : AbstractInvokable.typeInfo.keySet()) {
						if (name.contains(function.getClass().toString().replace("class ", ""))) {
							returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
							break;
						}
					}

					byte[] outBytes = new byte[numOfElements * returnSize];
					FlinkData f = new FlinkData(inputByteData, outBytes);
					new TaskSchedule("s0")
						.flinkCompilerData(fct0)
						.flinkInfo(f)
						.task("t0", msk::map, in, out)
						.streamOut(outBytes)
						.execute();

					//byte[] outT = f.getByteResults();
					int numOfElementsRes = outBytes.length / returnSize;
					acres.setRawData(outBytes);
					acres.setInputSize(numOfElementsRes);
					acres.setReturnSize(returnSize);
				}
			}
			tornadoVMCleanUp();
			//System.out.println("!!! Map outputCollector: " + this.taskContext.getOutputCollector());
			if (this.taskContext.getOutputCollector() instanceof ChainedAllReduceDriver) {
				ChainedAllReduceDriver chred = (ChainedAllReduceDriver) this.taskContext.getOutputCollector();
				((CountingCollector) output).numRecordsOut.inc(acres.getInputSize());
				chred.collect(acres);
			} else if (this.taskContext.getOutputCollector() instanceof ChainedMapDriver) {
				ChainedMapDriver chmad = (ChainedMapDriver) this.taskContext.getOutputCollector();
				((CountingCollector) output).numRecordsOut.inc(acres.getInputSize());
				chmad.collect(acres);
			} else {
				materializeTornadoBuffers(acres, outOwner, fct0);
			}
			return;
		}
		if (objectReuseEnabled) {
			IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();

			while (this.running && ((record = input.next(record)) != null)) {
				numRecordsIn.inc();
				output.collect(function.map(record));
			}
		} else {
			IT record = null;

			while (this.running && ((record = input.next()) != null)) {
				numRecordsIn.inc();
				output.collect(function.map(record));
			}

		}
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
			//	long breakFieldsStart = System.currentTimeMillis();
				byte[] recordBytes = getReturnRecordArrayField(out, numOfBytes, tupleReturnSizes,lengthOfArrayField,arrayFieldNo, totalBytes, diffReturnSize);
			//	long breakFieldsEnd = System.currentTimeMillis();
				if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
					System.out.println("WRITE RECORD IN COUNTINGCOLLECTOR");
					((CountingCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				} else if (this.taskContext.getOutputCollector() instanceof WorksetUpdateOutputCollector) {
					System.out.println("WRITE RECORD IN WORKSETUPDATEOUTPUTCOLLECTOR");
					((WorksetUpdateOutputCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				}
				numOfBytes += recordSize;
				//total += (breakFieldsEnd - breakFieldsStart);
			}
			//System.out.println("**** Function " + this.mapper + " break data into records: " + total);
			return;
		}
		if (type.equals("java/lang/Float[]")) {
			//System.out.println("Results are float[]");
			int numOfColumns = fct.getColumnSizeMatrix2();
			int typeSize = fct.getMatrixTypeSize();
			int numOfRows = fct.getRowSizeMatrix1();
			// 4 bytes for size + 4 bytes for columnsize
			// format of column: [0 0 0 24] [0 0 0 4] 1 |64 -128 0 0| 1 |64 -96 0 0| 1 |64 -64 0 0| 1 |64 -32 0 0|
			int recordSize =  typeSize*numOfColumns;
			int numOfBytesInRow = 4 + numOfColumns + recordSize; //8 + recordSize;
			DataTransformation dtrans = new DataTransformation();
			long totalTransfTime = 0;
			long startTrans, endTrans;
			for (int i = 0; i < out.length; i+=recordSize) {
			//	startTrans = System.currentTimeMillis();
				byte[] currentRow = Arrays.copyOfRange(out, i, i + recordSize);
				byte[] recordBytes = dtrans.getMatrixRowBytes(currentRow, numOfBytesInRow, numOfColumns,recordSize);
			//	endTrans = System.currentTimeMillis();
			//	totalTransfTime += (endTrans - startTrans);
				if (this.taskContext.getOutputCollector() instanceof CountingCollector) {
					((CountingCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				} else if (this.taskContext.getOutputCollector() instanceof OutputCollector) {
					((OutputCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
				}
			}
			//System.out.println("**** Function " + this.taskContext.getStub() + " data transformation: " + totalTransfTime);
		} else if (type.equals("java/lang/Integer")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Integer in = bf.getInt(i);
				this.taskContext.getOutputCollector().collect((OT) in);
				//records.add((OT) in);
			}
		} else if (type.equals("java/lang/Double")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Double d = bf.getDouble(i);
				this.taskContext.getOutputCollector().collect((OT) d);
				//records.add((OT) d);
			}
		} else if (type.equals("java/lang/Float")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Float f = bf.getFloat(i);
				this.taskContext.getOutputCollector().collect((OT) f);
				//records.add((OT) f);
			}
		} else if (type.equals("java/lang/Long")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Long l = bf.getLong(i);
				this.taskContext.getOutputCollector().collect((OT) l);
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
				} else if (this.taskContext.getOutputCollector() instanceof OutputCollector) {
					((OutputCollector) this.taskContext.getOutputCollector()).collect(recordBytes);
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
					DataTransformation<OT> dtrans = new DataTransformation();
				//	long materializeStart = System.currentTimeMillis();
					ArrayList<OT> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
				//	long materializeEnd = System.currentTimeMillis();
					//System.out.println("**** Function " + this.mapper + " materialize data to be sent to groupBy: " + (materializeEnd - materializeStart));
					for (OT record : records) {
						(((CountingCollector<OT>) this.taskContext.getOutputCollector()).getCollector()).collect(record);
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
					DataTransformation<OT> dtrans = new DataTransformation();
					ArrayList<OT> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					for (OT record : records) {
						(((CountingCollector<OT>) this.taskContext.getOutputCollector()).getCollector()).collect(record);
					}
				} else {
					DataTransformation dtrans = new DataTransformation();
					for (int i = 0; i < out.length; i += sizeOftuple) {
					//	long breakRecordsStart = System.currentTimeMillis();
						byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
					//	long breakRecordsEnd = System.currentTimeMillis();
						((CountingCollector)this.taskContext.getOutputCollector()).collect(recordBytes);
					//	total += (breakRecordsEnd - breakRecordsStart);
					}
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

	/***
	 * TODO: Make this function generic, at the moment it is tailored to Exus use case binary data
	 */
	public static byte[] getBinaryRawData(ArrayList<byte[]> binaryData) {
		// if binaryData is empty return
		if (binaryData.size() == 0) return null;

		// we know at this point that binaryData contains at least one byte array so this is safe
		byte[] firstB = binaryData.get(0);
		// calculate size of record in split
		byte[] firstFieldSizeBytes = new byte[] {firstB[0], firstB[1], firstB[2], firstB[3]};
		int firstFieldSize = ByteBuffer.wrap(firstFieldSizeBytes).getInt();
		//  System.out.println("first size: " + firstFieldSize);
		int secondFieldOff = 4 + 8 * firstFieldSize;
		byte[] secondFieldSizeInBytes = new byte[8];
		for (int i = 0; i < 8; i++) {
			secondFieldSizeInBytes[i] = firstB[secondFieldOff + i];
		}
		int secondFieldSize = (int) ByteBuffer.wrap(secondFieldSizeInBytes).getDouble();
		//	System.out.println("second size: " + secondFieldSize);
		int tupleSizeInRec = secondFieldOff + 8; //+ secondFieldSize * 8;

		int[] numOfRecordsPerSplit = new int[binaryData.size()];
		//System.out.println("++++++++++++++ numOfRecordsPerSplit size: " + numOfRecordsPerSplit.length);
		// we know that all splits have at least one record
		Arrays.fill(numOfRecordsPerSplit, 1);
		int numOfTotalBytes = 0;

		//System.out.println("=== Number of splits: " + binaryData.size());

		for (int k = 0; k < binaryData.size(); k++) {
			// calculate records per byte array
			byte[] b = binaryData.get(k);
			boolean data = true;
			int i = tupleSizeInRec;

			while (data) {
				if (b[i] == firstFieldSizeBytes[0] && b[i + 1] == firstFieldSizeBytes[1] && b[i + 2] == firstFieldSizeBytes[2] && b[i + 3] == firstFieldSizeBytes[3]) {
					i += tupleSizeInRec;
					numOfRecordsPerSplit[k] += 1;
				} else {
					data = false;
				}
			}
//			System.out.println("- block : " + k + " bytes: ");
//			for (int w = 0; w < b.length; w++) {
//				System.out.print(b[w] + " ");
//			}

			//	System.out.println("\n------------------------");
			//	System.out.println("number of tuples in this record: " + numOfRecordsPerSplit[k]);
		}
		for (int i = 0; i < numOfRecordsPerSplit.length; i++) {
			numOfTotalBytes += numOfRecordsPerSplit[i]* (firstFieldSize*8 + 8);
		}
		//System.out.println("total bytes size: " + numOfTotalBytes);
		if (numOfTotalBytes < Integer.MAX_VALUE - 8) {
			byte[] totalBytes = new byte[numOfTotalBytes];
			// EXUS USE CASE: <number of tuple elements> bytes of array <size of 2nd field> bytes of double
			// example: 0 0 0 83 64 1 -103 -103 -103 -103 -103 -102 ... 63 -16 0 0 0 0 0 0 64 62 0 0 0 0 0 0
			// 63 -16 0 0 0 0 0 0 = 1, since the second field consists of a single value in this case
			// TODO: Make this generic...use typeinfo to extract the information above
			int destIndx = 0;
			for (int j = 0; j < binaryData.size(); j++) {
				byte[] b = binaryData.get(j);
				int currTuple = 0;
				int firstHeader = 4;
				int secondHeader = 0;

				while (currTuple < numOfRecordsPerSplit[j]) {
					// <firstHeader  + (doubleSize*arrayElem) + (firstHeader + firstFieldSize*doubleSize + secondHeader + secondFieldSize) * currTuple>
					for (int arrayElem = 0; arrayElem < firstFieldSize; arrayElem++) {
						int startIndx = firstHeader + arrayElem*8 + (firstHeader + firstFieldSize*8 + secondHeader + 8)* currTuple;
						//System.out.println("-> F0) start index input: " + startIndx + " start index output: " + destIndx);
						//System.out.println("F0 write byte: " + b[startIndx]);
						changeOutputEndianess8Arr(b, totalBytes, startIndx, destIndx);
						destIndx+=8;
					}
					// <firstHeader + firstFieldSize*doubleSize + secondHeader + (firstHeader + firstFieldSize*doubleSize + secondHeader + secondFieldSize) * currTuple>
					int startIndx = firstHeader + firstFieldSize*8 + secondHeader + (firstHeader + firstFieldSize*8 + secondHeader + 8) * currTuple;
					//System.out.println("-> F1) start index input: " + startIndx + " start index output: " + destIndx);
					//System.out.println("F1 write byte: " + b[startIndx]);
					changeOutputEndianess8Arr(b, totalBytes, startIndx, destIndx);
					destIndx+=8;
					currTuple++;
				}
			}
//			System.out.println("Total bytes:");
//			for (int i = 0; i < totalBytes.length; i++) {
//				System.out.print(totalBytes[i] + " ");
//			}
//			System.out.println("\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			return totalBytes;
		} else {
			// multiple arrays
			//	System.out.println("MAXIMUM ARRAY SIZE!!!!!");
			return null;
		}
	}

	public static byte[] changeOutputEndianess8Arr(byte[] input, byte[] output, int startIndex, int startIndexW) {
		byte tmp;
		//for (int i = startIndex; i < output.length; i += 8) {
		// swap 0 and 7
		tmp = input[startIndex];
		output[startIndexW] = input[startIndex + 7];
		output[startIndexW + 7] = tmp;
		// swap 1 and 6
		tmp = input[startIndex + 1];
		output[startIndexW + 1] = input[startIndex + 6];
		output[startIndexW + 6] = tmp;
		// swap 2 and 5
		tmp = input[startIndex + 2];
		output[startIndexW + 2] = input[startIndex + 5];
		output[startIndexW + 5] = tmp;
		// swap 3 and 4
		tmp = input[startIndex + 3];
		output[startIndexW + 3] = input[startIndex + 4];
		output[startIndexW + 4] = tmp;
		//	}
		return output;
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
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {
		this.running = false;
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
