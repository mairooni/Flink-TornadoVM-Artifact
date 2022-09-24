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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.HashMap;

//import static org.apache.flink.api.asm.ExamineUDF.inOwner;
//import static org.apache.flink.api.asm.ExamineUDF.outOwner;
import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.examineTypeInfoForFlinkUDFs;
import static org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable.tailAccData;

/**
 * Chained Map Driver.
 */
public class ChainedMapDriver<IT, OT> extends ChainedDriver<IT, OT> {

	private MapFunction<IT, OT> mapper;

	private byte[] inputByteData;

	private int numOfElements;

	private boolean accelerateWithTornadoVM = Boolean.parseBoolean(System.getProperty("tornado", "false"));
	private boolean precompiled = Boolean.parseBoolean(System.getProperty("precompiledKernel", "false"));

	public static final boolean BREAKDOWN = Boolean.parseBoolean(System.getProperties().getProperty("flinktornado.breakdown", "false"));

	private static final int INT_LENGTH = 4;

	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	private boolean fromSerialExecutionToTornado;
	private ArrayList<IT> inputData = new ArrayList<>();

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		final MapFunction<IT, OT> mapper =
			BatchTask.instantiateUserCode(this.config, userCodeClassLoader, MapFunction.class);
		this.mapper = mapper;
		FunctionUtils.setFunctionRuntimeContext(mapper, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.mapper, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		BatchTask.closeUserCode(this.mapper);
	}

	@Override
	public void cancelTask() {
		try {
			FunctionUtils.closeFunction(this.mapper);
		} catch (Throwable t) {
			// Ignore exception.
		}
	}

	// --------------------------------------------------------------------------------------------

	public Function getStub() {
		return this.mapper;
	}

	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------

	private void createTaskScheduleAndRun(TornadoMap msk, int[] in, int[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, int[] in, float[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, int[] in, double[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, int[] in, long[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, double[] in, int[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, double[] in, float[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, double[] in, double[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, double[] in, long[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, long[] in, int[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, long[] in, float[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, long[] in, double[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, long[] in, long[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, float[] in, int[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, float[] in, float[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, float[] in, double[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, float[] in, long[] out, byte[] outBytes, FlinkData f) {
		FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private void createTaskScheduleAndRun(TornadoMap msk, Tuple2[] in, int[] out, byte[] outBytes, FlinkData f, FlinkCompilerInfo fct0) {
		new TaskSchedule("s0")
			.flinkCompilerData(fct0)
			.flinkInfo(f)
			.task("t0", msk::map, in, out)
			.streamOut(outBytes)
			.execute();
	}

	private abstract static class TypeBytes {
		protected int bytes = 0;
		public int getBytes() {
			return bytes;
		}
	}

	private static class TInteger extends TypeBytes {
		public TInteger() {
			bytes = 4;
		}
	}

	private static class TFloat extends TypeBytes {
		public TFloat() {
			bytes = 4;
		}
	}

	private static class TLong extends TypeBytes {
		public TLong() {
			bytes = 8;
		}
	}

	private static class TDouble extends TypeBytes {
		public TDouble() {
			bytes = 8;
		}
	}

	private AccelerationData examineType(FlinkCompilerInfo fct0) {
		AccelerationData acres = new AccelerationData();
		for (String name : AbstractInvokable.typeInfo.keySet()) {
			if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
				examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
				break;
			}
		}
		return acres;
	}

	public void collect (AccelerationData accData) {
		boolean tornadoConfFlag = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTornadoVMFlag();
		if (accelerateWithTornadoVM || tornadoConfFlag) {
			String mapUserClassName = this.mapper.getClass().toString().replace("class ", "").replace(".", "/");
			// TODO: Change to getting byte array from configuration and loading it
			Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesMap.getTypes(mapUserClassName);
			String inOwner = inOutTypes.f0;
			String outOwner = inOutTypes.f1;
			// Input data to the Map Operator
			inputByteData = accData.getRawData();

			// Number of elements to be processed
			numOfElements = accData.getInputSize();
			numRecordsIn.inc(numOfElements);
			TornadoMap msk = null;
			if (BREAKDOWN) {
				long skelStart = System.currentTimeMillis();
				// get asm skeleton in byte form
				byte[] def = new byte[0];
				byte[] bytesMsk = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleMap md = loader.loadClass("org.apache.flink.api.asm.MapASMSkeleton", bytesMsk);
				msk = new TornadoMap(md);
				long skelEnd = System.currentTimeMillis();
				FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " create ASM skeleton: " + (skelEnd - skelStart));
			} else {
				// get asm skeleton in byte form
				byte[] def = new byte[0];
				byte[] bytesMsk = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleMap md = loader.loadClass("org.apache.flink.api.asm.MapASMSkeleton", bytesMsk);
				msk = new TornadoMap(md);
			}
			TypeBytes typeBytes = null;
			FlinkData flinkData = null;
			byte[] outBytes = null;
			AccelerationData acres = null;
			FlinkCompilerInfo fct0 = null;
			//System.out.println("**** Function " + this.mapper + " create ASM skeleton: " + (skelEnd - skelStart));
			switch (inOwner) {
				case "java/lang/Integer": {
					int[] in = new int[numOfElements];
					/* Based on the output data type create the output array and the Task Schedule
					 * Currently, the input and output data can only be of type Integer, Double, Float or Long.
					 * Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
					 * since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
					 */
					switch (outOwner) {
						case "java/lang/Integer": {
							int[] out = new int[numOfElements];
							outBytes = new byte[numOfElements * INT_LENGTH];
							typeBytes = new TInteger();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Double": {
							double[] out = new double[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TDouble();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Float": {
							float[] out = new float[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TFloat();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Long": {
							long[] out = new long[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TLong();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
					}
					outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
					int numOfElementsRes = outBytes.length / typeBytes.getBytes();
					acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
					break;
				}
				case "java/lang/Double": {
					double[] in = new double[numOfElements];

					switch (outOwner) {
						case "java/lang/Integer": {
							int[] out = new int[numOfElements];
							typeBytes = new TInteger();
							outBytes = new byte[numOfElements * 4];
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Double": {
							double[] out = new double[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TDouble();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Float": {
							float[] out = new float[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TFloat();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Long": {
							long[] out = new long[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TLong();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
					}
					outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
					int numOfElementsRes = outBytes.length / typeBytes.getBytes();
					acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
					break;
				}
				case "java/lang/Float": {
					float[] in = new float[numOfElements];

					switch (outOwner) {
						case "java/lang/Integer": {
							int[] out = new int[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TInteger();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Double": {
							double[] out = new double[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TDouble();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Float": {
							float[] out = new float[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TFloat();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Long": {
							long[] out = new long[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TLong();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
					}
					outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
					int numOfElementsRes = outBytes.length / typeBytes.getBytes();
					acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
					break;
				}
				case "java/lang/Long": {
					long[] in = new long[numOfElements];

					switch (outOwner) {
						case "java/lang/Integer": {
							int[] out = new int[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TInteger();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Double": {
							double[] out = new double[numOfElements];
							outBytes = new byte[numOfElements * 8];
							typeBytes = new TDouble();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Float": {
							float[] out = new float[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TFloat();
							flinkData = new FlinkData(inputByteData, outBytes);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							break;
						}
						case "java/lang/Long": {
							if (precompiled) {
								byte[] size = new byte[4]; //numOfElements;
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								ByteBuffer.wrap(size).putInt(numOfElements);
								changeOutputEndianess4(size);
								FlinkData f = new FlinkData(true, inputByteData, outBytes);
								String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
								TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
								new TaskSchedule("s0")
									.flinkInfo(f)
									.prebuiltTask("t0",
										"map",
										path + "/pre-compiled/pi-map.cl",
										new Object[]{inputByteData, outBytes},
										new Access[]{Access.READ, Access.WRITE},
										defaultDevice,
										new int[]{numOfElements})
									.streamOut(outBytes)
									.execute();
							} else {
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
							}
							break;
						}
					}
					if (this.outputCollector instanceof CountingCollector) {
						if (((CountingCollector)this.outputCollector).collector instanceof ChainedAllReduceDriver) {
							// don't change endianess if collector is chained reducer
						} else {
							outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
						}
					} else {
						outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
					}
					//outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
					int numOfElementsRes = outBytes.length / typeBytes.getBytes();
					acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
					break;
				}
				case "org/apache/flink/api/java/tuple/Tuple2": {

					switch (outOwner) {
						case "java/lang/Integer": {
							Tuple2[] in = new Tuple2[numOfElements];
							int[] out = new int[numOfElements];
							outBytes = new byte[numOfElements * 4];
							typeBytes = new TInteger();
							flinkData = new FlinkData(inputByteData, outBytes);
							fct0 = new FlinkCompilerInfo();
							fct0.setStoreJavaKind(int.class);

							acres = examineType(fct0);
							createTaskScheduleAndRun(msk, in, out, outBytes, flinkData, fct0);

							outBytes = changeOutputEndianess4(outBytes);
							int numOfElementsRes = outBytes.length / typeBytes.getBytes();
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(typeBytes.getBytes());
							break;
						}
						case "java/lang/Double": {
							Tuple2[] in = new Tuple2[numOfElements];
							double[] out = new double[numOfElements];
							outBytes = new byte[numOfElements * 8];
							fct0 = new FlinkCompilerInfo();
							fct0.setStoreJavaKind(double.class);
							acres = examineType(fct0);

							flinkData = new FlinkData(inputByteData, outBytes);
							new TaskSchedule("s0")
								.flinkCompilerData(fct0)
								.flinkInfo(flinkData)
								.task("t0", msk::map, in, out)
								.streamOut(outBytes)
								.execute();

							outBytes = changeOutputEndianess8(outBytes);

							int numOfElementsRes = outBytes.length / 8;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(8);
							break;
						}
						case "java/lang/Float": {
							Tuple2[] in = new Tuple2[numOfElements];
							float[] out = new float[numOfElements];
							outBytes = new byte[numOfElements * 4];

							fct0 = new FlinkCompilerInfo();
							fct0.setStoreJavaKind(float.class);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
									examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
									break;
								}
							}

							if (precompiled) {
								byte[] size = new byte[4]; //numOfElements;
								ByteBuffer.wrap(size).putInt(numOfElements);
								changeOutputEndianess4(size);
								FlinkData f = new FlinkData(true, inputByteData, size, outBytes);
								String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
								TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
								new TaskSchedule("s0")
									.flinkInfo(f)
									.prebuiltTask("t0",
										"map",
										path + "/pre-compiled/vadd-map.cl",
										new Object[] { inputByteData, size, outBytes},
										new Access[] { Access.READ, Access.READ, Access.WRITE },
										defaultDevice,
										new int[] {numOfElements})
									.streamOut(outBytes)
									.execute();
							} else {
								FlinkData f = new FlinkData(inputByteData, outBytes);
								new TaskSchedule("s0")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t0", msk::map, in, out)
									.streamOut(outBytes)
									.execute();
							}
							byte[] outT = changeOutputEndianess4(outBytes);

							int numOfElementsRes = outT.length / 4;
							acres.setRawData(outT);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(4);
							break;
						}
						case "java/lang/Long": {
							Tuple2[] in = new Tuple2[numOfElements];
							long[] out = new long[numOfElements];
							outBytes = new byte[numOfElements * 8];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							fct0 = new FlinkCompilerInfo();
							fct0.setStoreJavaKind(long.class);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

							byte[] outT = changeOutputEndianess8(outBytes);

							int numOfElementsRes = outT.length / 8;
							acres.setRawData(outT);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(8);
							break;
						}
						case "org/apache/flink/api/java/tuple/Tuple2": {
							//	long compStart = System.currentTimeMillis();
							FlinkData f;
							int returnSize = 0;
							acres = new AccelerationData();
							fct0 = new FlinkCompilerInfo();
							int size = 0;
							int arrayPos = -1;
							if (BREAKDOWN) {
								long infoStart = System.currentTimeMillis();

								for (String name : AbstractInvokable.typeInfo.keySet()) {
									int ref = name.indexOf("@");
									String classname = name.substring(0, ref);
									if (classname.equals(mapper.getClass().toString().replace("class ", ""))) {
										//System.out.println("-> name: " + name + " mapper: " + mapper.getClass().toString().replace("class ", ""));
										returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
										break;
									}
								}

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
								f = new FlinkData(inputByteData, outBytes);
								long infoEnd = System.currentTimeMillis();
								System.out.println("**** Function " +  this.mapper + " collect information: " + (infoEnd - infoStart));
							} else {
								for (String name : AbstractInvokable.typeInfo.keySet()) {
									int ref = name.indexOf("@");
									String classname = name.substring(0, ref);
									if (classname.equals(mapper.getClass().toString().replace("class ", ""))) {
										//System.out.println("-> name: " + name + " mapper: " + mapper.getClass().toString().replace("class ", ""));
										returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
										break;
									}
								}

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
								f = new FlinkData(inputByteData, outBytes);
							}
							//System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
							// TODO: Change to getting byte array from configuration and loading it
							//TornadoMap2 msk2 = AcceleratedFlinkUserFunction.getTornadoMap2();
//								System.out.println("======== Hi");
							TaskSchedule ts;
							byte[] def2 = new byte[0];
							byte[] bytesMsk2 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes2(def2);

							byte[] def3 = new byte[0];
							byte[] bytesMsk3 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes3(def3);

							if (bytesMsk2.length != 0 && mapper.toString().contains("UpdateAccumulator")) {
								AsmClassLoader loader2 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap2 md2 = loader2.loadClass2("org.apache.flink.api.asm.MapASMSkeleton2", bytesMsk2);
								TornadoMap2 msk2 = new TornadoMap2(md2);

								Tuple2[] out = new Tuple2[1];
								Tuple2[] in = new Tuple2[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s1.t1", worker);

								ts = new TaskSchedule("s1")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t1", msk2::map, in, out)
									.streamOut(outBytes);

								worker.setGlobalWork(numOfElements, 1, 1);
								if (BREAKDOWN) {
									long start = System.currentTimeMillis();
									ts.execute(gridScheduler);
									long end = System.currentTimeMillis();
									FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " execution time: " + (end - start));
								} else {
									ts.execute(gridScheduler);
								}
							} else if (bytesMsk3.length != 0) {
								AsmClassLoader loader3 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap3 md3 = loader3.loadClass3("org.apache.flink.api.asm.MapASMSkeleton3", bytesMsk3);
								TornadoMap3 msk3 = new TornadoMap3(md3);

								Tuple2[] out = new Tuple2[1];
								Tuple2[] in = new Tuple2[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s3.t3", worker);

								ts = new TaskSchedule("s3")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t3", msk3::map, in, out)
									.streamOut(outBytes);

								worker.setGlobalWork(numOfElements, 1, 1);
								//	long start = System.currentTimeMillis();
								if (BREAKDOWN) {
									long start = System.currentTimeMillis();
									ts.execute(gridScheduler);
									long end = System.currentTimeMillis();
									FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " execution time: " + (end - start));
								} else {
									ts.execute(gridScheduler);
								}
							} else {
								Tuple2[] in = new Tuple2[numOfElements];
								Tuple2[] out = new Tuple2[numOfElements];

								ts = new TaskSchedule("s3")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t3", msk::map, in, out)
									.streamOut(outBytes);
								//	long start = System.currentTimeMillis();
								ts.execute();
								//	long end = System.currentTimeMillis();
								//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
							}


							int numOfElementsRes; // = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							//acres.setInputSize(numOfElementsRes);
							//acres.setReturnSize(returnSize);
							//acres.setRecordSize(8);
							if (fct0.getReturnArrayField()) {
								acres.hasArrayField();
								acres.setLengthOfArrayField(accData.getLengthOfArrayField());
								acres.setArrayFieldNo(accData.getArrayFieldNo());
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
							// check if tail iteration
							tailAccData.set(acres);
							//tornadoVMCleanUp();
							//	System.out.println("== Chainmap conf " + conf);
							break;
						}
						case "org/apache/flink/api/java/tuple/Tuple3": {
							//	long compStart = System.currentTimeMillis();
							fct0 = new FlinkCompilerInfo();

							int returnSize = 0;
							acres = new AccelerationData();
							try {
								byte[] typeB = new byte[0];
								typeB = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTypeInfo(typeB);
								ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
								ObjectInputStream ins = new ObjectInputStream(bObj);
								HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
								ins.close();
								bObj.close();

								for (String name : typeInf.keySet()) {
									if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
										returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
										break;
									}
								}
							} catch (Exception e) {
								System.out.println(e);
							}
							//	long compEnd = System.currentTimeMillis();
							//System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
							// TODO: Change to getting byte array from configuration and loading it
							//TornadoMap2 msk2 = AcceleratedFlinkUserFunction.getTornadoMap2();

							outBytes = new byte[numOfElements * returnSize];

							FlinkData f = new FlinkData(inputByteData, outBytes);

							TaskSchedule ts;

							byte[] def2 = new byte[0];
							byte[] bytesMsk2 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes2(def2);

							if (bytesMsk2.length != 0) {
								AsmClassLoader loader2 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap2 md2 = loader2.loadClass2("org.apache.flink.api.asm.MapASMSkeleton2", bytesMsk2);
								TornadoMap2 msk2 = new TornadoMap2(md2);
								Tuple2[] in = new Tuple2[1];
								Tuple3[] out = new Tuple3[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s1.t1", worker);
								try {
									ts = new TaskSchedule("s1")
										.flinkCompilerData(fct0)
										.flinkInfo(f)
										.task("t1", msk2::map, in, out)
										.streamOut(outBytes);

									worker.setGlobalWork(numOfElements, 1, 1);
									//	long start = System.currentTimeMillis();
									ts.execute(gridScheduler);
									//	long end = System.currentTimeMillis();
									//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
								} catch (Error | Exception e) {
									LOG.info("An exception " + e + " occurred during the heterogeneous execution, computation falls back on the CPU");
									int sizeOftuple = 0;
									boolean padding = false;
									for (int i = 0; i < fct0.getFieldSizes().size(); i++) {
										for (int j = i + 1; j < fct0.getFieldSizes().size(); j++) {
											if (fct0.getFieldSizes().get(i) != fct0.getFieldSizes().get(j)) {
												padding = true;
												break;
											}
										}
									}
									int actualSize = 0;
									for (int fieldSize : fct0.getFieldSizes()) {
										actualSize += fieldSize;
									}
									if (padding) {
										sizeOftuple = fct0.getFieldSizes().size() * 8;
									} else {
										sizeOftuple = actualSize;
									}
									DataTransformation dtrans = new DataTransformation();
									ArrayList<IT> records = dtrans.materializeInputRecords(inOwner, inputByteData, fct0, padding, sizeOftuple, actualSize);
									try {
										for (IT record : records) {
											this.outputCollector.collect(this.mapper.map(record));
										}
										return;
									} catch (Exception e2) {
										System.out.println(e2);
									}
								}
							} else {
								Tuple2[] in = new Tuple2[numOfElements];
								Tuple3[] out = new Tuple3[numOfElements];

								ts = new TaskSchedule("s1")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t1", msk::map, in, out)
									.streamOut(outBytes);
								//	long start = System.currentTimeMillis();
								ts.execute();
								//	long end = System.currentTimeMillis();
								//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
							}

							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
							break;
						}
					}

					break;
				}
				case "org/apache/flink/api/java/tuple/Tuple3": {

					if (outOwner.equals("java/lang/Integer")) {
						Tuple3[] in = new Tuple3[numOfElements];
						int[] out = new int[numOfElements];
						fct0 = new FlinkCompilerInfo();
						fct0.setStoreJavaKind(int.class);

						outBytes = new byte[numOfElements * 4];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						acres = new AccelerationData();

						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

						outBytes = new byte[numOfElements * 8];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						acres = new AccelerationData();

						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

						outBytes = new byte[numOfElements * 4];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						acres = new AccelerationData();

						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

						outBytes = new byte[numOfElements * 8];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						acres = new AccelerationData();

						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

						byte[] outT = changeOutputEndianess8(outBytes);

						int numOfElementsRes = outT.length / 8;
						acres.setRawData(outT);
						acres.setInputSize(numOfElementsRes);
						acres.setReturnSize(8);
					} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
						//long compStart = System.currentTimeMillis();
						//out[0] = new Tuple3();
						//in[0] = new Tuple3();
						fct0 = new FlinkCompilerInfo();

						int returnSize = 0;
						acres = new AccelerationData();

						try {
							byte[] typeB = new byte[0];
							typeB = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTypeInfo(typeB);
							ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
							ObjectInputStream ins = new ObjectInputStream(bObj);
							HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
							ins.close();
							bObj.close();

							for (String name : typeInf.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
									returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
									break;
								}
							}
						} catch (Exception e) {
							System.out.println(e);
						}
//						for (String name : AbstractInvokable.typeInfo.keySet()) {
//							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
//								returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
//								break;
//							}
//						}
						outBytes = new byte[numOfElements * returnSize];
						//	long compEnd = System.currentTimeMillis();
						//System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
						// TODO: Change to getting byte array from configuration and loading it
						//TornadoMap3 msk3 = AcceleratedFlinkUserFunction.getTornadoMap3();

						FlinkData f = new FlinkData(inputByteData, outBytes);
						TaskSchedule ts;
						byte[] def3 = new byte[0];
						byte[] bytesMsk3 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes3(def3);

						if (bytesMsk3.length != 0) {
							AsmClassLoader loader3 = new AsmClassLoader();
							// either load class using classloader or retrieve loaded class
							MiddleMap3 md3 = loader3.loadClass3("org.apache.flink.api.asm.MapASMSkeleton3", bytesMsk3);
							TornadoMap3 msk3 = new TornadoMap3(md3);

							Tuple3[] in = new Tuple3[1];
							Tuple3[] out = new Tuple3[1];
							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s3.t3", worker);

							ts = new TaskSchedule("s3")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t3", msk3::map, in, out)
								.streamOut(outBytes);

							worker.setGlobalWork(numOfElements, 1, 1);
							//	long start = System.currentTimeMillis();
							ts.execute(gridScheduler);
							//	long end = System.currentTimeMillis();
							//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));

						} else {
							Tuple3[] in = new Tuple3[numOfElements];
							Tuple3[] out = new Tuple3[numOfElements];

							ts = new TaskSchedule("s3")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t3", msk::map, in, out)
								.streamOut(outBytes);

							//	long start = System.currentTimeMillis();
							ts.execute();
							//	long end = System.currentTimeMillis();
							//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
						}

						int numOfElementsRes = outBytes.length / returnSize;
						acres.setRawData(outBytes);
						acres.setInputSize(numOfElementsRes);
						acres.setReturnSize(returnSize);
					}

					break;
				}
				case "org/apache/flink/api/java/tuple/Tuple4": {

					if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
						Tuple4[] in = new Tuple4[numOfElements];
						fct0 = new FlinkCompilerInfo();

						Tuple3[] out = new Tuple3[numOfElements];
						int returnSize = 0;
						acres = new AccelerationData();

						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
								returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
								break;
							}
						}

						outBytes = new byte[numOfElements * returnSize];
						FlinkData f = new FlinkData(inputByteData, outBytes);

						new TaskSchedule("s0")
							.flinkCompilerData(fct0)
							.flinkInfo(f)
							.task("t0", msk::map, in, out)
							.streamOut(outBytes)
							.execute();

						int numOfElementsRes = outBytes.length / returnSize;
						acres.setRawData(outBytes);
						acres.setInputSize(numOfElementsRes);
						acres.setReturnSize(returnSize);
					} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
						long compStart = 0, compEnd;
						if (BREAKDOWN) {
							compStart = System.currentTimeMillis();
						}
						fct0 = new FlinkCompilerInfo();

						acres = new AccelerationData();
						int returnSize = 0;
						for (String name : AbstractInvokable.typeInfo.keySet()) {
							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
								returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
								break;
							}
						}
						if (BREAKDOWN) {
							compEnd = System.currentTimeMillis();
							FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
						}
						// TODO: Change to getting byte array from configuration and loading it
						//TornadoMap4 msk4 = AcceleratedFlinkUserFunction.getTornadoMap4();

//							System.out.println("==== Chained Map INPUT:");
//							for (int i = 0; i < inputByteData.length; i++) {
//								System.out.print(inputByteData[i] + " ");
//							}
//							System.out.println("\n");

						outBytes = new byte[numOfElements * returnSize];
						FlinkData f = new FlinkData(inputByteData, outBytes);
						TaskSchedule ts;

						byte[] def5 = new byte[0];
						byte[] bytesMsk5 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes5(def5);

						if (bytesMsk5.length != 0) {
							AsmClassLoader loader5 = new AsmClassLoader();
							// either load class using classloader or retrieve loaded class
							MiddleMap5 md5 = loader5.loadClass5("org.apache.flink.api.asm.MapASMSkeleton5", bytesMsk5);
							TornadoMap5 msk5 = new TornadoMap5(md5);
							Tuple4[] in = new Tuple4[1];
							Tuple4[] out = new Tuple4[1];
							WorkerGrid worker = new WorkerGrid1D(numOfElements);
							GridScheduler gridScheduler = new GridScheduler("s6.t6", worker);

							ts = new TaskSchedule("s6")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t6", msk5::map, in, out)
								.streamOut(outBytes);

							worker.setGlobalWork(numOfElements, 1, 1);
							if (BREAKDOWN) {
								long start = System.currentTimeMillis();
								ts.execute(gridScheduler);
								long end = System.currentTimeMillis();
								FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " execution time: " + (end - start));
							} else {
								ts.execute(gridScheduler);
							}
						} else {
							Tuple4[] in = new Tuple4[numOfElements];
							Tuple4[] out = new Tuple4[numOfElements];

							ts = new TaskSchedule("s6")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t6", msk::map, in, out)
								.streamOut(outBytes);
							//	long start = System.currentTimeMillis();
							ts.execute();
							//	long end = System.currentTimeMillis();
							//System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
						}

//							System.out.println("==== Chained Map Res:");
//							for (int i = 0; i < outBytes.length; i++) {
//								System.out.print(outBytes[i] + " ");
//							}
//							System.out.println("\n");

						int numOfElementsRes = outBytes.length / returnSize;
						acres.setRawData(outBytes);
						acres.setInputSize(numOfElementsRes);
						acres.setReturnSize(returnSize);

					}
					break;
				}
			}
			tornadoVMCleanUp();
			TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
			TypeSerializer<IT> serializer = serializerFactory.getSerializer();
			if (this.getOutputCollector() instanceof ChainedAllReduceDriver) {
				((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
				ChainedAllReduceDriver chred = (ChainedAllReduceDriver) getOutputCollector();
				chred.collect(acres);
			} else if (this.getOutputCollector() instanceof ChainedMapDriver) {
				ChainedMapDriver chmad = (ChainedMapDriver) getOutputCollector();
				((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
				chmad.collect(acres);
			} else if (this.getOutputCollector() instanceof  CountingCollector) {
				if (((CountingCollector)this.getOutputCollector()).collector instanceof ChainedAllReduceDriver) {
					((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) ((CountingCollector)this.getOutputCollector()).collector;
					chred.collect(acres);
				} else {
					materializeTornadoBuffers(acres, outOwner, fct0);
				}
		    } else {
				materializeTornadoBuffers(acres, outOwner, fct0);
			}
			// collector is OutputCollector
		}

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
			if (BREAKDOWN) {
				long endianessStart = System.currentTimeMillis();
				changeEndianess(true, out, recordSize, diffReturnSize, tupleReturnSizes);
				long endianessEnd = System.currentTimeMillis();
				FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " change endianess of result buffer: " + (endianessEnd - endianessStart));
				while (numOfBytes < out.length) {
					long breakFieldsStart = System.currentTimeMillis();
					byte[] recordBytes = getReturnRecordArrayField(out, numOfBytes, tupleReturnSizes,lengthOfArrayField,arrayFieldNo, totalBytes, diffReturnSize);
					long breakFieldsEnd = System.currentTimeMillis();
					if (outputCollector instanceof CountingCollector) {
						((CountingCollector) outputCollector).collect(recordBytes);
					}
					numOfBytes += recordSize;
					total += (breakFieldsEnd - breakFieldsStart);
				}
				FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " break data into records: " + total);
			} else {
				changeEndianess(true, out, recordSize, diffReturnSize, tupleReturnSizes);
				while (numOfBytes < out.length) {
					byte[] recordBytes = getReturnRecordArrayField(out, numOfBytes, tupleReturnSizes,lengthOfArrayField,arrayFieldNo, totalBytes, diffReturnSize);
					if (outputCollector instanceof CountingCollector) {
						((CountingCollector) outputCollector).collect(recordBytes);
					}
					numOfBytes += recordSize;

				}
			}
			return;
		}

		if (type.equals("java/lang/Integer")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Integer in = bf.getInt(i);
				outputCollector.collect((OT) in);
				//records.add((OT) in);
			}
		} else if (type.equals("java/lang/Double")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Double d = bf.getDouble(i);
				outputCollector.collect((OT) d);
				//records.add((OT) d);
			}
		} else if (type.equals("java/lang/Float")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=4) {
				Float f = bf.getFloat(i);
				outputCollector.collect((OT) f);
				//records.add((OT) f);
			}
		} else if (type.equals("java/lang/Long")) {
			ByteBuffer bf = ByteBuffer.wrap(out);
			for (int i = 0; i < out.length; i+=8) {
				Long l = bf.getLong(i);
				outputCollector.collect((OT) l);
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
					ArrayList<OT> records;
					if (BREAKDOWN) {
						DataTransformation<OT> dtrans = new DataTransformation();
						long materializeStart = System.currentTimeMillis();
						records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
						long materializeEnd = System.currentTimeMillis();
						FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " materialize data to be sent to groupBy: " + (materializeEnd - materializeStart));
					} else {
						DataTransformation<OT> dtrans = new DataTransformation();
						records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					}
					for (OT record : records) {
						(((CountingCollector<OT>) outputCollector).getCollector()).collect(record);
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

			// KMeans
			// TODO: include this option in the other cases
			if (outputCollector instanceof CountingCollector) {
				if (((CountingCollector) outputCollector).getCollector() instanceof ChainedReduceCombineDriver) {
					DataTransformation<OT> dtrans = new DataTransformation();
					ArrayList<OT> records = dtrans.materializeRecords(type, out, fct, padding, sizeOftuple, actualSize);
					for (OT record : records) {
						(((CountingCollector<OT>) outputCollector).getCollector()).collect(record);
					}
				} else {
					DataTransformation dtrans = new DataTransformation();
					if (BREAKDOWN) {
						for (int i = 0; i < out.length; i += sizeOftuple) {
							long breakRecordsStart = System.currentTimeMillis();
							byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
							long breakRecordsEnd = System.currentTimeMillis();
							((CountingCollector) outputCollector).collect(recordBytes);
							total += (breakRecordsEnd - breakRecordsStart);
						}
						FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " break data into records: " + total);
					} else {
						for (int i = 0; i < out.length; i += sizeOftuple) {
							byte[] recordBytes = dtrans.getTupleByteRecord(type, fct, padding, out, actualSize, i);
							((CountingCollector) outputCollector).collect(recordBytes);
						}
					}
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
	public void collect(IT record) {
		try {
			this.numRecordsIn.inc();
			TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
			TypeSerializer<IT> serializer = serializerFactory.getSerializer();
			boolean tornadoConfFlag = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTornadoVMFlag();
			if (accelerateWithTornadoVM || tornadoConfFlag) {
				inputData.add(record);
				fromSerialExecutionToTornado = true;
			} else {
				this.outputCollector.collect(this.mapper.map(record));
			}
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(this.taskName, ex);
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

	private void tornadoVMCleanUp() {
		for (int k = 0; k < TornadoRuntime.getTornadoRuntime().getNumDrivers(); k++) {
			final TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(k);
			for (int j = 0; j < driver.getDeviceCount(); j++) {
				driver.getDevice(j).reset();
			}
		}
	}

	@Override
	public void close() {
		if (fromSerialExecutionToTornado) {
			{
				String mapUserClassName = this.mapper.getClass().toString().replace("class ", "").replace(".", "/");
				// TODO: Change to getting byte array from configuration and loading it
				Tuple2<String, String> inOutTypes = AcceleratedFlinkUserFunction.GetTypesMap.getTypes(mapUserClassName);
				String inOwner = inOutTypes.f0;
				String outOwner = inOutTypes.f1;
				// Input data to the Map Operator
				DataTransformation dtrans = new DataTransformation();
				TypeSerializer ser = null;
				AccelerationData accData = dtrans.generateBroadcastedInputAccelerationData(inputData, ser);
				inputByteData = accData.getRawData();

				// Number of elements to be processed
				numOfElements = accData.getInputSize();
				numRecordsIn.inc(numOfElements);
				//long skelStart = System.currentTimeMillis();

				TornadoMap msk = null;
				// get asm skeleton in byte form
				byte[] def = new byte[0];
				byte[] bytesMsk = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes(def);
				AsmClassLoader loader = new AsmClassLoader();
				// either load class using classloader or retrieve loaded class
				MiddleMap md = loader.loadClass("org.apache.flink.api.asm.MapASMSkeleton", bytesMsk);
				msk = new TornadoMap(md);
				//long skelEnd = System.currentTimeMillis();
				TypeBytes typeBytes = null;
				FlinkData flinkData = null;
				byte[] outBytes = null;
				AccelerationData acres = null;
				FlinkCompilerInfo fct0 = null;
				//System.out.println("**** Function " + this.mapper + " create ASM skeleton: " + (skelEnd - skelStart));
				switch (inOwner) {
					case "java/lang/Integer": {
						int[] in = new int[numOfElements];
						/* Based on the output data type create the output array and the Task Schedule
						 * Currently, the input and output data can only be of type Integer, Double, Float or Long.
						 * Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
						 * since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
						 */
						switch (outOwner) {
							case "java/lang/Integer": {
								int[] out = new int[numOfElements];
								outBytes = new byte[numOfElements * INT_LENGTH];
								typeBytes = new TInteger();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Double": {
								double[] out = new double[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TDouble();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Float": {
								float[] out = new float[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TFloat();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Long": {
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
						}
						outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
						int numOfElementsRes = outBytes.length / typeBytes.getBytes();
						acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
						break;
					}
					case "java/lang/Double": {
						double[] in = new double[numOfElements];

						switch (outOwner) {
							case "java/lang/Integer": {
								int[] out = new int[numOfElements];
								typeBytes = new TInteger();
								outBytes = new byte[numOfElements * 4];
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Double": {
								double[] out = new double[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TDouble();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Float": {
								float[] out = new float[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TFloat();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Long": {
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
						}
						outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
						int numOfElementsRes = outBytes.length / typeBytes.getBytes();
						acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
						break;
					}
					case "java/lang/Float": {
						float[] in = new float[numOfElements];

						switch (outOwner) {
							case "java/lang/Integer": {
								int[] out = new int[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TInteger();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Double": {
								double[] out = new double[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TDouble();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Float": {
								float[] out = new float[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TFloat();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Long": {
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
						}
						outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
						int numOfElementsRes = outBytes.length / typeBytes.getBytes();
						acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
						break;
					}
					case "java/lang/Long": {
						long[] in = new long[numOfElements];

						switch (outOwner) {
							case "java/lang/Integer": {
								int[] out = new int[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TInteger();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Double": {
								double[] out = new double[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TDouble();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Float": {
								float[] out = new float[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TFloat();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
							case "java/lang/Long": {
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								typeBytes = new TLong();
								flinkData = new FlinkData(inputByteData, outBytes);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
								break;
							}
						}
						outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
						int numOfElementsRes = outBytes.length / typeBytes.getBytes();
						acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
						break;
					}
					case "org/apache/flink/api/java/tuple/Tuple2": {

						switch (outOwner) {
							case "java/lang/Integer": {
								Tuple2[] in = new Tuple2[numOfElements];
								int[] out = new int[numOfElements];
								outBytes = new byte[numOfElements * 4];
								typeBytes = new TInteger();
								flinkData = new FlinkData(inputByteData, outBytes);
								fct0 = new FlinkCompilerInfo();
								fct0.setStoreJavaKind(int.class);

								acres = examineType(fct0);
								createTaskScheduleAndRun(msk, in, out, outBytes, flinkData, fct0);

								outBytes = changeOutputEndianess4(outBytes);
								int numOfElementsRes = outBytes.length / typeBytes.getBytes();
								acres.setRawData(outBytes);
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(typeBytes.getBytes());
								break;
							}
							case "java/lang/Double": {
								Tuple2[] in = new Tuple2[numOfElements];
								double[] out = new double[numOfElements];
								outBytes = new byte[numOfElements * 8];
								fct0 = new FlinkCompilerInfo();
								fct0.setStoreJavaKind(double.class);
								acres = examineType(fct0);

								flinkData = new FlinkData(inputByteData, outBytes);
								new TaskSchedule("s0")
									.flinkCompilerData(fct0)
									.flinkInfo(flinkData)
									.task("t0", msk::map, in, out)
									.streamOut(outBytes)
									.execute();

								outBytes = changeOutputEndianess8(outBytes);

								int numOfElementsRes = outBytes.length / 8;
								acres.setRawData(outBytes);
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(8);
								break;
							}
							case "java/lang/Float": {
								Tuple2[] in = new Tuple2[numOfElements];
								float[] out = new float[numOfElements];
								outBytes = new byte[numOfElements * 4];
								FlinkData f = new FlinkData(inputByteData, outBytes);
								fct0 = new FlinkCompilerInfo();
								fct0.setStoreJavaKind(float.class);
								acres = new AccelerationData();

								for (String name : AbstractInvokable.typeInfo.keySet()) {
									if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
										examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
										break;
									}
								}


								byte[] outT = changeOutputEndianess4(outBytes);

								int numOfElementsRes = outT.length / 4;
								acres.setRawData(outT);
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(4);
								break;
							}
							case "java/lang/Long": {
								Tuple2[] in = new Tuple2[numOfElements];
								long[] out = new long[numOfElements];
								outBytes = new byte[numOfElements * 8];
								FlinkData f = new FlinkData(inputByteData, outBytes);
								fct0 = new FlinkCompilerInfo();
								fct0.setStoreJavaKind(long.class);
								acres = new AccelerationData();

								for (String name : AbstractInvokable.typeInfo.keySet()) {
									if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

								byte[] outT = changeOutputEndianess8(outBytes);

								int numOfElementsRes = outT.length / 8;
								acres.setRawData(outT);
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(8);
								break;
							}
							case "org/apache/flink/api/java/tuple/Tuple2": {
							//	long compStart = System.currentTimeMillis();

								int returnSize = 0;
								acres = new AccelerationData();

								fct0 = new FlinkCompilerInfo();
								for (String name : AbstractInvokable.typeInfo.keySet()) {
									int ref = name.indexOf("@");
									String classname = name.substring(0 , ref);
									if (classname.equals(mapper.getClass().toString().replace("class ", ""))) {
										//System.out.println("-> name: " + name + " mapper: " + mapper.getClass().toString().replace("class ", ""));
										returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
										break;
									}
								}
								//numOfElements = 1;
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
								FlinkData f = new FlinkData(inputByteData, outBytes);
								//long compEnd = System.currentTimeMillis();
								//System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
								// TODO: Change to getting byte array from configuration and loading it
								//TornadoMap2 msk2 = AcceleratedFlinkUserFunction.getTornadoMap2();
//								System.out.println("======== Hi");
								TaskSchedule ts;
								byte[] def2 = new byte[0];
								byte[] bytesMsk2 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes2(def2);

								if (bytesMsk2.length != 0) {
									AsmClassLoader loader2 = new AsmClassLoader();
									// either load class using classloader or retrieve loaded class
									MiddleMap2 md2 = loader2.loadClass2("org.apache.flink.api.asm.MapASMSkeleton2", bytesMsk2);
									TornadoMap2 msk2 = new TornadoMap2(md2);

									Tuple2[] out = new Tuple2[1];
									Tuple2[] in = new Tuple2[1];
									WorkerGrid worker = new WorkerGrid1D(numOfElements);
									GridScheduler gridScheduler = new GridScheduler("s3.t3", worker);

									ts = new TaskSchedule("s3")
										.flinkCompilerData(fct0)
										.flinkInfo(f)
										.task("t3", msk2::map, in, out)
										.streamOut(outBytes);

									worker.setGlobalWork(numOfElements, 1, 1);
								//	long start = System.currentTimeMillis();
									ts.execute(gridScheduler);
								//	long end = System.currentTimeMillis();
								//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
								} else {
									Tuple2[] in = new Tuple2[numOfElements];
									Tuple2[] out = new Tuple2[numOfElements];

									ts = new TaskSchedule("s3")
										.flinkCompilerData(fct0)
										.flinkInfo(f)
										.task("t3", msk::map, in, out)
										.streamOut(outBytes);
								//	long start = System.currentTimeMillis();
									ts.execute();
								//	long end = System.currentTimeMillis();
								//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
								}



								int numOfElementsRes; // = outBytes.length / returnSize;
								acres.setRawData(outBytes);
								//acres.setInputSize(numOfElementsRes);
								//acres.setReturnSize(returnSize);
								//acres.setRecordSize(8);
								if (fct0.getReturnArrayField()) {
									acres.hasArrayField();
									acres.setLengthOfArrayField(accData.getLengthOfArrayField());
									acres.setArrayFieldNo(accData.getArrayFieldNo());
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
								// check if tail iteration
								tailAccData.set(acres);
								//tornadoVMCleanUp();
								//	System.out.println("== Chainmap conf " + conf);
								break;
							}
							case "org/apache/flink/api/java/tuple/Tuple3": {
							//	long compStart = System.currentTimeMillis();
								fct0 = new FlinkCompilerInfo();

								int returnSize = 0;
								acres = new AccelerationData();
								try {
									byte[] typeB = new byte[0];
									typeB = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTypeInfo(typeB);
									ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
									ObjectInputStream ins = new ObjectInputStream(bObj);
									HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
									ins.close();
									bObj.close();

									for (String name : typeInf.keySet()) {
										if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
											returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
											break;
										}
									}
								} catch (Exception e) {
									System.out.println(e);
								}
							//	long compEnd = System.currentTimeMillis();
							//	System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
								// TODO: Change to getting byte array from configuration and loading it
								//TornadoMap2 msk2 = AcceleratedFlinkUserFunction.getTornadoMap2();

								outBytes = new byte[numOfElements * returnSize];

								FlinkData f = new FlinkData(inputByteData, outBytes);

								TaskSchedule ts;

								byte[] def2 = new byte[0];
								byte[] bytesMsk2 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes2(def2);

								if (bytesMsk2.length != 0) {
									AsmClassLoader loader2 = new AsmClassLoader();
									// either load class using classloader or retrieve loaded class
									MiddleMap2 md2 = loader2.loadClass2("org.apache.flink.api.asm.MapASMSkeleton2", bytesMsk2);
									TornadoMap2 msk2 = new TornadoMap2(md2);
									Tuple2[] in = new Tuple2[1];
									Tuple3[] out = new Tuple3[1];
									WorkerGrid worker = new WorkerGrid1D(numOfElements);
									GridScheduler gridScheduler = new GridScheduler("s1.t1", worker);
									try {
										ts = new TaskSchedule("s1")
											.flinkCompilerData(fct0)
											.flinkInfo(f)
											.task("t1", msk2::map, in, out)
											.streamOut(outBytes);

										worker.setGlobalWork(numOfElements, 1, 1);
									//	long start = System.currentTimeMillis();
										ts.execute(gridScheduler);
									//	long end = System.currentTimeMillis();
									//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
									} catch (Error | Exception e) {
										LOG.info("An exception " + e + " occurred during the heterogeneous execution, computation falls back on the CPU");
										int sizeOftuple = 0;
										boolean padding = false;
										for (int i = 0; i < fct0.getFieldSizes().size(); i++) {
											for (int j = i + 1; j < fct0.getFieldSizes().size(); j++) {
												if (fct0.getFieldSizes().get(i) != fct0.getFieldSizes().get(j)) {
													padding = true;
													break;
												}
											}
										}
										int actualSize = 0;
										for (int fieldSize : fct0.getFieldSizes()) {
											actualSize += fieldSize;
										}
										if (padding) {
											sizeOftuple = fct0.getFieldSizes().size()*8;
										} else {
											sizeOftuple = actualSize;
										}
										DataTransformation dtrans2 = new DataTransformation();
										ArrayList<IT> records = dtrans2.materializeInputRecords(inOwner, inputByteData, fct0, padding, sizeOftuple, actualSize);
										try {
											for (IT record : records) {
												this.outputCollector.collect(this.mapper.map(record));
											}
											return;
										} catch (Exception e2) {
											System.out.println(e2);
										}
									}
								} else {
									Tuple2[] in = new Tuple2[numOfElements];
									Tuple3[] out = new Tuple3[numOfElements];

									ts = new TaskSchedule("s1")
										.flinkCompilerData(fct0)
										.flinkInfo(f)
										.task("t1", msk::map, in, out)
										.streamOut(outBytes);
								//	long start = System.currentTimeMillis();
									ts.execute();
								//	long end = System.currentTimeMillis();
								//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
								}

								int numOfElementsRes = outBytes.length / returnSize;
								acres.setRawData(outBytes);
								acres.setInputSize(numOfElementsRes);
								acres.setReturnSize(returnSize);
								break;
							}
						}

						break;
					}
					case "org/apache/flink/api/java/tuple/Tuple3": {

						if (outOwner.equals("java/lang/Integer")) {
							Tuple3[] in = new Tuple3[numOfElements];
							int[] out = new int[numOfElements];
							fct0 = new FlinkCompilerInfo();
							fct0.setStoreJavaKind(int.class);

							outBytes = new byte[numOfElements * 4];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

							outBytes = new byte[numOfElements * 8];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

							outBytes = new byte[numOfElements * 4];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

							outBytes = new byte[numOfElements * 8];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

							byte[] outT = changeOutputEndianess8(outBytes);

							int numOfElementsRes = outT.length / 8;
							acres.setRawData(outT);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(8);
						} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
						//	long compStart = System.currentTimeMillis();
							//out[0] = new Tuple3();
							//in[0] = new Tuple3();
							fct0 = new FlinkCompilerInfo();

							int returnSize = 0;
							acres = new AccelerationData();

							try {
								byte[] typeB = new byte[0];
								typeB = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getTypeInfo(typeB);
								ByteArrayInputStream bObj = new ByteArrayInputStream(typeB);
								ObjectInputStream ins = new ObjectInputStream(bObj);
								HashMap<String, TypeInformation[]> typeInf = (HashMap<String, TypeInformation[]>) ins.readObject();
								ins.close();
								bObj.close();

								for (String name : typeInf.keySet()) {
									if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
										returnSize = examineTypeInfoForFlinkUDFs(typeInf.get(name)[0], typeInf.get(name)[1], fct0, acres);
										break;
									}
								}
							} catch (Exception e) {
								System.out.println(e);
							}
//						for (String name : AbstractInvokable.typeInfo.keySet()) {
//							if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
//								returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
//								break;
//							}
//						}
							outBytes = new byte[numOfElements * returnSize];
						//	long compEnd = System.currentTimeMillis();
						//	System.out.println("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));
							// TODO: Change to getting byte array from configuration and loading it
							//TornadoMap3 msk3 = AcceleratedFlinkUserFunction.getTornadoMap3();

							FlinkData f = new FlinkData(inputByteData, outBytes);
							TaskSchedule ts;
							byte[] def3 = new byte[0];
							byte[] bytesMsk3 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes3(def3);

							if (bytesMsk3.length != 0) {
								AsmClassLoader loader3 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap3 md3 = loader3.loadClass3("org.apache.flink.api.asm.MapASMSkeleton3", bytesMsk3);
								TornadoMap3 msk3 = new TornadoMap3(md3);

								Tuple3[] in = new Tuple3[1];
								Tuple3[] out = new Tuple3[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s3.t3", worker);

								ts = new TaskSchedule("s3")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t3", msk3::map, in, out)
									.streamOut(outBytes);

								worker.setGlobalWork(numOfElements, 1, 1);
							//	long start = System.currentTimeMillis();
								ts.execute(gridScheduler);
							//	long end = System.currentTimeMillis();
							//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));

							} else {
								Tuple3[] in = new Tuple3[numOfElements];
								Tuple3[] out = new Tuple3[numOfElements];

								ts = new TaskSchedule("s3")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t3", msk::map, in, out)
									.streamOut(outBytes);

							//	long start = System.currentTimeMillis();
								ts.execute();
							//	long end = System.currentTimeMillis();
							//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
							}

							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
						}

						break;
					}
					case "org/apache/flink/api/java/tuple/Tuple4": {

						if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
							Tuple4[] in = new Tuple4[numOfElements];
							fct0 = new FlinkCompilerInfo();

							Tuple3[] out = new Tuple3[numOfElements];
							int returnSize = 0;
							acres = new AccelerationData();

							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
									returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
									break;
								}
							}

							outBytes = new byte[numOfElements * returnSize];
							FlinkData f = new FlinkData(inputByteData, outBytes);

							new TaskSchedule("s0")
								.flinkCompilerData(fct0)
								.flinkInfo(f)
								.task("t0", msk::map, in, out)
								.streamOut(outBytes)
								.execute();

							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);
						} else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {

							//long compStart = System.currentTimeMillis();

							fct0 = new FlinkCompilerInfo();

							acres = new AccelerationData();
							int returnSize = 0;
							for (String name : AbstractInvokable.typeInfo.keySet()) {
								if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
									returnSize = examineTypeInfoForFlinkUDFs(AbstractInvokable.typeInfo.get(name)[0], AbstractInvokable.typeInfo.get(name)[1], fct0, acres);
									break;
								}
							}

							//long compEnd = System.currentTimeMillis();
							//FlinkTornadoVMLogger.printBreakDown("**** Function " + this.mapper + " set compiler info: " + (compEnd - compStart));

							// TODO: Change to getting byte array from configuration and loading it
							//TornadoMap4 msk4 = AcceleratedFlinkUserFunction.getTornadoMap4();

//							System.out.println("==== Chained Map INPUT:");
//							for (int i = 0; i < inputByteData.length; i++) {
//								System.out.print(inputByteData[i] + " ");
//							}
//							System.out.println("\n");

							outBytes = new byte[numOfElements * returnSize];
							FlinkData f = new FlinkData(inputByteData, outBytes);
							TaskSchedule ts;

							byte[] def4 = new byte[0];
							byte[] bytesMsk4 = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig().getSkeletonMapBytes4(def4);

							if (bytesMsk4.length != 0) {
								AsmClassLoader loader4 = new AsmClassLoader();
								// either load class using classloader or retrieve loaded class
								MiddleMap4 md4 = loader4.loadClass4("org.apache.flink.api.asm.MapASMSkeleton4", bytesMsk4);
								TornadoMap4 msk4 = new TornadoMap4(md4);
								Tuple4[] in = new Tuple4[1];
								Tuple4[] out = new Tuple4[1];
								WorkerGrid worker = new WorkerGrid1D(numOfElements);
								GridScheduler gridScheduler = new GridScheduler("s6.t6", worker);

								ts = new TaskSchedule("s6")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t6", msk4::map, in, out)
									.streamOut(outBytes);

								worker.setGlobalWork(numOfElements, 1, 1);

							//	long start = System.currentTimeMillis();
								ts.execute(gridScheduler);
							//	long end = System.currentTimeMillis();
							//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
							} else {
								Tuple4[] in = new Tuple4[numOfElements];
								Tuple4[] out = new Tuple4[numOfElements];

								ts = new TaskSchedule("s6")
									.flinkCompilerData(fct0)
									.flinkInfo(f)
									.task("t6", msk::map, in, out)
									.streamOut(outBytes);
							//	long start = System.currentTimeMillis();
								ts.execute();
							//	long end = System.currentTimeMillis();
							//	System.out.println("**** Function " + this.mapper + " execution time: " + (end - start));
							}

//							System.out.println("==== Chained Map Res:");
//							for (int i = 0; i < outBytes.length; i++) {
//								System.out.print(outBytes[i] + " ");
//							}
//							System.out.println("\n");

							int numOfElementsRes = outBytes.length / returnSize;
							acres.setRawData(outBytes);
							acres.setInputSize(numOfElementsRes);
							acres.setReturnSize(returnSize);

						}
						break;
					}
				}
				//tornadoVMCleanUp();
				TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
				TypeSerializer<IT> serializer = serializerFactory.getSerializer();
				if (this.getOutputCollector() instanceof ChainedAllReduceDriver) {
					((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
					ChainedAllReduceDriver chred = (ChainedAllReduceDriver) getOutputCollector();
					chred.collect(acres);
				} else if (this.getOutputCollector() instanceof ChainedMapDriver) {
					ChainedMapDriver chmad = (ChainedMapDriver) getOutputCollector();
					((CountingCollector) this.outputCollector).numRecordsOut.inc(acres.getInputSize());
					chmad.collect(acres);
				} else {
					materializeTornadoBuffers(acres, outOwner, fct0);
				}
				// collector is OutputCollector
			}
		}
		this.outputCollector.close();
	}

}
