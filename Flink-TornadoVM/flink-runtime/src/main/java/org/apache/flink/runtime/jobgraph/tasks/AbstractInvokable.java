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

package org.apache.flink.runtime.jobgraph.tasks;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Future;
import org.apache.flink.runtime.tornadovm.AccelerationData;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.util.TraceClassVisitor;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the abstract base class for every task that can be executed by a TaskManager.
 * Concrete tasks extend this class, for example the streaming and batch tasks.
 *
 * <p>The TaskManager invokes the {@link #invoke()} method when executing a
 * task. All operations of the task happen in this method (setting up input
 * output stream readers and writers as well as the task's core operation).
 *
 * <p>All classes that extend must offer a constructor {@code MyTask(Environment, TaskStateSnapshot)}.
 * Tasks that are always stateless can, for convenience, also only implement the constructor
 * {@code MyTask(Environment)}.
 *
 * <p><i>Developer note: While constructors cannot be enforced at compile time, we did not yet venture
 * on the endeavor of introducing factories (it is only an internal API after all, and with Java 8,
 * one can use {@code Class::new} almost like a factory lambda.</i>
 *
 * <p><b>NOTE:</b> There is no constructor that accepts and initial task state snapshot
 * and stores it in a variable. That is on purpose, because the AbstractInvokable itself
 * does not need the state snapshot (only subclasses such as StreamTask do need the state)
 * and we do not want to store a reference indefinitely, thus preventing cleanup of
 * the initial state structure by the Garbage Collector.
 *
 * <p>Any subclass that supports recoverable state and participates in
 * checkpointing needs to override {@link #triggerCheckpointAsync(CheckpointMetaData, CheckpointOptions, boolean)},
 * {@link #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetrics)},
 * {@link #abortCheckpointOnBarrier(long, Throwable)} and {@link #notifyCheckpointCompleteAsync(long)}.
 */
public abstract class AbstractInvokable {

	/** The environment assigned to this invokable. */
	private final Environment environment;

	/** Flag whether cancellation should interrupt the executing thread. */
	private volatile boolean shouldInterruptOnCancel = true;

	public static HashMap<String, TypeInformation[]> typeInfo; // = new HashMap<>();

	private static int tupleArrayFieldTotalBytes;

	public static boolean firstIteration = true;

	//public static AccelerationData tailAccData;
	public static AtomicReference<AccelerationData> tailAccData = new AtomicReference<>();

	public static AtomicReference<TypeInformation> broadTypeInfo = new AtomicReference<>();

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public AbstractInvokable(Environment environment) {
		this.environment = checkNotNull(environment);
	}

	public static ArrayList<byte[]> binaryData = new ArrayList<>();

	// ------------------------------------------------------------------------
	//  Core methods
	// ------------------------------------------------------------------------

	/**
	 * Starts the execution.
	 *
	 * <p>Must be overwritten by the concrete task implementation. This method
	 * is called by the task manager when the actual execution of the task
	 * starts.
	 *
	 * <p>All resources should be cleaned up when the method returns. Make sure
	 * to guard the code with <code>try-finally</code> blocks where necessary.
	 *
	 * @throws Exception
	 *         Tasks may forward their exceptions for the TaskManager to handle through failure/recovery.
	 */
	public abstract void invoke() throws Exception;

	/**
	 * This method is called when a task is canceled either as a result of a user abort or an execution failure. It can
	 * be overwritten to respond to shut down the user code properly.
	 *
	 * @throws Exception
	 *         thrown if any exception occurs during the execution of the user code
	 */
	public void cancel() throws Exception {
		// The default implementation does nothing.
	}

	/**
	 * Sets whether the thread that executes the {@link #invoke()} method should be
	 * interrupted during cancellation. This method sets the flag for both the initial
	 * interrupt, as well as for the repeated interrupt. Setting the interruption to
	 * false at some point during the cancellation procedure is a way to stop further
	 * interrupts from happening.
	 */
	public void setShouldInterruptOnCancel(boolean shouldInterruptOnCancel) {
		this.shouldInterruptOnCancel = shouldInterruptOnCancel;
	}

	/**
	 * Checks whether the task should be interrupted during cancellation.
	 * This method is check both for the initial interrupt, as well as for the
	 * repeated interrupt. Setting the interruption to false via
	 * {@link #setShouldInterruptOnCancel(boolean)} is a way to stop further interrupts
	 * from happening.
	 */
	public boolean shouldInterruptOnCancel() {
		return shouldInterruptOnCancel;
	}

	// ------------------------------------------------------------------------
	//  Access to Environment and Configuration
	// ------------------------------------------------------------------------

	/**
	 * Returns the environment of this task.
	 *
	 * @return The environment of this task.
	 */
	public final Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * Returns the user code class loader of this invokable.
	 *
	 * @return user code class loader of this invokable.
	 */
	public final ClassLoader getUserCodeClassLoader() {
		return getEnvironment().getUserClassLoader();
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 *
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {
		return this.environment.getTaskInfo().getNumberOfParallelSubtasks();
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 *
	 * @return the index of this subtask in the subtask group
	 */
	public int getIndexInSubtaskGroup() {
		return this.environment.getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Returns the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}.
	 *
	 * @return the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}
	 */
	public final Configuration getTaskConfiguration() {
		return this.environment.getTaskConfiguration();
	}

	/**
	 * Returns the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}.
	 *
	 * @return the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}
	 */
	public Configuration getJobConfiguration() {
		return this.environment.getJobConfiguration();
	}

	/**
	 * Returns the global ExecutionConfig.
	 */
	public ExecutionConfig getExecutionConfig() {
		return this.environment.getExecutionConfig();
	}

	// ------------------------------------------------------------------------
	//  Checkpointing Methods
	// ------------------------------------------------------------------------

	/**
	 * This method is called to trigger a checkpoint, asynchronously by the checkpoint
	 * coordinator.
	 *
	 * <p>This method is called for tasks that start the checkpoints by injecting the initial barriers,
	 * i.e., the source tasks. In contrast, checkpoints on downstream operators, which are the result of
	 * receiving checkpoint barriers, invoke the {@link #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetrics)}
	 * method.
	 *
	 * @param checkpointMetaData Meta data for about this checkpoint
	 * @param checkpointOptions Options for performing this checkpoint
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                          to fire any registered event-time timers
	 *
	 * @return future with value of {@code false} if the checkpoint was not carried out, {@code true} otherwise
	 */
	public Future<Boolean> triggerCheckpointAsync(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) {
		throw new UnsupportedOperationException(String.format("triggerCheckpointAsync not supported by %s", this.getClass().getName()));
	}

	/**
	 * This method is called when a checkpoint is triggered as a result of receiving checkpoint
	 * barriers on all input streams.
	 *
	 * @param checkpointMetaData Meta data for about this checkpoint
	 * @param checkpointOptions Options for performing this checkpoint
	 * @param checkpointMetrics Metrics about this checkpoint
	 *
	 * @throws Exception Exceptions thrown as the result of triggering a checkpoint are forwarded.
	 */
	public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws IOException {
		throw new UnsupportedOperationException(String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass().getName()));
	}

	/**
	 * This method performs some action asynchronously in the task thread.
	 *
	 * @param runnable the action to perform
	 * @param descriptionFormat the optional description for the command that is used for debugging and error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 */
	public <E extends Exception> void executeInTaskThread(
			ThrowingRunnable<E> runnable,
			String descriptionFormat,
			Object... descriptionArgs) throws E {
		throw new UnsupportedOperationException(
			String.format("executeInTaskThread not supported by %s", getClass().getName()));
	}

	/**
	 * Aborts a checkpoint as the result of receiving possibly some checkpoint barriers,
	 * but at least one {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker}.
	 *
	 * <p>This requires implementing tasks to forward a
	 * {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker} to their outputs.
	 *
	 * @param checkpointId The ID of the checkpoint to be aborted.
	 * @param cause The reason why the checkpoint was aborted during alignment
	 */
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws IOException {
		throw new UnsupportedOperationException(String.format("abortCheckpointOnBarrier not supported by %s", this.getClass().getName()));
	}

	/**
	 * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has received
	 * the notification from all participating tasks.
	 *
	 * @param checkpointId The ID of the checkpoint that is complete.
	 *
	 * @return future that completes when the notification has been processed by the task.
	 */
	public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
		throw new UnsupportedOperationException(String.format("notifyCheckpointCompleteAsync not supported by %s", this.getClass().getName()));
	}

	/**
	 * Invoked when a checkpoint has been aborted, i.e., when the checkpoint coordinator has received a decline message
	 * from one task and try to abort the targeted checkpoint by notification.
	 *
	 * @param checkpointId The ID of the checkpoint that is aborted.
	 *
	 * @return future that completes when the notification has been processed by the task.
	 */
	public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
		throw new UnsupportedOperationException(String.format("notifyCheckpointAbortAsync not supported by %s", this.getClass().getName()));
	}

	public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException {
		throw new UnsupportedOperationException("dispatchOperatorEvent not supported by " + getClass().getName());
	}

	/**
	 * Transforms a long value to its corresponding value in bytes and writes results to array passed.
	 * @param data the long value
	 * @param b The byte array
	 * @param pos The index of the byte array to start writing the bytes
	 */
	public static void longToByte(long data, byte[] b, int pos) {
		b[pos] = (byte) ((data >> 0) & 0xff);
		b[pos + 1] = (byte) ((data >> 8) & 0xff);
		b[pos + 2] = (byte) ((data >> 16) & 0xff);
		b[pos + 3] = (byte) ((data >> 24) & 0xff);
		b[pos + 4] = (byte) ((data >> 32) & 0xff);
		b[pos + 5] = (byte) ((data >> 40) & 0xff);
		b[pos + 6] = (byte) ((data >> 48) & 0xff);
		b[pos + 7] = (byte) ((data >> 56) & 0xff);
	}

	/**
	 * Transforms an double to a long and passes it to longToByte function, which will return the bytes.
	 * @param data the double value
	 * @param b The byte array
	 * @param pos The index of the byte array to start writing the bytes
	 */
	public static void doubleToByte(double data, byte[] b, int pos) {
		longToByte(Double.doubleToRawLongBits(data), b, pos);
	}

	/*public static TornadoReduce transformReduceUDF(String name) {
		try {
			TransformUDF.redUserClassName = name.replace("class ", "").replace(".", "/");
			// examine udf
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.redUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesReduce();

			// ASM work for reduce
			ClassReader readerRed = new ClassReader("org.apache.flink.runtime.asm.reduce.ReduceASMSkeleton");
			ClassWriter writerRed = new ClassWriter(readerRed, ClassWriter.COMPUTE_MAXS);
			//TraceClassVisitor printerRed = new TraceClassVisitor(writerRed, new PrintWriter(System.out));
			// to remove debugging info, just replace the printer in class adapter call with
			// the writer
			TransformUDF.ReduceClassAdapter adapterRed = new TransformUDF.ReduceClassAdapter(writerRed);
			readerRed.accept(adapterRed, ClassReader.EXPAND_FRAMES);
			byte[] b = writerRed.toByteArray();
			AsmClassLoader loader = new AsmClassLoader();
			Class<?> clazzRed = loader.defineClass("org.apache.flink.runtime.asm.reduce.ReduceASMSkeleton", b);
			MiddleReduce mdr = (MiddleReduce) clazzRed.newInstance();
			TornadoReduce rsk = new TornadoReduce(mdr);
			return rsk;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}

	public static TornadoMap transformUDF(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			if (TransformUDF.mapUserClassName.contains("Predict")) {
				TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton3";
			} else {
				TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton";
			}
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap;
			if (TransformUDF.mapUserClassName.contains("Predict")) {
				readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton3");
			} else {
				readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton");
			}
			//ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			AsmClassLoader loader = new AsmClassLoader();
			TornadoMap msk;
			if (TransformUDF.mapUserClassName.contains("Predict")) {
				Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton3", b);
				MiddleMap3 md = (MiddleMap3) clazzMap.newInstance();
				msk = new TornadoMap(md);
			} else {
				Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton", b);
				MiddleMap md = (MiddleMap) clazzMap.newInstance();
				msk = new TornadoMap(md);
			}
			return msk;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}

	public static TornadoMap2 transformUDF2(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton2";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton2");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			AsmClassLoader loader = new AsmClassLoader();
			Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton2", b);
			MiddleMap2 md = (MiddleMap2) clazzMap.newInstance();
			TornadoMap2 msk = new TornadoMap2(md);
			return msk;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}


	public static TornadoMap3 transformUDF3(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton3";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton3");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			AsmClassLoader loader = new AsmClassLoader();
			Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton3", b);
			MiddleMap3 md = (MiddleMap3) clazzMap.newInstance();
			TornadoMap3 msk = new TornadoMap3(md);
			return msk;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}

	public static TornadoMap4 transformUDF4(String name) {
		try {
			TransformUDF.mapUserClassName = name.replace("class ", "").replace(".", "/");
			TransformUDF.tornadoMapName = "org/apache/flink/runtime/asm/map/MapASMSkeleton4";
			ExamineUDF.FlinkClassVisitor flinkVisit = new ExamineUDF.FlinkClassVisitor();
			ClassReader flinkClassReader = new ClassReader(TransformUDF.mapUserClassName);
			flinkClassReader.accept(flinkVisit, 0);

			setTypeVariablesMap();

			// ASM work for map
			// patch udf into the appropriate MapASMSkeleton
			String desc = "L" + TransformUDF.mapUserClassName + ";";
			ClassReader readerMap = new ClassReader("org.apache.flink.runtime.asm.map.MapASMSkeleton4");
			ClassWriter writerMap = new ClassWriter(readerMap, ClassWriter.COMPUTE_MAXS);
			writerMap.visitField(Opcodes.ACC_PUBLIC, "udf", desc, null, null).visitEnd();
			//TraceClassVisitor printer = new TraceClassVisitor(writerMap, new PrintWriter(System.out));
			TransformUDF.MapClassAdapter adapterMap = new TransformUDF.MapClassAdapter(writerMap);
			readerMap.accept(adapterMap, ClassReader.EXPAND_FRAMES);
			// tornado
			byte[] b = writerMap.toByteArray();
			AsmClassLoader loader = new AsmClassLoader();
			Class<?> clazzMap = loader.defineClass("org.apache.flink.runtime.asm.map.MapASMSkeleton4", b);
			MiddleMap4 md = (MiddleMap4) clazzMap.newInstance();
			TornadoMap4 msk = new TornadoMap4(md);
			return msk;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	} */

	public static void setTypeVariablesForSecondInput(TypeInformation inputType, FlinkCompilerInfo flinkCompilerInfo) {
		if (inputType.getClass() == TupleTypeInfo.class) {
			TupleTypeInfo tinfo = (TupleTypeInfo) inputType;
			TypeInformation[] tuparray = tinfo.getTypeArray();
			// examine if fields are tuples, if they are not
			ArrayList<Class> tupleFieldKindSecondDataSet = new ArrayList<>();
			ArrayList<Integer> fieldSizesInner = new ArrayList();
			ArrayList<String> fieldTypesInner = new ArrayList<>();

			int tupleSize = tuparray.length;

			for (int i = 0; i < tuparray.length; i++) {
				Class typeClass = tuparray[i].getTypeClass();
				if (typeClass.toString().contains("Double")) {
					tupleFieldKindSecondDataSet.add(double.class);
					fieldSizesInner.add(8);
					fieldTypesInner.add("double");
				} else if (tuparray[i] instanceof TupleTypeInfo) {
					TupleTypeInfo nestedTuple = (TupleTypeInfo) tuparray[i];
					TypeInformation[] nestedtuparray = nestedTuple.getTypeArray();
					tupleSize += nestedtuparray.length;
					for (int j = 0; j < nestedtuparray.length; j++) {
						Class nestedTypeClass = nestedtuparray[i].getTypeClass();
						if (nestedTypeClass.toString().contains("Double")) {
							tupleFieldKindSecondDataSet.add(double.class);
							fieldSizesInner.add(8);
							fieldTypesInner.add("double");
						} else if (nestedtuparray[i] instanceof TupleTypeInfo) {
							System.out.println("We can currently support only 2 factor nesting for Tuples!");
							return;
						} else if (nestedTypeClass.toString().contains("Float")) {
							tupleFieldKindSecondDataSet.add(float.class);
							fieldSizesInner.add(4);
							fieldTypesInner.add("float");
						} else if (nestedTypeClass.toString().contains("Long")) {
							tupleFieldKindSecondDataSet.add(long.class);
							fieldSizesInner.add(8);
							fieldTypesInner.add("long");
						} else if (nestedTypeClass.toString().contains("Integer")) {
							tupleFieldKindSecondDataSet.add(int.class);
							fieldSizesInner.add(4);
							fieldTypesInner.add("int");
						}
					}
				} else if (typeClass.toString().contains("Float")) {
					tupleFieldKindSecondDataSet.add(float.class);
					fieldSizesInner.add(4);
					fieldTypesInner.add("float");
				} else if (typeClass.toString().contains("Long")) {
					tupleFieldKindSecondDataSet.add(long.class);
					fieldSizesInner.add(8);
					fieldTypesInner.add("long");
				} else if (typeClass.toString().contains("Integer")) {
					tupleFieldKindSecondDataSet.add(int.class);
					fieldSizesInner.add(4);
					fieldTypesInner.add("int");
				} else if (typeClass.toString().contains("[D")) {
					tupleFieldKindSecondDataSet.add(double.class);
					fieldSizesInner.add(8);
					fieldTypesInner.add("double");
					flinkCompilerInfo.setArrayType("double");
					flinkCompilerInfo.setBroadcastedArrayField(true);
					flinkCompilerInfo.setBroadcastedTupleArrayFieldNo(i);
				} else if (typeClass.toString().contains("[F")) {
					tupleFieldKindSecondDataSet.add(float.class);
					fieldSizesInner.add(4);
					flinkCompilerInfo.setArrayType("float");
					fieldTypesInner.add("float");
					flinkCompilerInfo.setBroadcastedArrayField(true);
					flinkCompilerInfo.setBroadcastedTupleArrayFieldNo(i);
				}

			}
			int tupleSizeSecondDataSet = tupleSize;
			boolean differentTypesInner = false;

			for (int i = 0; i < tupleSize; i++) {
				for (int j = i + 1; j < tupleSize; j++) {
					if (!(fieldSizesInner.get(i).equals(fieldSizesInner.get(j)))) {
						differentTypesInner = true;
					}
				}
			}
			flinkCompilerInfo.setTupleFieldKindSecondDataSet(tupleFieldKindSecondDataSet);
			flinkCompilerInfo.setFieldSizesInner(fieldSizesInner);
			flinkCompilerInfo.setFieldTypesInner(fieldTypesInner);
			flinkCompilerInfo.setTupleSizeSecondDataSet(tupleSizeSecondDataSet);
			flinkCompilerInfo.setDifferentTypesInner(differentTypesInner);
		} else {
			// examine if fields are tuples, if they are not
			ArrayList<Class> tupleFieldKindSecondDataSet = new ArrayList<>();
			ArrayList<Integer> fieldSizesInner = new ArrayList();
			ArrayList<String> fieldTypesInner = new ArrayList<>();


			Class typeClass = inputType.getTypeClass();
			if (typeClass.toString().contains("Double")) {
				tupleFieldKindSecondDataSet.add(double.class);
				fieldSizesInner.add(8);
				fieldTypesInner.add("double");
			} else if (typeClass.toString().contains("Float")) {
					tupleFieldKindSecondDataSet.add(float.class);
					fieldSizesInner.add(4);
					fieldTypesInner.add("float");
			} else if (typeClass.toString().contains("Long")) {
				tupleFieldKindSecondDataSet.add(long.class);
				fieldSizesInner.add(8);
				fieldTypesInner.add("long");
			} else if (typeClass.toString().contains("Integer")) {
				tupleFieldKindSecondDataSet.add(int.class);
				fieldSizesInner.add(4);
				fieldTypesInner.add("int");
			}

			int tupleSizeSecondDataSet = 1;
			boolean differentTypesInner = false;

			flinkCompilerInfo.setTupleFieldKindSecondDataSet(tupleFieldKindSecondDataSet);
			flinkCompilerInfo.setFieldSizesInner(fieldSizesInner);
			flinkCompilerInfo.setFieldTypesInner(fieldTypesInner);
			flinkCompilerInfo.setTupleSizeSecondDataSet(tupleSizeSecondDataSet);
			flinkCompilerInfo.setDifferentTypesInner(differentTypesInner);
		}
	}

	public static int examineTypeInfoForFlinkUDFs(TypeInformation inputType, TypeInformation returnType, FlinkCompilerInfo flinkCompilerInfo, AccelerationData acdata) {
		if (inputType.getClass() == TupleTypeInfo.class) {
			TupleTypeInfo tinfo = (TupleTypeInfo) inputType;
			TypeInformation[] tuparray = tinfo.getTypeArray();

			ArrayList<Class> tupleFieldKind = new ArrayList<>();
			ArrayList<String> fieldTypes = new ArrayList<>();
			ArrayList<Integer> fieldSizes = new ArrayList<>();

			int tupleSize = tuparray.length;
			boolean hasTuples = true;
			boolean arrayfield = false;
			int arrayFieldPos = -1;
			int arrayFieldSize = 0;

			for (int i = 0; i < tuparray.length; i++) {
				Class typeClass = tuparray[i].getTypeClass();
				if (typeClass.toString().contains("Double")) {
					tupleFieldKind.add(double.class);
					fieldSizes.add(8);
					fieldTypes.add("double");
				} else if (tuparray[i] instanceof TupleTypeInfo) {
					TupleTypeInfo nestedTuple = (TupleTypeInfo) tuparray[i];
					TypeInformation[] nestedtuparray = nestedTuple.getTypeArray();
					tupleSize += (nestedtuparray.length - 1);
					flinkCompilerInfo.setNestedTuples(true);
					flinkCompilerInfo.setNestedTupleField(i);
					flinkCompilerInfo.setSizeOfNestedTuple(nestedtuparray.length);
					for (int j = 0; j < nestedtuparray.length; j++) {
						Class nestedTypeClass = nestedtuparray[j].getTypeClass();
						if (nestedTypeClass.toString().contains("Double")) {
							tupleFieldKind.add(double.class);
							fieldSizes.add(8);
							fieldTypes.add("double");
						} else if (nestedtuparray[i] instanceof TupleTypeInfo) {
							System.out.println("We can currently support only 2 factor nesting for Tuples!");
							return -1;
						} else if (nestedTypeClass.toString().contains("Float")) {
							tupleFieldKind.add(float.class);
							fieldSizes.add(4);
							fieldTypes.add("float");
						} else if (nestedTypeClass.toString().contains("Long")) {
							tupleFieldKind.add(long.class);
							fieldSizes.add(8);
							fieldTypes.add("long");
						} else if (nestedTypeClass.toString().contains("Integer")) {
							tupleFieldKind.add(int.class);
							fieldSizes.add(4);
							fieldTypes.add("int");
						} else if (nestedTypeClass.toString().contains("[I")) {
							tupleFieldKind.add(int.class);
							fieldSizes.add(4);
							fieldTypes.add("int");
							arrayfield = true;
							// TODO: CHECK THIS
							arrayFieldPos = i + j;
							arrayFieldSize = 4;
							flinkCompilerInfo.setArrayField(true);
							flinkCompilerInfo.setArrayType("int");
							flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
							flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
						} else if (nestedTypeClass.toString().contains("[D")) {
							tupleFieldKind.add(double.class);
							fieldSizes.add(8);
							fieldTypes.add("double");
							arrayfield = true;
							arrayFieldPos = i + j;
							arrayFieldSize = 8;
							flinkCompilerInfo.setArrayType("double");
							flinkCompilerInfo.setArrayField(true);
							flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
							flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
						} else if (nestedTypeClass.toString().contains("[F")) {
							tupleFieldKind.add(float.class);
							fieldSizes.add(4);
							fieldTypes.add("float");
							arrayfield = true;
							arrayFieldPos = i + j;
							arrayFieldSize = 4;
							flinkCompilerInfo.setArrayField(true);
							flinkCompilerInfo.setArrayType("float");
							flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
							flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
						} else if (nestedTypeClass.toString().contains("[J")) {
							tupleFieldKind.add(long.class);
							fieldSizes.add(8);
							fieldTypes.add("long");
							arrayfield = true;
							arrayFieldPos = i + j;
							arrayFieldSize = 8;
							flinkCompilerInfo.setArrayField(true);
							flinkCompilerInfo.setArrayType("long");
							flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
							flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
						}
					}
				} else if (typeClass.toString().contains("Float")) {
					tupleFieldKind.add(float.class);
					fieldSizes.add(4);
					fieldTypes.add("float");
				} else if (typeClass.toString().contains("Long")) {
					tupleFieldKind.add(long.class);
					fieldSizes.add(8);
					fieldTypes.add("long");
				} else if (typeClass.toString().contains("Integer")) {
					tupleFieldKind.add(int.class);
					fieldSizes.add(4);
					fieldTypes.add("int");
				} else if (typeClass.toString().contains("[I")) {
					tupleFieldKind.add(int.class);
					fieldSizes.add(4);
					fieldTypes.add("int");
					arrayfield = true;
					arrayFieldPos = i;
					arrayFieldSize = 4;
					flinkCompilerInfo.setArrayField(true);
					flinkCompilerInfo.setArrayType("int");
					flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
				} else if (typeClass.toString().contains("[D")) {
					tupleFieldKind.add(double.class);
					fieldSizes.add(8);
					fieldTypes.add("double");
					arrayfield = true;
					arrayFieldPos = i;
					arrayFieldSize = 8;
					flinkCompilerInfo.setArrayType("double");
					flinkCompilerInfo.setArrayField(true);
					flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
				} else if (typeClass.toString().contains("[F")) {
					tupleFieldKind.add(float.class);
					fieldSizes.add(4);
					fieldTypes.add("float");
					arrayfield = true;
					arrayFieldPos = i;
					arrayFieldSize = 4;
					flinkCompilerInfo.setArrayField(true);
					flinkCompilerInfo.setArrayType("float");
					flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
				} else if (typeClass.toString().contains("[J")) {
					tupleFieldKind.add(long.class);
					fieldSizes.add(8);
					fieldTypes.add("long");
					arrayfield = true;
					arrayFieldPos = i;
					arrayFieldSize = 8;
					flinkCompilerInfo.setArrayField(true);
					flinkCompilerInfo.setArrayType("long");
					flinkCompilerInfo.setArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setTupleArrayFieldNo(arrayFieldPos);
				}

			}

			flinkCompilerInfo.setTupleSize(tupleSize);
			boolean differentTypes = false;

			for (int i = 0; i < tupleSize; i++) {
				for (int j = i + 1; j < tupleSize; j++) {
					if (arrayfield) {
						if (i == arrayFieldPos) {
							if (!fieldSizes.get(j).equals(arrayFieldSize)) {
								differentTypes = true;
								break;
							}
						} else if (j == arrayFieldPos) {
							if (!fieldSizes.get(i).equals(arrayFieldSize)) {
								differentTypes = true;
								break;
							}
						} else {
							if (!(fieldSizes.get(i).equals(fieldSizes.get(j)))) {
								differentTypes = true;
							}
						}
					} else {
						if (!(fieldSizes.get(i).equals(fieldSizes.get(j)))) {
							differentTypes = true;
						}
					}
				}
			}

			int arraySize = 0;
			if (arrayfield) {
				if (differentTypes) {
					arraySize = tupleArrayFieldTotalBytes / 8;
				} else {
					arraySize = tupleArrayFieldTotalBytes / arrayFieldSize;
				}
			}
			flinkCompilerInfo.setHasTuples(hasTuples);
			flinkCompilerInfo.setTupleFieldKind(tupleFieldKind);
			flinkCompilerInfo.setFieldTypes(fieldTypes);
			flinkCompilerInfo.setFieldSizes(fieldSizes);
			flinkCompilerInfo.setDifferentTypes(differentTypes);
			flinkCompilerInfo.setArraySize(arraySize);
		}
		int returnSize = 0;
		if (returnType.getClass() == TupleTypeInfo.class) {
			int numOfFields = 0;
			TupleTypeInfo tinfo = (TupleTypeInfo) returnType;
			TypeInformation[] tuparray = tinfo.getTypeArray();
			flinkCompilerInfo.setReturnTuple(true);

			ArrayList<Class> returnFieldKind = new ArrayList<>();
			ArrayList<String> fieldTypesRet = new ArrayList<>();
			ArrayList<Integer> fieldSizesRet = new ArrayList<>();

			int retTupleSize = tuparray.length;
			ArrayList<Integer> tupleReturnSizes = new ArrayList<>();


			for (int i = 0; i < tuparray.length; i++) {
				Class typeClass = tuparray[i].getTypeClass();
				if (typeClass.toString().contains("Double")) {
					returnFieldKind.add(double.class);
					returnSize += 8;
					numOfFields++;
					fieldSizesRet.add(8);
					tupleReturnSizes.add(8);
					fieldTypesRet.add("double");
				} else if (tuparray[i] instanceof TupleTypeInfo) {
					//flinkCompilerInfo.setReturnNestedTuple();
					TupleTypeInfo nestedTuple = (TupleTypeInfo) tuparray[i];
					TypeInformation[] nestedtuparray = nestedTuple.getTypeArray();
					retTupleSize += (nestedtuparray.length - 1);
					for (int j = 0; j < nestedtuparray.length; j++) {
						Class nestedTypeClass = nestedtuparray[j].getTypeClass();
						if (nestedTypeClass.toString().contains("Double")) {
							returnFieldKind.add(double.class);
							returnSize += 8;
							numOfFields++;
							fieldSizesRet.add(8);
							fieldTypesRet.add("double");
							tupleReturnSizes.add(8);
						} else if (nestedtuparray[i] instanceof TupleTypeInfo) {
							System.out.println("We can currently support only 2 factor nesting for Tuples!");
							return -1;
						} else if (nestedTypeClass.toString().contains("Float")) {
							returnFieldKind.add(float.class);
							returnSize += 4;
							numOfFields++;
							fieldSizesRet.add(4);
							fieldTypesRet.add("float");
							tupleReturnSizes.add(4);
						} else if (nestedTypeClass.toString().contains("Long")) {
							returnFieldKind.add(long.class);
							returnSize += 8;
							numOfFields++;
							fieldSizesRet.add(8);
							fieldTypesRet.add("long");
							tupleReturnSizes.add(8);
						} else if (nestedTypeClass.toString().contains("Integer")) {
							returnFieldKind.add(int.class);
							returnSize += 4;
							numOfFields++;
							fieldSizesRet.add(4);
							fieldTypesRet.add("int");
							tupleReturnSizes.add(4);

						} else if (nestedTypeClass.toString().contains("[D")) {
							returnFieldKind.add(double.class);
							returnSize += 8*83;
							numOfFields++;
							fieldSizesRet.add(8);
							fieldTypesRet.add("double");
							tupleReturnSizes.add(8);
							flinkCompilerInfo.setArrayType("double");
							flinkCompilerInfo.setReturnArrayField(true);
							flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
							//flinkCompilerInfo.setTupleArrayFieldNo(i + j);
						} else if (nestedTypeClass.toString().contains("[F")) {
							returnFieldKind.add(float.class);
							returnSize += 4 * 83;
							numOfFields++;
							fieldSizesRet.add(4);
							fieldTypesRet.add("float");
							flinkCompilerInfo.setArrayType("float");
							tupleReturnSizes.add(4);
							flinkCompilerInfo.setReturnArrayField(true);
							flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
						}
					}
				} else if (typeClass.toString().contains("Float")) {
					returnFieldKind.add(float.class);
					returnSize += 4;
					numOfFields++;
					fieldSizesRet.add(4);
					fieldTypesRet.add("float");
					tupleReturnSizes.add(4);
				} else if (typeClass.toString().contains("Long")) {
					returnFieldKind.add(long.class);
					returnSize += 8;
					numOfFields++;
					fieldSizesRet.add(8);
					fieldTypesRet.add("long");
					tupleReturnSizes.add(8);
				} else if (typeClass.toString().contains("Integer")) {
					returnFieldKind.add(int.class);
					returnSize += 4;
					numOfFields++;
					fieldSizesRet.add(4);
					fieldTypesRet.add("int");
					tupleReturnSizes.add(4);
				} else if (typeClass.toString().contains("[D")) {
					returnFieldKind.add(double.class);
					flinkCompilerInfo.setReturnArrayField(true);
					flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setArrayType("double");
					returnSize += tupleArrayFieldTotalBytes;
					numOfFields++;
					fieldSizesRet.add(tupleArrayFieldTotalBytes);
					fieldTypesRet.add("double");
					tupleReturnSizes.add(8);
					acdata.hasArrayField();
				} else if (typeClass.toString().contains("[F")) {
					returnFieldKind.add(float.class);
					flinkCompilerInfo.setReturnArrayField(true);
					flinkCompilerInfo.setReturnArrayFieldTotalBytes(tupleArrayFieldTotalBytes);
					flinkCompilerInfo.setArrayType("float");
					returnSize += tupleArrayFieldTotalBytes;
					numOfFields++;
					fieldSizesRet.add(tupleArrayFieldTotalBytes);
					fieldTypesRet.add("float");
					tupleReturnSizes.add(4);
					acdata.hasArrayField();
				}
			}

			flinkCompilerInfo.setReturnTupleSize(retTupleSize);
			boolean differentTypesRet = false;

			if (acdata.getArrayField()) {
				for (int i = 0; i < retTupleSize; i++) {
					for (int j = i + 1; j < retTupleSize; j++) {
						if (!(tupleReturnSizes.get(i).equals(tupleReturnSizes.get(j)))) {
							differentTypesRet = true;
							returnSize = numOfFields * 8;
						}
					}
				}
			} else {
				for (int i = 0; i < retTupleSize; i++) {
					for (int j = i + 1; j < retTupleSize; j++) {
						if (!(fieldSizesRet.get(i).equals(fieldSizesRet.get(j)))) {
							differentTypesRet = true;
							returnSize = numOfFields * 8;
						}
					}
				}
			}
			ArrayList fieldTypeRetCopy = new ArrayList();
			for (String fieldType : fieldTypesRet) {
				fieldTypeRetCopy.add(fieldType);
			}

			flinkCompilerInfo.setReturnFieldKind(returnFieldKind);
			flinkCompilerInfo.setFieldTypesRet(fieldTypesRet);
			flinkCompilerInfo.setFieldSizesRet(fieldSizesRet);
			flinkCompilerInfo.setDifferentTypesRet(differentTypesRet);
			flinkCompilerInfo.setFieldTypesRetCopy(fieldTypeRetCopy);
			acdata.setDifferentReturnTupleFields(differentTypesRet);
			acdata.setReturnFieldSizes(tupleReturnSizes);
		}
		return returnSize;
	}

	public static void setTupleArrayFieldTotalBytes(int arrayFieldTotalBytes) {
		tupleArrayFieldTotalBytes = arrayFieldTotalBytes;
	}

}
