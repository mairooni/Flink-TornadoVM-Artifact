/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
 * The University of Manchester. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Authors: James Clarkson
 *
 */
package uk.ac.manchester.tornado.runtime;

import static uk.ac.manchester.tornado.api.enums.TornadoExecutionStatus.COMPLETE;
import static uk.ac.manchester.tornado.runtime.common.Tornado.ENABLE_PROFILING;
import static uk.ac.manchester.tornado.runtime.common.Tornado.USE_VM_FLUSH;
import static uk.ac.manchester.tornado.runtime.common.Tornado.VM_USE_DEPS;
import static uk.ac.manchester.tornado.runtime.common.TornadoOptions.VIRTUAL_DEVICE_ENABLED;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.Event;
import uk.ac.manchester.tornado.api.common.SchedulableTask;
import uk.ac.manchester.tornado.api.common.TornadoEvents;
import uk.ac.manchester.tornado.api.exceptions.TornadoBailoutRuntimeException;
import uk.ac.manchester.tornado.api.exceptions.TornadoDeviceFP64NotSupported;
import uk.ac.manchester.tornado.api.exceptions.TornadoFailureException;
import uk.ac.manchester.tornado.api.exceptions.TornadoInternalError;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.mm.ObjectBuffer;
import uk.ac.manchester.tornado.api.profiler.ProfilerType;
import uk.ac.manchester.tornado.api.profiler.TornadoProfiler;
import uk.ac.manchester.tornado.runtime.common.CallStack;
import uk.ac.manchester.tornado.runtime.common.DeviceObjectState;
import uk.ac.manchester.tornado.runtime.common.Tornado;
import uk.ac.manchester.tornado.runtime.common.TornadoAcceleratorDevice;
import uk.ac.manchester.tornado.runtime.common.TornadoInstalledCode;
import uk.ac.manchester.tornado.runtime.common.TornadoLogger;
import uk.ac.manchester.tornado.runtime.common.TornadoOptions;
import uk.ac.manchester.tornado.runtime.graph.TornadoExecutionContext;
import uk.ac.manchester.tornado.runtime.graph.TornadoGraphAssembler.TornadoVMBytecodes;
import uk.ac.manchester.tornado.runtime.profiler.TimeProfiler;
import uk.ac.manchester.tornado.runtime.tasks.GlobalObjectState;
import uk.ac.manchester.tornado.runtime.tasks.PrebuiltTask;
import uk.ac.manchester.tornado.runtime.tasks.TornadoTaskSchedule;
import uk.ac.manchester.tornado.runtime.tasks.meta.TaskMetaData;

/**
 * TornadoVM: it includes a bytecode interpreter (Tornado bytecodes), a memory
 * manager for all devices (FPGAs, GPUs and multi-core that follows the OpenCL
 * programming model), and a JIT compiler from Java bytecode to OpenCL.
 * <p>
 * The JIT compiler extends the Graal JIT Compiler for OpenCL compilation.
 * <p>
 * There is an instance of the {@link TornadoVM} per
 * {@link TornadoTaskSchedule}. Each TornadoVM contains the logic to orchestrate
 * the execution on the parallel device (e.g., a GPU).
 */
public class TornadoVM extends TornadoLogger {

    private static final Event EMPTY_EVENT = new EmptyEvent();

    private static final int MAX_EVENTS = 32;
    private final boolean useDependencies;

    private final TornadoExecutionContext graphContext;
    private final List<Object> objects;

    private ConcurrentHashMap<Object, Integer> mappingAtomics;

    private final GlobalObjectState[] globalStates;
    private final CallStack[] stacks;
    private final int[][] events;
    private final int[] eventsIndexes;
    private final List<TornadoAcceleratorDevice> contexts;
    private final TornadoInstalledCode[] installedCodes;

    private final List<Object> constants;
    private final List<SchedulableTask> tasks;

    private final ByteBuffer buffer;

    private double totalTime;
    private long invocations;
    private TornadoProfiler timeProfiler;
    private boolean finishedWarmup;
    private boolean doUpdate;

    private GridScheduler gridScheduler;

    public TornadoVM(TornadoExecutionContext graphContext, byte[] code, int limit, TornadoProfiler timeProfiler) {

        this.graphContext = graphContext;
        this.timeProfiler = timeProfiler;

        useDependencies = graphContext.meta().enableOooExecution() | VM_USE_DEPS;
        totalTime = 0;
        invocations = 0;

        buffer = ByteBuffer.wrap(code);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.limit(limit);

        debug("loading tornado vm...");

        TornadoInternalError.guarantee(buffer.get() == TornadoVMBytecodes.SETUP.value(), "invalid code");

        contexts = graphContext.getDevices();
        buffer.getInt();
        int taskCount = buffer.getInt();
        stacks = graphContext.getFrames();
        events = new int[buffer.getInt()][MAX_EVENTS];
        eventsIndexes = new int[events.length];

        // System.out.println("- initialize installedCodes in constructor");
        installedCodes = new TornadoInstalledCode[taskCount];

        for (int i = 0; i < events.length; i++) {
            Arrays.fill(events[i], -1);
            eventsIndexes[i] = 0;
        }

        debug("found %d contexts", contexts.size());
        debug("created %d stacks", stacks.length);
        debug("created %d event lists", events.length);

        objects = graphContext.getObjects();
        globalStates = new GlobalObjectState[objects.size()];
        fetchGlobalStates();

        byte op = buffer.get();
        while (op != TornadoVMBytecodes.BEGIN.value()) {
            TornadoInternalError.guarantee(op == TornadoVMBytecodes.CONTEXT.value(), "invalid code: 0x%x", op);
            final int deviceIndex = buffer.getInt();
            debug("loading context %s", contexts.get(deviceIndex));
            final long t0 = System.nanoTime();
            if (contexts.get(deviceIndex) != null) {
                contexts.get(deviceIndex).ensureLoaded();
            }
            final long t1 = System.nanoTime();
            debug("loaded in %.9f s", (t1 - t0) * 1e-9);
            op = buffer.get();
        }

        constants = graphContext.getConstants();
        tasks = graphContext.getTasks();

        debug("%s - vm ready to go", graphContext.getId());
        buffer.mark();

        mappingAtomics = new ConcurrentHashMap<>();
    }

    public void setCompileUpdate() {
        this.doUpdate = true;
    }

    public void fetchGlobalStates() {
        debug("fetching %d object states...", globalStates.length);
        for (int i = 0; i < objects.size(); i++) {
            final Object object = objects.get(i);
            TornadoInternalError.guarantee(object != null, "null object found in TornadoVM");
            globalStates[i] = TornadoCoreRuntime.getTornadoRuntime().resolveObject(object);
            debug("\tobject[%d]: [0x%x] %s %s", i, object.hashCode(), object.getClass().getTypeName(), globalStates[i]);
        }
    }

    private GlobalObjectState resolveGlobalObjectState(int index) {
        return globalStates[index];
    }

    private DeviceObjectState resolveObjectState(int index, int device) {
        return globalStates[index].getDeviceState(contexts.get(device));
    }

    private CallStack resolveStack(int index, int numArgs, CallStack[] stacks, TornadoAcceleratorDevice device, boolean setNewDevice) {
        if (graphContext.meta().isDebug() && setNewDevice) {
            debug("Recompiling task on device " + device);
        }
        if (stacks[index] == null || setNewDevice) {
            stacks[index] = device.createStack(numArgs);
        }
        return stacks[index];
    }

    public void invalidateObjects() {
        for (GlobalObjectState globalState : globalStates) {
            globalState.invalidate();
        }
    }

    public void warmup() {
        execute(true);
        finishedWarmup = true;
    }

    public void compile() {
        execute(true);
    }

    public Event execute() {
        return execute(false);
    }

    private void initWaitEventList() {
        for (int[] waitList : events) {
            Arrays.fill(waitList, -1);
        }
    }

    public void clearInstalledCode() {
        Arrays.fill(installedCodes, null);
    }

    private int executeAllocate(StringBuilder tornadoVMBytecodeList, final int objectIndex, final int contextIndex, final long sizeBatch) {
        final Object object;
        if (graphContext.getFinfo() != null && graphContext.getFinfo().isPlainReduction()) {
            object = graphContext.getFinfo().getByteDataSets().get(objectIndex);
        } else {
            object = objects.get(objectIndex);
        }

        final TornadoAcceleratorDevice device = contexts.get(contextIndex);

        if (TornadoOptions.printBytecodes && !isObjectAtomic(object)) {
            String verbose = String.format("vm: ALLOCATE [0x%x] %s on %s, size=%d", object.hashCode(), object, device, sizeBatch);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        final DeviceObjectState objectState = resolveObjectState(objectIndex, contextIndex);
        return device.ensureAllocated(object, sizeBatch, objectState);
    }

    private boolean isObjectAtomic(Object object) {
        return object instanceof AtomicInteger;
    }

    private boolean isObjectKernelContext(Object object) {
        return object instanceof KernelContext;
    }

    private int executeCopyIn(StringBuilder tornadoVMBytecodeList, final int objectIndex, final int contextIndex, final long offset, final int eventList, final long sizeBatch, final int[] waitList) {
        final TornadoAcceleratorDevice device = contexts.get(contextIndex);

        final Object object;
        if (graphContext.getFinfo() != null && graphContext.getFinfo().isPlainReduction()) {
            object = graphContext.getFinfo().getByteDataSets().get(objectIndex);
        } else {
            object = objects.get(objectIndex);
        }

        if (isObjectKernelContext(object)) {
            return 0;
        }

        final DeviceObjectState objectState = resolveObjectState(objectIndex, contextIndex);

        if (TornadoOptions.printBytecodes & !isObjectAtomic(object)) {
            String verbose = String.format("vm: COPY_IN [Object Hash Code=0x%x] %s on %s, size=%d, offset=%d [event list=%d]", object.hashCode(), object, device, sizeBatch, offset, eventList);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        List<Integer> allEvents;
        if (sizeBatch > 0) {
            // We need to stream-in when using batches, because the
            // whole data is not copied yet.
            allEvents = device.streamIn(object, sizeBatch, offset, objectState, waitList);
        } else {
            allEvents = device.ensurePresent(object, objectState, waitList, sizeBatch, offset);
        }

        resetEventIndexes(eventList);

        if (TornadoOptions.isProfilerEnabled() && allEvents != null) {
            for (Integer e : allEvents) {
                Event event = device.resolveEvent(e);
                event.waitForEvents();
                long copyInTimer = timeProfiler.getTimer(ProfilerType.COPY_IN_TIME);
                copyInTimer += event.getElapsedTime();
                timeProfiler.setTimer(ProfilerType.COPY_IN_TIME, copyInTimer);

                timeProfiler.addValueToMetric(ProfilerType.TOTAL_COPY_IN_SIZE_BYTES, TimeProfiler.NO_TASK_NAME, objectState.getBuffer().size());

                long dispatchValue = timeProfiler.getTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME);
                dispatchValue += event.getDriverDispatchTime();
                timeProfiler.setTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME, dispatchValue);
            }
        }
        return 0;
    }

    private int executeStreamIn(StringBuilder tornadoVMBytecodeList, final int objectIndex, final int contextIndex, final long offset, final int eventList, final long sizeBatch,
            final int[] waitList) {
        final TornadoAcceleratorDevice device = contexts.get(contextIndex);
        final Object object;
        if (graphContext.getFinfo() != null && graphContext.getFinfo().isPlainReduction()) {
            object = graphContext.getFinfo().getByteDataSets().get(objectIndex);
        } else {
            object = objects.get(objectIndex);
        }

        if (isObjectKernelContext(object)) {
            return 0;
        }

        if (TornadoOptions.printBytecodes && !isObjectAtomic(object)) {
            String verbose = String.format("vm: STREAM_IN [0x%x] %s on %s, size=%d, offset=%d [event list=%d]", object.hashCode(), object, device, sizeBatch, offset, eventList);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        final DeviceObjectState objectState = resolveObjectState(objectIndex, contextIndex);
        List<Integer> allEvents = device.streamIn(object, sizeBatch, offset, objectState, waitList);

        resetEventIndexes(eventList);

        if (TornadoOptions.isProfilerEnabled() && allEvents != null) {
            for (Integer e : allEvents) {
                Event event = device.resolveEvent(e);
                event.waitForEvents();
                long copyInTimer = timeProfiler.getTimer(ProfilerType.COPY_IN_TIME);
                copyInTimer += event.getElapsedTime();
                timeProfiler.setTimer(ProfilerType.COPY_IN_TIME, copyInTimer);

                timeProfiler.addValueToMetric(ProfilerType.TOTAL_COPY_IN_SIZE_BYTES, TimeProfiler.NO_TASK_NAME, objectState.getBuffer().size());

                long dispatchValue = timeProfiler.getTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME);
                dispatchValue += event.getDriverDispatchTime();
                timeProfiler.setTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME, dispatchValue);
            }
        }
        return 0;
    }

    private int executeStreamOut(StringBuilder tornadoVMBytecodeList, final int objectIndex, final int contextIndex, final long offset, final int eventList, final long sizeBatch,
            final int[] waitList) {
        final TornadoAcceleratorDevice device = contexts.get(contextIndex);
        final Object object;
        if (graphContext.getFinfo() != null && graphContext.getFinfo().isPlainReduction()) {
            object = graphContext.getFinfo().getByteDataSets().get(objectIndex);
        } else {
            object = objects.get(objectIndex);
        }

        if (isObjectKernelContext(object)) {
            return 0;
        }

        if (TornadoOptions.printBytecodes) {
            String verbose = String.format("vm: STREAM_OUT [0x%x] %s on %s, size=%d, offset=%d [event list=%d]", object.hashCode(), object, device, sizeBatch, offset, eventList);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        final DeviceObjectState objectState = resolveObjectState(objectIndex, contextIndex);

        int lastEvent = device.streamOutBlocking(object, offset, objectState, waitList);

        resetEventIndexes(eventList);

        if (TornadoOptions.isProfilerEnabled() && lastEvent != -1) {
            Event event = device.resolveEvent(lastEvent);
            event.waitForEvents();
            long value = timeProfiler.getTimer(ProfilerType.COPY_OUT_TIME);
            value += event.getElapsedTime();
            timeProfiler.setTimer(ProfilerType.COPY_OUT_TIME, value);

            timeProfiler.addValueToMetric(ProfilerType.TOTAL_COPY_OUT_SIZE_BYTES, TimeProfiler.NO_TASK_NAME, objectState.getBuffer().size());

            long dispatchValue = timeProfiler.getTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME);
            dispatchValue += event.getDriverDispatchTime();
            timeProfiler.setTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME, dispatchValue);
        }
        return lastEvent;
    }

    private void executeStreamOutBlocking(StringBuilder tornadoVMBytecodeList, final int objectIndex, final int contextIndex, final long offset, final int eventList, final long sizeBatch,
            final int[] waitList) {

        final TornadoAcceleratorDevice device = contexts.get(contextIndex);
        final Object object;
        if (graphContext.getFinfo() != null && graphContext.getFinfo().isPlainReduction()) {
            object = graphContext.getFinfo().getByteDataSets().get(objectIndex);
        } else {
            object = objects.get(objectIndex);
        }

        if (isObjectKernelContext(object)) {
            return;
        }

        if (TornadoOptions.printBytecodes) {
            String verbose = String.format("vm: STREAM_OUT_BLOCKING [0x%x] %s on %s, size=%d, offset=%d [event list=%d]", object.hashCode(), object, device, sizeBatch, offset, eventList);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        final DeviceObjectState objectState = resolveObjectState(objectIndex, contextIndex);

        final int tornadoEventID = device.streamOutBlocking(object, offset, objectState, waitList);

        if (TornadoOptions.isProfilerEnabled() && tornadoEventID != -1) {
            Event event = device.resolveEvent(tornadoEventID);
            event.waitForEvents();
            long value = timeProfiler.getTimer(ProfilerType.COPY_OUT_TIME);
            value += event.getElapsedTime();
            timeProfiler.setTimer(ProfilerType.COPY_OUT_TIME, value);

            timeProfiler.addValueToMetric(ProfilerType.TOTAL_COPY_OUT_SIZE_BYTES, TimeProfiler.NO_TASK_NAME, objectState.getBuffer().size());

            long dispatchValue = timeProfiler.getTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME);
            dispatchValue += event.getDriverDispatchTime();
            timeProfiler.setTimer(ProfilerType.TOTAL_DISPATCH_DATA_TRANSFERS_TIME, dispatchValue);
        }
        resetEventIndexes(eventList);
    }

    private static class ExecutionInfo {
        CallStack stack;
        int[] waitList;

        public ExecutionInfo(CallStack stack, int[] waitList) {
            this.stack = stack;
            this.waitList = waitList;
        }
    }

    private void profilerUpdateForPreCompiledTask(SchedulableTask task) {
        if (task instanceof PrebuiltTask && timeProfiler instanceof TimeProfiler) {
            PrebuiltTask prebuiltTask = (PrebuiltTask) task;
            timeProfiler.registerDeviceID(ProfilerType.DEVICE_ID, task.getId(), prebuiltTask.meta().getLogicDevice().getDriverIndex() + ":" + prebuiltTask.meta().getDeviceIndex());
            timeProfiler.registerDeviceName(ProfilerType.DEVICE, task.getId(), prebuiltTask.meta().getLogicDevice().getPhysicalDevice().getDeviceName());
        }
    }

    private ExecutionInfo compileTaskFromBytecodeToBinary(final int contextIndex, final int stackIndex, final int numArgs, final int eventList, final int taskIndex, final long batchThreads) {
        final TornadoAcceleratorDevice device = contexts.get(contextIndex);

        if (device.getDeviceContext().wasReset() && finishedWarmup) {
            throw new TornadoFailureException("[ERROR] reset() was called after warmup()");
        }

        boolean redeployOnDevice = graphContext.redeployOnDevice();

        final CallStack stack = resolveStack(stackIndex, numArgs, stacks, device, redeployOnDevice);

        final int[] waitList = (useDependencies && eventList != -1) ? events[eventList] : null;
        final SchedulableTask task = tasks.get(taskIndex);

        // Set the batch size in the task information
        task.setBatchThreads(batchThreads);
        task.enableDefaultThreadScheduler(graphContext.useDefaultThreadScheduler());

        if (gridScheduler != null && gridScheduler.get(task.getId()) != null) {
            task.setUseGridScheduler(true);
            task.setGridScheduler(gridScheduler);
        }

        if (shouldCompile(installedCodes[taskIndex])) {
            task.mapTo(device);
            try {
                task.attachProfiler(timeProfiler);
                if (taskIndex == (tasks.size() - 1)) {
                    // If it is the last task within the task-schedule -> we force compilation
                    // This is useful when compiling code for Xilinx/Altera FPGAs, that has to
                    // be a single source
                    task.forceCompilation();
                }
                if (doUpdate) {
                    task.forceCompilation();
                }
                installedCodes[taskIndex] = device.installCode(task);
                profilerUpdateForPreCompiledTask(task);
                doUpdate = false;
            } catch (TornadoBailoutRuntimeException e) {
                throw new TornadoBailoutRuntimeException("Unable to compile task " + task.getFullName() + "\n" + Arrays.toString(e.getStackTrace()), e);
            } catch (TornadoDeviceFP64NotSupported e) {
                throw e;
            }
        }
        return new ExecutionInfo(stack, waitList);
    }

    private boolean shouldCompile(TornadoInstalledCode installedCode) {
        return installedCode == null || !installedCode.isValid();
    }

    private void setObjectState(DeviceObjectState objectState, boolean flag) {
        objectState.setContents(flag);
        objectState.setModified(flag);
    }

    private boolean isObjectInAtomicRegion(DeviceObjectState objectState, TornadoAcceleratorDevice device, SchedulableTask task) {
        return objectState.isAtomicRegionPresent() && device.checkAtomicsParametersForTask(task);
    }

    private int executeLaunch(StringBuilder tornadoVMBytecodeList, final int contextIndex, final int numArgs, final int eventList, final int taskIndex, final long batchThreads, final long offset,
            ExecutionInfo info) {

        final SchedulableTask task = tasks.get(taskIndex);
        final TornadoAcceleratorDevice device = contexts.get(contextIndex);
        boolean redeployOnDevice = graphContext.redeployOnDevice();
        CallStack stack = info.stack;
        int[] waitList = info.waitList;

        if (installedCodes[taskIndex] == null) {
            // After warming-up, it is possible to get a null pointer in the task-cache due
            // to lazy compilation for FPGAs. In tha case, we check again the code cache.
            installedCodes[taskIndex] = device.getCodeFromCache(task);
        }

        final TornadoInstalledCode installedCode = installedCodes[taskIndex];
        if (installedCode == null) {
            // There was an error during compilation -> bailout
            throw new TornadoBailoutRuntimeException("Code generator Failed");
        }

        int[] atomicsArray;
        if (task instanceof PrebuiltTask) {
            atomicsArray = ((PrebuiltTask) task).getAtomics();
        } else {
            atomicsArray = device.checkAtomicsForTask(task);
        }

        final Access[] accesses = task.getArgumentsAccess();
        if (redeployOnDevice || !stack.isOnDevice()) {
            stack.reset();
        }

        HashMap<Integer, Integer> map = new HashMap<>();
        if (gridScheduler != null && gridScheduler.get(task.getId()) != null) {
            WorkerGrid workerGrid = gridScheduler.get(task.getId());
            long[] global = workerGrid.getGlobalWork();
            int i = 0;
            for (long maxThread : global) {
                map.put(i++, (int) maxThread);
            }
        }
        stack.setHeader(map);

        ObjectBuffer bufferAtomics = null;

        for (int i = 0; i < numArgs; i++) {
            final byte argType = buffer.get();
            final int argIndex = buffer.getInt();

            if (argType == TornadoVMBytecodes.REFERENCE_ARGUMENT.value()) {
                final GlobalObjectState globalState = resolveGlobalObjectState(argIndex);
                final DeviceObjectState objectState = globalState.getDeviceState(contexts.get(contextIndex));

                if (isObjectInAtomicRegion(objectState, device, task)) {
                    atomicsArray = device.updateAtomicRegionAndObjectState(task, atomicsArray, i, objects.get(argIndex), objectState);
                    setObjectState(objectState, true);
                }
            }

            if (stack.isOnDevice()) {
                continue;
            }

            if (argType == TornadoVMBytecodes.CONSTANT_ARGUMENT.value()) {
                stack.push(constants.get(argIndex));
            } else if (argType == TornadoVMBytecodes.REFERENCE_ARGUMENT.value()) {
                if (isObjectKernelContext(objects.get(argIndex))) {
                    stack.push(null);
                    continue;
                }

                final GlobalObjectState globalState = resolveGlobalObjectState(argIndex);
                final DeviceObjectState objectState = globalState.getDeviceState(contexts.get(contextIndex));

                if (!isObjectInAtomicRegion(objectState, device, task)) {
                    final String ERROR_MESSAGE = "object is not valid: %s %s";
                    TornadoInternalError.guarantee(objectState.isValid(), ERROR_MESSAGE, objects.get(argIndex), objectState);
                    stack.push(objects.get(argIndex), objectState);
                    if (accesses[i] == Access.WRITE || accesses[i] == Access.READ_WRITE) {
                        setObjectState(objectState, true);
                    }
                }
            } else {
                TornadoInternalError.shouldNotReachHere();
            }
        }

        if (atomicsArray != null) {
            bufferAtomics = device.createOrReuseBuffer(atomicsArray);
            List<Integer> allEvents = bufferAtomics.enqueueWrite(null, 0, 0, null, false);
            if (TornadoOptions.isProfilerEnabled()) {
                for (Integer e : allEvents) {
                    Event event = device.resolveEvent(e);
                    event.waitForEvents();
                    long value = timeProfiler.getTimer(ProfilerType.COPY_IN_TIME);
                    value += event.getElapsedTime();
                    timeProfiler.setTimer(ProfilerType.COPY_IN_TIME, value);
                }
            }
            if (TornadoOptions.printBytecodes) {
                String verbose = String.format("vm: STREAM_IN  ATOMIC [0x%x] %s on %s, size=%d, offset=%d [event list=%d]", bufferAtomics.hashCode(), bufferAtomics, device, 0, 0, eventList);
                tornadoVMBytecodeList.append(verbose).append("\n");
            }
        }

        if (TornadoOptions.printBytecodes) {
            String verbose = String.format("vm: LAUNCH %s on %s, size=%d, offset=%d [event list=%d]", task.getFullName(), contexts.get(contextIndex), batchThreads, offset, eventList);
            tornadoVMBytecodeList.append(verbose).append("\n");
        }

        TaskMetaData metadata;
        if (task.meta() instanceof TaskMetaData) {
            metadata = (TaskMetaData) task.meta();
        } else {
            throw new RuntimeException("task.meta is not instanceof TaskMetadata");
        }

        // We attach the profiler
        metadata.attachProfiler(timeProfiler);
        metadata.setGridScheduler(gridScheduler);

        int lastEvent;
        try {
            if (useDependencies) {
                lastEvent = installedCode.launchWithDependencies(stack, bufferAtomics, metadata, batchThreads, waitList);
            } else {
                lastEvent = installedCode.launchWithoutDependencies(stack, bufferAtomics, metadata, batchThreads);
            }

            resetEventIndexes(eventList);

        } catch (Exception e) {
            String re = e.toString();
            if (Tornado.DEBUG) {
                e.printStackTrace();
            }
            throw new TornadoBailoutRuntimeException("Bailout from LAUNCH Bytecode: \nReason: " + re, e);
        }
        return lastEvent;
    }

    private void executeDependency(StringBuilder tornadoVMBytecodeList, int lastEvent, int eventList) {
        if (useDependencies && lastEvent != -1) {
            if (TornadoOptions.printBytecodes) {
                String verbose = String.format("vm: ADD_DEP %s to event list %d", lastEvent, eventList);
                tornadoVMBytecodeList.append(verbose).append("\n");
            }
            TornadoInternalError.guarantee(eventsIndexes[eventList] < events[eventList].length, "event list is too small");
            events[eventList][eventsIndexes[eventList]] = lastEvent;
            eventsIndexes[eventList]++;
        }
    }

    private int executeBarrier(StringBuilder tornadoVMBytecodeList, int eventList, int[] waitList) {
        if (TornadoOptions.printBytecodes) {
            tornadoVMBytecodeList.append(String.format("vm: BARRIER event-list %d\n", eventList));
        }

        int id = contexts.size() - 1;
        final TornadoAcceleratorDevice device = contexts.get(id);
        int lastEvent = device.enqueueMarker(waitList);

        resetEventIndexes(eventList);
        return lastEvent;
    }

    private void throwError(byte op) {
        if (graphContext.meta().isDebug()) {
            debug("vm: invalid op 0x%x(%d)", op, op);
        }
        throw new TornadoRuntimeException("[ERROR] TornadoVM Bytecode not recognized");
    }

    private Event execute(boolean isWarmup) {
        isWarmup = isWarmup || VIRTUAL_DEVICE_ENABLED;
        contexts.stream().filter(Objects::nonNull).forEach(TornadoAcceleratorDevice::enableThreadSharing);

        final long t0 = System.nanoTime();
        int lastEvent = -1;
        initWaitEventList();

        StringBuilder tornadoVMBytecodeList = null;
        if (TornadoOptions.printBytecodes) {
            tornadoVMBytecodeList = new StringBuilder();
        }
        while (buffer.hasRemaining()) {
            final byte op = buffer.get();
            if (op == TornadoVMBytecodes.ALLOCATE.value()) {
                final int objectIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final long sizeBatch = buffer.getLong();
                if (isWarmup) {
                    continue;
                }
                lastEvent = executeAllocate(tornadoVMBytecodeList, objectIndex, contextIndex, sizeBatch);

            } else if (op == TornadoVMBytecodes.COPY_IN.value()) {
                final int objectIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final int eventList = buffer.getInt();
                final long offset = buffer.getLong();
                final long sizeBatch = buffer.getLong();
                final int[] waitList = (useDependencies && eventList != -1) ? events[eventList] : null;
                if (isWarmup) {
                    continue;
                }

                executeCopyIn(tornadoVMBytecodeList, objectIndex, contextIndex, offset, eventList, sizeBatch, waitList);

            } else if (op == TornadoVMBytecodes.STREAM_IN.value()) {
                final int objectIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final int eventList = buffer.getInt();
                final long offset = buffer.getLong();
                final long sizeBatch = buffer.getLong();
                final int[] waitList = (useDependencies && eventList != -1) ? events[eventList] : null;
                if (isWarmup) {
                    continue;
                }

                executeStreamIn(tornadoVMBytecodeList, objectIndex, contextIndex, offset, eventList, sizeBatch, waitList);
            } else if (op == TornadoVMBytecodes.STREAM_OUT.value()) {
                final int objectIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final int eventList = buffer.getInt();
                final long offset = buffer.getLong();
                final long sizeBatch = buffer.getLong();
                final int[] waitList = (useDependencies) ? events[eventList] : null;
                if (isWarmup) {
                    continue;
                }

                lastEvent = executeStreamOut(tornadoVMBytecodeList, objectIndex, contextIndex, offset, eventList, sizeBatch, waitList);

            } else if (op == TornadoVMBytecodes.STREAM_OUT_BLOCKING.value()) {
                final int objectIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final int eventList = buffer.getInt();
                final long offset = buffer.getLong();
                final long sizeBatch = buffer.getLong();

                final int[] waitList = (useDependencies) ? events[eventList] : null;
                if (isWarmup) {
                    continue;
                }

                executeStreamOutBlocking(tornadoVMBytecodeList, objectIndex, contextIndex, offset, eventList, sizeBatch, waitList);

            } else if (op == TornadoVMBytecodes.LAUNCH.value()) {
                final int stackIndex = buffer.getInt();
                final int contextIndex = buffer.getInt();
                final int taskIndex = buffer.getInt();
                final int numArgs = buffer.getInt();
                final int eventList = buffer.getInt();
                final long offset = buffer.getLong();
                final long batchThreads = buffer.getLong();

                ExecutionInfo info = compileTaskFromBytecodeToBinary(contextIndex, stackIndex, numArgs, eventList, taskIndex, batchThreads);

                if (isWarmup) {
                    popArgumentsFromStack(numArgs);
                    continue;
                }

                lastEvent = executeLaunch(tornadoVMBytecodeList, contextIndex, numArgs, eventList, taskIndex, batchThreads, offset, info);
            } else if (op == TornadoVMBytecodes.ADD_DEP.value()) {
                final int eventList = buffer.getInt();
                if (isWarmup) {
                    continue;
                }
                executeDependency(tornadoVMBytecodeList, lastEvent, eventList);
            } else if (op == TornadoVMBytecodes.BARRIER.value()) {
                final int eventList = buffer.getInt();
                final int[] waitList = (useDependencies && eventList != -1) ? events[eventList] : null;
                if (TornadoOptions.printBytecodes) {
                    tornadoVMBytecodeList.append("BARRIER\n");
                }
                if (isWarmup) {
                    continue;
                }
                lastEvent = executeBarrier(tornadoVMBytecodeList, eventList, waitList);
            } else if (op == TornadoVMBytecodes.END.value()) {
                if (TornadoOptions.printBytecodes) {
                    tornadoVMBytecodeList.append("END\n");
                }
                break;
            } else {
                throwError(op);
            }
        }

        Event barrier = EMPTY_EVENT;
        if (!isWarmup) {
            for (TornadoAcceleratorDevice dev : contexts) {
                if (dev != null) {
                    if (useDependencies) {
                        final int event = dev.enqueueMarker();
                        barrier = dev.resolveEvent(event);
                    }

                    if (USE_VM_FLUSH) {
                        dev.flush();
                    }
                }
            }
        }

        final long t1 = System.nanoTime();
        final double elapsed = (t1 - t0) * 1e-9;
        if (!isWarmup) {
            totalTime += elapsed;
            invocations++;
        }

        if (graphContext.meta().isDebug()) {
            debug("vm: complete elapsed=%.9f s (%d iterations, %.9f s mean)", elapsed, invocations, (totalTime / invocations));
        }

        buffer.reset();

        if (TornadoOptions.printBytecodes) {
            System.out.println(tornadoVMBytecodeList.toString());
        }

        return barrier;
    }

    private void resetEventIndexes(int eventList) {
        if (eventList != -1) {
            eventsIndexes[eventList] = 0;
        }
    }

    private void popArgumentsFromStack(int numArgs) {
        for (int i = 0; i < numArgs; i++) {
            buffer.get();
            buffer.getInt();
        }
    }

    public void setGridScheduler(GridScheduler gridScheduler) {
        this.gridScheduler = gridScheduler;
    }

    public void printTimes() {
        System.out.printf("vm: complete %d iterations - %.9f s mean and %.9f s total\n", invocations, (totalTime / invocations), totalTime);
    }

    public void clearProfiles() {
        for (final SchedulableTask task : tasks) {
            task.meta().getProfiles().clear();
        }
    }

    public void dumpEvents() {
        if (!ENABLE_PROFILING || !graphContext.meta().shouldDumpEvents()) {
            info("profiling and/or event dumping is not enabled");
            return;
        }

        for (final TornadoAcceleratorDevice device : contexts) {
            device.dumpEvents();
        }
    }

    public void dumpProfiles() {
        if (!graphContext.meta().shouldDumpProfiles()) {
            info("profiling is not enabled");
            return;
        }

        for (final SchedulableTask task : tasks) {
            final TaskMetaData meta = (TaskMetaData) task.meta();
            for (final TornadoEvents eventSet : meta.getProfiles()) {
                final BitSet profiles = eventSet.getProfiles();
                for (int i = profiles.nextSetBit(0); i != -1; i = profiles.nextSetBit(i + 1)) {

                    if (!(eventSet.getDevice() instanceof TornadoAcceleratorDevice)) {
                        throw new RuntimeException("TornadoDevice not found");
                    }

                    TornadoAcceleratorDevice device = (TornadoAcceleratorDevice) eventSet.getDevice();
                    final Event profile = device.resolveEvent(i);
                    if (profile.getStatus() == COMPLETE) {
                        System.out.printf("task: %s %s %9d %9d %9d %9d %9d\n", device.getDeviceName(), meta.getId(), profile.getElapsedTime(), profile.getQueuedTime(), profile.getSubmitTime(),
                                profile.getStartTime(), profile.getEndTime());
                    }
                }
            }
        }
    }

}
