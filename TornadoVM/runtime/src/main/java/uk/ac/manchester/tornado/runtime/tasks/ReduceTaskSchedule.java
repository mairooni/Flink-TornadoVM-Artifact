/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
 */
package uk.ac.manchester.tornado.runtime.tasks;

import static uk.ac.manchester.tornado.runtime.TornadoCoreRuntime.getDebugContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.graalvm.compiler.graph.CachedGraph;
import org.graalvm.compiler.nodes.StructuredGraph;

import jdk.vm.ci.code.InstalledCode;
import jdk.vm.ci.code.InvalidInstalledCodeException;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.TaskPackage;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.enums.TornadoDeviceType;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.runtime.TornadoCoreRuntime;
import uk.ac.manchester.tornado.runtime.analyzer.CodeAnalysis;
import uk.ac.manchester.tornado.runtime.analyzer.MetaReduceCodeAnalysis;
import uk.ac.manchester.tornado.runtime.analyzer.MetaReduceTasks;
import uk.ac.manchester.tornado.runtime.analyzer.ReduceCodeAnalysis;
import uk.ac.manchester.tornado.runtime.analyzer.ReduceCodeAnalysis.REDUCE_OPERATION;
import uk.ac.manchester.tornado.runtime.common.TornadoOptions;
import uk.ac.manchester.tornado.runtime.tasks.meta.MetaDataUtils;

class ReduceTaskSchedule {

    private static final String SEQUENTIAL_TASK_REDUCE_NAME = "reduce_seq";

    private static final String TASK_SCHEDULE_PREFIX = "XXX__GENERATED_REDUCE";
    private static final int DEFAULT_GPU_WORK_GROUP = 256;
    private static final int DEFAULT_DRIVER_INDEX = 0;
    private static final int DEFAULT_DEVICE_INDEX = 0;
    private static AtomicInteger counterName = new AtomicInteger(0);
    private static AtomicInteger counterSeqName = new AtomicInteger(0);

    private String idTaskSchedule;
    private ArrayList<TaskPackage> taskPackages;
    private ArrayList<Object> streamOutObjects;
    private ArrayList<Object> streamInObjects;
    private HashMap<Object, Object> originalReduceVariables;
    private HashMap<Object, Object> hostHybridVariables;
    private ArrayList<Thread> threadSequentialExecution;
    private ArrayList<HybridThreadMeta> hybridThreadMetas;
    private HashMap<Object, Object> neutralElementsNew = new HashMap<>();
    private HashMap<Object, Object> neutralElementsOriginal = new HashMap<>();
    private TaskSchedule rewrittenTaskSchedule;
    private HashMap<Object, LinkedList<Integer>> reduceOperandTable;
    private CachedGraph<?> sketchGraph;

    private FlinkData flinkData;

    private boolean hybridMode;
    private HashMap<Object, REDUCE_OPERATION> hybridMergeTable;
    private boolean hybridInitialized;

    ReduceTaskSchedule(String taskScheduleID, ArrayList<TaskPackage> taskPackages, ArrayList<Object> streamInObjects, ArrayList<Object> streamOutObjects, CachedGraph<?> graph) {
        this.taskPackages = taskPackages;
        this.idTaskSchedule = taskScheduleID;
        this.streamInObjects = streamInObjects;
        this.streamOutObjects = streamOutObjects;
        this.sketchGraph = graph;
    }

    private boolean isAheadOfTime() {
        return TornadoOptions.FPGA_BINARIES == null ? false : true;
    }

    private void inspectBinariesFPGA(String taskScheduleName, String tsName, String taskName, boolean sequential) {
        String idTaskName = tsName + "." + taskName;
        StringBuffer originalBinaries = TornadoOptions.FPGA_BINARIES;
        if (originalBinaries != null) {
            String[] binaries = originalBinaries.toString().split(",");
            if (binaries.length == 1) {
                binaries = MetaDataUtils.processPrecompiledBinariesFromFile(binaries[0]);
                StringBuilder sb = new StringBuilder();
                for (String binary : binaries) {
                    sb.append(binary.replaceAll(" ", "")).append(",");
                }
                sb = sb.deleteCharAt(sb.length() - 1);
                originalBinaries = new StringBuffer(sb.toString());
            }

            for (int i = 0; i < binaries.length; i += 2) {
                String givenTaskName = binaries[i + 1].split(".device")[0];
                if (givenTaskName.equals(idTaskName)) {
                    int[] info = MetaDataUtils.resolveDriverDeviceIndexes(MetaDataUtils.getProperty(idTaskName + ".device"));
                    int deviceNumber = info[1];

                    if (!sequential) {
                        originalBinaries.append("," + binaries[i] + "," + taskScheduleName + "." + taskName + ".device=0:" + deviceNumber);
                    } else {
                        originalBinaries.append("," + binaries[i] + "," + taskScheduleName + "." + SEQUENTIAL_TASK_REDUCE_NAME + counterSeqName + ".device=0:" + deviceNumber);
                    }
                }
            }
            TornadoOptions.FPGA_BINARIES = originalBinaries;
        }
    }

    private int[] changeDriverAndDeviceIfNeeded(String taskScheduleName, String tsName, String taskName) {
        String idTaskName = tsName + "." + taskName;
        boolean isDeviceDefined = MetaDataUtils.getProperty(idTaskName + ".device") != null;
        if (isDeviceDefined) {
            int[] info = MetaDataUtils.resolveDriverDeviceIndexes(MetaDataUtils.getProperty(idTaskName + ".device"));
            int driverNumber = info[0];
            int deviceNumber = info[1];
            TornadoRuntime.setProperty(taskScheduleName + "." + taskName + ".device", driverNumber + ":" + deviceNumber);
            return info;
        }
        return null;
    }

    private void fillOutputArrayWithNeutral(Object reduceArray, Object neutral) {
        if (reduceArray instanceof int[]) {
            Arrays.fill((int[]) reduceArray, (int) neutral);
        } else if (reduceArray instanceof float[]) {
            Arrays.fill((float[]) reduceArray, (float) neutral);
        } else if (reduceArray instanceof double[]) {
            Arrays.fill((double[]) reduceArray, (double) neutral);
        } else if (reduceArray instanceof long[]) {
            Arrays.fill((long[]) reduceArray, (long) neutral);
        } else {
            throw new TornadoRuntimeException("[ERROR] reduce type not supported yet: " + reduceArray.getClass());
        }
    }

    private Object createNewReduceArray(Object reduceVariable, int size) {
        if (size == 1) {
            return reduceVariable;
        }
        if (reduceVariable instanceof int[]) {
            return new int[size];
        } else if (reduceVariable instanceof float[]) {
            return new float[size];
        } else if (reduceVariable instanceof double[]) {
            return new double[size];
        } else if (reduceVariable instanceof long[]) {
            return new long[size];
        } else {
            throw new TornadoRuntimeException("[ERROR] reduce type not supported yet: " + reduceVariable.getClass());
        }
    }

    private Object createNewReduceArray(Object reduceVariable) {
        if (reduceVariable instanceof int[]) {
            return new int[1];
        } else if (reduceVariable instanceof float[]) {
            return new float[1];
        } else if (reduceVariable instanceof double[]) {
            return new double[1];
        } else if (reduceVariable instanceof long[]) {
            return new long[1];
        } else {
            throw new TornadoRuntimeException("[ERROR] reduce type not supported yet: " + reduceVariable.getClass());
        }
    }

    private Object getNeutralElement(Object originalArray) {
        if (originalArray instanceof int[]) {
            return ((int[]) originalArray)[0];
        } else if (originalArray instanceof float[]) {
            return ((float[]) originalArray)[0];
        } else if (originalArray instanceof double[]) {
            return ((double[]) originalArray)[0];
        } else if (originalArray instanceof long[]) {
            return ((long[]) originalArray)[0];
        } else {
            throw new TornadoRuntimeException("[ERROR] reduce type not supported yet: " + originalArray.getClass());
        }
    }

    private boolean isPowerOfTwo(final long number) {
        return ((number & (number - 1)) == 0);
    }

    /**
     * It runs a compiled method by Graal in HotSpot.
     *
     * @param taskPackage
     *            {@link TaskPackage} metadata that stores the method parameters.
     * @param code
     *            {@link InstalledCode} code to be executed
     * @param hostHybridVariables
     *            HashMap that relates the GPU buffer with the new CPU buffer.
     */
    private void runBinaryCodeForReduction(TaskPackage taskPackage, InstalledCode code, HashMap<Object, Object> hostHybridVariables) {
        try {
            // Execute the generated binary with Graal with
            // the host loop-bound

            // 1. Set arguments to the method-compiled code
            int numArgs = taskPackage.getTaskParameters().length - 1;
            Object[] args = new Object[numArgs];
            for (int i = 0; i < numArgs; i++) {
                Object argument = taskPackage.getTaskParameters()[i + 1];
                args[i] = hostHybridVariables.getOrDefault(argument, argument);
            }

            // 2. Run the binary
            code.executeVarargs(args);

        } catch (InvalidInstalledCodeException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns true if the input size is not power of 2 and the target device is
     * either the GPU or the FPGA.
     *
     * @param targetDeviceToRun
     *            index of the target device within the Tornado device list.
     * @return boolean
     */
    private boolean isTaskEligibleSplitHostAndDevice(final int targetDeviceToRun, final long elementsReductionLeftOver) {
        if (elementsReductionLeftOver > 0) {
            TornadoDeviceType deviceType = TornadoCoreRuntime.getTornadoRuntime().getDriver(0).getDevice(targetDeviceToRun).getDeviceType();
            return (deviceType == TornadoDeviceType.GPU || deviceType == TornadoDeviceType.FPGA || deviceType == TornadoDeviceType.ACCELERATOR);
        }
        return false;
    }

    private void joinHostThreads() {
        if (threadSequentialExecution != null && !threadSequentialExecution.isEmpty()) {
            threadSequentialExecution.stream().forEach(thread -> {
                try {
                    thread.join();
                    hybridInitialized = false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static class CompilationThread extends Thread {
        private Object codeTask;
        private final long sizeTargetDevice;
        private InstalledCode code;
        private boolean finished;

        CompilationThread(Object codeTask, final long sizeTargetDevice) {
            this.codeTask = codeTask;
            this.sizeTargetDevice = sizeTargetDevice;
        }

        public InstalledCode getCode() {
            return this.code;
        }

        public boolean isFinished() {
            return finished;
        }

        @Override
        public void run() {
            StructuredGraph originalGraph = CodeAnalysis.buildHighLevelGraalGraph(codeTask);
            assert originalGraph != null;
            StructuredGraph graph = (StructuredGraph) originalGraph.copy(getDebugContext());
            ReduceCodeAnalysis.performLoopBoundNodeSubstitution(graph, sizeTargetDevice);
            code = CodeAnalysis.compileAndInstallMethod(graph);
            finished = true;
        }
    }

    private class SequentialExecutionThread extends Thread {

        final CompilationThread compilationThread;
        private TaskPackage taskPackage;
        private HashMap<Object, Object> hostHybridVariables;

        SequentialExecutionThread(CompilationThread compilationThread, TaskPackage taskPackage, HashMap<Object, Object> hostHybridVariables) {
            this.compilationThread = compilationThread;
            this.taskPackage = taskPackage;
            this.hostHybridVariables = hostHybridVariables;
        }

        @Override
        public void run() {
            try {
                // We need to wait for the compilation to be finished
                compilationThread.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
            runBinaryCodeForReduction(taskPackage, compilationThread.getCode(), hostHybridVariables);
        }
    }

    private CompilationThread createCompilationThread(final TaskPackage taskPackage, final long sizeTargetDevice, HashMap<Object, Object> hostHybridVariables) {
        Object codeTask = taskPackage.getTaskParameters()[0];
        CompilationThread compilationThread = new CompilationThread(codeTask, sizeTargetDevice);
        return compilationThread;
    }

    private void updateStreamInOutVariables(HashMap<Integer, MetaReduceTasks> tableReduce) {
        // Update Stream IN and Stream OUT
        for (int taskNumber = 0; taskNumber < taskPackages.size(); taskNumber++) {

            // Update streamIn if needed (substitute if output appears as
            // stream-in with the new created array).
            for (int i = 0; i < streamInObjects.size(); i++) {

                // Update table that consistency between input variables and reduce tasks.
                // This part is used to STREAM_IN data when performing multiple reductions in
                // the same task-schedule
                if (tableReduce.containsKey(taskNumber)) {
                    if (!reduceOperandTable.containsKey(streamInObjects.get(i))) {
                        LinkedList<Integer> taskList = new LinkedList<>();
                        taskList.add(taskNumber);
                        reduceOperandTable.put(streamInObjects.get(i), taskList);
                    }
                }

                if (originalReduceVariables.containsKey(streamInObjects.get(i))) {
                    streamInObjects.set(i, originalReduceVariables.get(streamInObjects.get(i)));
                }
            }

            // Add the rest of the variables
            for (Entry<Object, Object> reduceArray : originalReduceVariables.entrySet()) {
                streamInObjects.add(reduceArray.getValue());
            }

            TornadoTaskSchedule.performStreamInThread(rewrittenTaskSchedule, streamInObjects);

            for (int i = 0; i < streamOutObjects.size(); i++) {
                if (originalReduceVariables.containsKey(streamOutObjects.get(i))) {
                    Object newArray = originalReduceVariables.get(streamOutObjects.get(i));
                    streamOutObjects.set(i, newArray);
                }
            }
        }
    }

    private boolean isDeviceAnAccelerator(final int deviceToRun) {
        TornadoDeviceType deviceType = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(deviceToRun).getDeviceType();
        return (deviceType == TornadoDeviceType.ACCELERATOR);
    }

    private void updateGlobalAndLocalDimensionsFPGA(final int deviceToRun, String taskScheduleReduceName, TaskPackage taskPackage, int inputSize) {
        // Update GLOBAL and LOCAL Dims if device to run is the FPGA
        if (isAheadOfTime() && isDeviceAnAccelerator(deviceToRun)) {
            TornadoRuntime.setProperty(taskScheduleReduceName + "." + taskPackage.getId() + ".global.dims", Integer.toString(inputSize));
            TornadoRuntime.setProperty(taskScheduleReduceName + "." + taskPackage.getId() + ".local.dims", "64");
        }
    }

    private Object createHostArrayForHybridMode(Object originalReduceArray, TaskPackage taskPackage, int sizeTargetDevice) {
        hybridMode = true;
        if (hostHybridVariables == null) {
            hostHybridVariables = new HashMap<>();
        }
        Object hybridArray = createNewReduceArray(originalReduceArray);
        Object neutralElement = getNeutralElement(originalReduceArray);
        fillOutputArrayWithNeutral(hybridArray, neutralElement);
        taskPackage.setNumThreadsToRun(sizeTargetDevice);
        return hybridArray;
    }

    private static class HybridThreadMeta {
        private TaskPackage taskPackage;
        private CompilationThread compilationThread;

        public HybridThreadMeta(TaskPackage taskPackage, CompilationThread compilationThread) {
            this.taskPackage = taskPackage;
            this.compilationThread = compilationThread;
        }
    }

    /**
     * Compose and execute the new reduction. It dynamically creates a new
     * task-schedule expression that contains: a) the parallel reduction; b) the
     * final sequential reduction.
     * <p>
     * It also creates a new thread in the case the input size for the reduction is
     * not power of two and the target device is either the FPGA or the GPU. In this
     * case, the new thread will compile the host part with the corresponding
     * sub-range that does not fit into the power-of-two part.
     *
     * @param metaReduceTable
     *            Metadata to create all new tasks for the reductions dynamically.
     * @return {@link TaskSchedule} with the new reduction
     */
    TaskSchedule scheduleWithReduction(MetaReduceCodeAnalysis metaReduceTable) {

        assert metaReduceTable != null;

        HashMap<Integer, MetaReduceTasks> tableReduce = metaReduceTable.getTable();

        String taskScheduleReduceName = TASK_SCHEDULE_PREFIX + counterName.get();
        String tsName = idTaskSchedule;

        HashMap<Integer, ArrayList<Object>> streamReduceTable = new HashMap<>();
        ArrayList<Integer> sizesReductionArray = new ArrayList<>();
        if (originalReduceVariables == null) {
            originalReduceVariables = new HashMap<>();
        }

        if (reduceOperandTable == null) {
            reduceOperandTable = new HashMap<>();
        }

        int driverToRun = DEFAULT_DRIVER_INDEX;
        int deviceToRun = DEFAULT_DEVICE_INDEX;

        // Create new buffer variables and update the corresponding streamIn and
        // streamOut
        for (int taskNumber = 0; taskNumber < taskPackages.size(); taskNumber++) {

            ArrayList<Integer> listOfReduceIndexParameters;
            TaskPackage taskPackage = taskPackages.get(taskNumber);

            ArrayList<Object> streamReduceList = new ArrayList<>();

            int[] driverAndDevice = changeDriverAndDeviceIfNeeded(taskScheduleReduceName, tsName, taskPackage.getId());
            if (driverAndDevice != null) {
                driverToRun = driverAndDevice[0];
                deviceToRun = driverAndDevice[1];
            }
            inspectBinariesFPGA(taskScheduleReduceName, tsName, taskPackage.getId(), false);

            if (tableReduce.containsKey(taskNumber)) {

                MetaReduceTasks metaReduceTasks = tableReduce.get(taskNumber);
                listOfReduceIndexParameters = metaReduceTasks.getListOfReduceParameters(taskNumber);

                int inputSize = 0;
                for (Integer paramIndex : listOfReduceIndexParameters) {

                    Object originalReduceArray = taskPackage.getTaskParameters()[paramIndex + 1];

                    // If the array has been already created, we don't have to create another one,
                    // just obtain the already created reference from the cache-table.
                    if (originalReduceVariables.containsKey(originalReduceArray)) {
                        continue;
                    }

                    inputSize = metaReduceTasks.getInputSize(taskNumber);

                    updateGlobalAndLocalDimensionsFPGA(deviceToRun, taskScheduleReduceName, taskPackage, inputSize);

                    // Analyse Input Size - if not power of 2 -> split host and device executions
                    boolean isInputPowerOfTwo = isPowerOfTwo(inputSize);
                    Object hostHybridModeArray = null;
                    if (!isInputPowerOfTwo) {
                        int exp = (int) (Math.log(inputSize) / Math.log(2));
                        double closestPowerOf2 = Math.pow(2, exp);
                        int elementsReductionLeftOver = (int) (inputSize - closestPowerOf2);
                        inputSize -= elementsReductionLeftOver;
                        final int sizeTargetDevice = inputSize;
                        if (isTaskEligibleSplitHostAndDevice(deviceToRun, elementsReductionLeftOver)) {
                            hostHybridModeArray = createHostArrayForHybridMode(originalReduceArray, taskPackage, sizeTargetDevice);
                        }
                    }

                    // Set the new array size
                    int sizeReductionArray = obtainSizeArrayResult(driverToRun, deviceToRun, inputSize);
                    Object newDeviceArray = createNewReduceArray(originalReduceArray, sizeReductionArray);
                    Object neutralElement = getNeutralElement(originalReduceArray);
                    fillOutputArrayWithNeutral(newDeviceArray, neutralElement);

                    neutralElementsNew.put(newDeviceArray, neutralElement);
                    neutralElementsOriginal.put(originalReduceArray, neutralElement);

                    // Store metadata
                    streamReduceList.add(newDeviceArray);
                    sizesReductionArray.add(sizeReductionArray);
                    originalReduceVariables.put(originalReduceArray, newDeviceArray);

                    if (hybridMode) {
                        hostHybridVariables.put(newDeviceArray, hostHybridModeArray);
                    }
                }

                streamReduceTable.put(taskNumber, streamReduceList);

                if (hybridMode) {
                    CompilationThread compilationThread = createCompilationThread(taskPackage, inputSize, hostHybridVariables);
                    compilationThread.start();
                    if (threadSequentialExecution == null) {
                        threadSequentialExecution = new ArrayList<>();
                    }

                    HybridThreadMeta meta = new HybridThreadMeta(taskPackage, compilationThread);
                    if (hybridThreadMetas == null) {
                        hybridThreadMetas = new ArrayList<>();
                    }
                    hybridThreadMetas.add(meta);

                    SequentialExecutionThread sequentialExecutionThread = new SequentialExecutionThread(compilationThread, taskPackage, hostHybridVariables);
                    threadSequentialExecution.add(sequentialExecutionThread);
                    sequentialExecutionThread.start();
                    hybridInitialized = true;
                }
            }
        }

        rewrittenTaskSchedule = new TaskSchedule(taskScheduleReduceName);
        rewrittenTaskSchedule.flinkInfo(flinkData);

        updateStreamInOutVariables(metaReduceTable.getTable());

        // Compose Task Schedule
        for (int taskNumber = 0; taskNumber < taskPackages.size(); taskNumber++) {

            TaskPackage taskPackage = taskPackages.get(taskNumber);

            // Update the reference for the new tasks if there is a data
            // dependency with the new variables created by the TornadoVM
            int taskType = taskPackages.get(taskNumber).getTaskType();
            for (int i = 0; i < taskType; i++) {
                Object key = taskPackages.get(taskNumber).getTaskParameters()[i + 1];
                if (originalReduceVariables.containsKey(key)) {
                    Object value = originalReduceVariables.get(key);
                    taskPackages.get(taskNumber).getTaskParameters()[i + 1] = value;
                }
            }

            // Analyze of we have multiple reduce tasks in the same task-schedule. In the
            // case we reuse same input data, we need to stream in the input the rest of the
            // reduce parallel tasks
            if (tableReduce.containsKey(taskNumber)) {
                // We only analyze for parallel tasks
                for (int i = 0; i < taskPackages.get(taskNumber).getTaskParameters().length - 1; i++) {
                    Object parameterToMethod = taskPackages.get(taskNumber).getTaskParameters()[i + 1];
                    if (reduceOperandTable.containsKey(parameterToMethod)) {
                        if (reduceOperandTable.get(parameterToMethod).size() > 1) {
                            rewrittenTaskSchedule.forceCopyIn(parameterToMethod);
                        }
                    }
                }
            }

            rewrittenTaskSchedule.addTask(taskPackages.get(taskNumber));
            // Add extra task with the final reduction
            if (tableReduce.containsKey(taskNumber)) {

                MetaReduceTasks metaReduceTasks = tableReduce.get(taskNumber);
                ArrayList<Integer> listOfReduceParameters = metaReduceTasks.getListOfReduceParameters(taskNumber);
                StructuredGraph graph = metaReduceTasks.getGraph();
                ArrayList<REDUCE_OPERATION> operations = ReduceCodeAnalysis.getReduceOperation(graph, listOfReduceParameters);

                if (operations.isEmpty()) {
                    // perform analysis with cached graph (after sketch phase)
                    operations = ReduceCodeAnalysis.getReduceOperatorFromSketch(sketchGraph, listOfReduceParameters);
                }

                ArrayList<Object> streamUpdateList = streamReduceTable.get(taskNumber);

                for (int i = 0; i < streamUpdateList.size(); i++) {
                    Object newArray = streamUpdateList.get(i);
                    int sizeReduceArray = sizesReductionArray.get(i);
                    for (REDUCE_OPERATION operation : operations) {
                        final String newTaskSequentialName = SEQUENTIAL_TASK_REDUCE_NAME + counterSeqName.get();
                        String fullName = rewrittenTaskSchedule.getTaskScheduleName() + "." + newTaskSequentialName;
                        TornadoRuntime.setProperty(fullName + ".device", driverToRun + ":" + deviceToRun);
                        inspectBinariesFPGA(taskScheduleReduceName, tsName, taskPackage.getId(), true);

                        switch (operation) {
                            case ADD:
                                ReduceFactory.handleAdd(newArray, rewrittenTaskSchedule, sizeReduceArray, newTaskSequentialName);
                                break;
                            case MUL:
                                ReduceFactory.handleMul(newArray, rewrittenTaskSchedule, sizeReduceArray, newTaskSequentialName);
                                break;
                            case MAX:
                                ReduceFactory.handleMax(newArray, rewrittenTaskSchedule, sizeReduceArray, newTaskSequentialName);
                                break;
                            case MIN:
                                ReduceFactory.handleMin(newArray, rewrittenTaskSchedule, sizeReduceArray, newTaskSequentialName);
                                break;
                            default:
                                throw new TornadoRuntimeException("[ERROR] Reduce operation not supported yet.");
                        }

                        if (hybridMode) {
                            if (hybridMergeTable == null) {
                                hybridMergeTable = new HashMap<>();
                            }
                            hybridMergeTable.put(newArray, operation);
                        }
                        counterSeqName.incrementAndGet();
                    }
                }
            }
        }
        TornadoTaskSchedule.performStreamOutThreads(rewrittenTaskSchedule, streamOutObjects);
        executeExpression();
        counterName.incrementAndGet();
        return rewrittenTaskSchedule;
    }

    void executeExpression() {
        setNeutralElement();
        if (hybridMode && !hybridInitialized) {
            hybridInitialized = true;
            threadSequentialExecution.clear();
            for (HybridThreadMeta meta : hybridThreadMetas) {
                threadSequentialExecution.add(new SequentialExecutionThread(meta.compilationThread, meta.taskPackage, hostHybridVariables));
            }
            threadSequentialExecution.stream().forEach(Thread::start);
        }
        rewrittenTaskSchedule.execute();
        updateOutputArrays();
    }

    private void setNeutralElement() {
        for (Entry<Object, Object> pair : neutralElementsNew.entrySet()) {
            Object newArray = pair.getKey();
            Object neutralElement = pair.getValue();
            fillOutputArrayWithNeutral(newArray, neutralElement);

            // Hybrid Execution
            if (hostHybridVariables != null && hostHybridVariables.containsKey(newArray)) {
                Object arrayCPU = hostHybridVariables.get(newArray);
                fillOutputArrayWithNeutral(arrayCPU, neutralElement);
            }

        }

        for (Entry<Object, Object> pair : neutralElementsOriginal.entrySet()) {
            Object originalArray = pair.getKey();
            Object neutralElement = pair.getValue();
            fillOutputArrayWithNeutral(originalArray, neutralElement);
        }
    }

    private int operateFinalReduction(int a, int b, REDUCE_OPERATION operation) {
        switch (operation) {
            case ADD:
                return a + b;
            case MUL:
                return a * b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default:
                throw new TornadoRuntimeException("Operation not supported");
        }
    }

    private float operateFinalReduction(float a, float b, REDUCE_OPERATION operation) {
        switch (operation) {
            case ADD:
                return a + b;
            case MUL:
                return a * b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default:
                throw new TornadoRuntimeException("Operation not supported");
        }
    }

    private double operateFinalReduction(double a, double b, REDUCE_OPERATION operation) {
        switch (operation) {
            case ADD:
                return a + b;
            case MUL:
                return a * b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default:
                throw new TornadoRuntimeException("Operation not supported");
        }
    }

    private long operateFinalReduction(long a, long b, REDUCE_OPERATION operation) {
        switch (operation) {
            case ADD:
                return a + b;
            case MUL:
                return a * b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default:
                throw new TornadoRuntimeException("Operation not supported");
        }
    }

    private void updateVariableFromAccelerator(Object originalReduceVariable, Object newArray) {
        switch (newArray.getClass().getTypeName()) {
            case "int[]":
                ((int[]) originalReduceVariable)[0] = ((int[]) newArray)[0];
                break;
            case "float[]":
                ((float[]) originalReduceVariable)[0] = ((float[]) newArray)[0];
                break;
            case "double[]":
                ((double[]) originalReduceVariable)[0] = ((double[]) newArray)[0];
                break;
            case "long[]":
                ((long[]) originalReduceVariable)[0] = ((long[]) newArray)[0];
                break;
            default:
                throw new TornadoRuntimeException("[ERROR] Reduce data type not supported yet: " + newArray.getClass().getTypeName());
        }
    }

    private void mergeHybridMode(Object originalReduceVariable, Object newArray) {
        switch (newArray.getClass().getTypeName()) {
            case "int[]":
                int a = ((int[]) hostHybridVariables.get(newArray))[0];
                int b = ((int[]) newArray)[0];
                ((int[]) originalReduceVariable)[0] = operateFinalReduction(a, b, hybridMergeTable.get(newArray));
                break;
            case "float[]":
                float af = ((float[]) hostHybridVariables.get(newArray))[0];
                float bf = ((float[]) newArray)[0];
                ((float[]) originalReduceVariable)[0] = operateFinalReduction(af, bf, hybridMergeTable.get(newArray));
                break;
            case "double[]":
                double ad = ((double[]) hostHybridVariables.get(newArray))[0];
                double bd = ((double[]) newArray)[0];
                ((double[]) originalReduceVariable)[0] = operateFinalReduction(ad, bd, hybridMergeTable.get(newArray));
                break;
            case "long[]":
                long al = ((long[]) hostHybridVariables.get(newArray))[0];
                long bl = ((long[]) newArray)[0];
                ((long[]) originalReduceVariable)[0] = operateFinalReduction(al, bl, hybridMergeTable.get(newArray));
                break;
            default:
                throw new TornadoRuntimeException("[ERROR] Reduce data type not supported yet: " + newArray.getClass().getTypeName());
        }
    }

    /**
     * Copy out the result back to the original buffer.
     *
     * <p>
     * If the hybrid mode is enabled, it performs the final 1D reduction between the
     * two elements left (one from the accelerator and the other from the CPU)
     * </p>
     */
    private void updateOutputArrays() {
        joinHostThreads();
        for (Entry<Object, Object> pair : originalReduceVariables.entrySet()) {
            Object originalReduceVariable = pair.getKey();
            Object newArray = pair.getValue();
            if (hostHybridVariables != null && hostHybridVariables.containsKey(newArray)) {
                mergeHybridMode(originalReduceVariable, newArray);
            } else {
                updateVariableFromAccelerator(originalReduceVariable, newArray);
            }
        }
    }

    /**
     * @param driverIndex
     *            Index within the Tornado drivers' index
     * @param device
     *            Index of the device within the Tornado's device list.
     * @param inputSize
     *            Input size
     * @return Output array size
     */
    private static int obtainSizeArrayResult(int driverIndex, int device, int inputSize) {
        TornadoDeviceType deviceType = TornadoCoreRuntime.getTornadoRuntime().getDriver(driverIndex).getDevice(device).getDeviceType();
        TornadoDevice deviceToRun = TornadoCoreRuntime.getTornadoRuntime().getDriver(driverIndex).getDevice(device);
        switch (deviceType) {
            case CPU:
                return deviceToRun.getAvailableProcessors() + 1;
            case GPU:
            case ACCELERATOR:
                return inputSize > calculateAcceleratorGroupSize(deviceToRun, inputSize) ? (inputSize / calculateAcceleratorGroupSize(deviceToRun, inputSize)) + 1 : 2;
            default:
                break;
        }
        return 0;
    }

    /**
     * It computes the right local work group size for GPUs/FPGAs.
     *
     * @param device
     *            Input device.
     * @param globalWorkSize
     *            Number of global threads to run.
     * @return Local Work Threads.
     */
    private static int calculateAcceleratorGroupSize(TornadoDevice device, long globalWorkSize) {

        if (device.getPlatformName().contains("AMD")) {
            return DEFAULT_GPU_WORK_GROUP;
        }

        int maxBlockSize = (int) device.getDeviceMaxWorkgroupDimensions()[0];

        if (maxBlockSize <= 0) {
            // Due to a bug on Xilinx platforms, this value can be -1. In that case, we
            // setup the block size to the default value.
            return DEFAULT_GPU_WORK_GROUP;
        }

        if (maxBlockSize == globalWorkSize) {
            maxBlockSize /= 4;
        }

        int value = (int) Math.min(maxBlockSize, globalWorkSize);
        while (globalWorkSize % value != 0) {
            value--;
        }
        return value;
    }

    private ArrayList<Thread> getHostThreadReduction() {
        return this.threadSequentialExecution;
    }

    public void setFlinkData(FlinkData flinkData) {
        this.flinkData = flinkData;
    }

}
