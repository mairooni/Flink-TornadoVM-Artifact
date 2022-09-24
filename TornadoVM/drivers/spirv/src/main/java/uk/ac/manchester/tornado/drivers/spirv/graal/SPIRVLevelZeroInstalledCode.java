/*
 * This file is part of Tornado: A heterogeneous programming framework:
 * https://github.com/beehive-lab/tornadovm
 *
 * Copyright (c) 2021, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
package uk.ac.manchester.tornado.drivers.spirv.graal;

import java.util.Arrays;

import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.mm.ObjectBuffer;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVDeviceContext;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVLevelZeroCommandQueue;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVLevelZeroModule;
import uk.ac.manchester.tornado.drivers.spirv.SPIRVModule;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.LevelZeroCommandList;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.LevelZeroKernel;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.Sizeof;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.ZeEventHandle;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.ZeGroupDispatch;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.ZeKernelHandle;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.ZeResult;
import uk.ac.manchester.tornado.drivers.spirv.levelzero.utils.LevelZeroUtils;
import uk.ac.manchester.tornado.drivers.spirv.mm.SPIRVByteBuffer;
import uk.ac.manchester.tornado.drivers.spirv.timestamps.LevelZeroKernelTimeStamp;
import uk.ac.manchester.tornado.runtime.common.CallStack;
import uk.ac.manchester.tornado.runtime.common.TornadoOptions;
import uk.ac.manchester.tornado.runtime.tasks.meta.TaskMetaData;

public class SPIRVLevelZeroInstalledCode extends SPIRVInstalledCode {

    public static final String WARNING_THREAD_LOCAL = "[TornadoVM SPIR-V] Warning: TornadoVM changed the user-defined local thread sizes to the suggested values by the driver.";

    private static final int WARP_SIZE = 32;
    private boolean valid;
    private boolean ADJUST_IRREGULAR = false;
    private LevelZeroKernelTimeStamp kernelTimeStamp;
    private ThreadBlockDispatcher dispatcher;
    private DeviceThreadScheduling threadScheduling;

    public SPIRVLevelZeroInstalledCode(String name, SPIRVModule spirvModule, SPIRVDeviceContext deviceContext) {
        super(name, spirvModule, deviceContext);
        this.valid = true;
    }

    @Override
    public int launchWithDependencies(CallStack stack, ObjectBuffer atomicSpace, TaskMetaData meta, long batchThreads, int[] waitEvents) {
        throw new RuntimeException("Unimplemented");
    }

    private void setKernelArgs(final SPIRVByteBuffer stack, final ObjectBuffer atomicSpace, TaskMetaData meta) {
        // Enqueue write
        stack.enqueueWrite(null);

        SPIRVLevelZeroModule module = (SPIRVLevelZeroModule) spirvModule;
        LevelZeroKernel levelZeroKernel = module.getKernel();
        ZeKernelHandle kernel = levelZeroKernel.getKernelHandle();

        int index = 0;
        // device's heap (on the device global's memory)
        int result = levelZeroKernel.zeKernelSetArgumentValue(kernel.getPtrZeKernelHandle(), index, Sizeof.LONG.getNumBytes(), stack.toBuffer());
        LevelZeroUtils.errorLog("zeKernelSetArgumentValue", result);
        index++;

        // index of the stack pointer (it is usually zero)
        result = levelZeroKernel.zeKernelSetArgumentValue(kernel.getPtrZeKernelHandle(), index, Sizeof.LONG.getNumBytes(), stack.toRelativeAddress());
        LevelZeroUtils.errorLog("zeKernelSetArgumentValue", result);
        index++;
    }

    private static class DeviceThreadScheduling {
        long[] globalWork;
        long[] localWork;

        public DeviceThreadScheduling(long[] globalWork, long[] localWork) {
            this.globalWork = globalWork;
            this.localWork = localWork;
        }

    }

    private DeviceThreadScheduling calculateGlobalAndLocalBlockOfThreads(TaskMetaData meta, long batchThreads) {
        long[] globalWork = new long[3];
        long[] localWork = new long[3];
        Arrays.fill(globalWork, 1);
        Arrays.fill(localWork, 1);

        if (!meta.isGridSchedulerEnabled()) {
            int dims = meta.getDims();
            if (!meta.isGlobalWorkDefined()) {
                calculateGlobalWork(meta, batchThreads);
            }
            if (!meta.isLocalWorkDefined()) {
                calculateLocalWork(meta);
            }
            System.arraycopy(meta.getGlobalWork(), 0, globalWork, 0, dims);
            System.arraycopy(meta.getLocalWork(), 0, localWork, 0, dims);
        } else {
            checkLocalWorkGroupFitsOnDevice(meta);

            WorkerGrid worker = meta.getWorkerGrid(meta.getId());
            int dims = worker.dimension();

            System.arraycopy(worker.getGlobalWork(), 0, globalWork, 0, dims);

            if (worker.getLocalWork() != null) {
                System.arraycopy(worker.getLocalWork(), 0, localWork, 0, dims);
            }

        }
        return new DeviceThreadScheduling(globalWork, localWork);
    }

    private static class ThreadBlockDispatcher {
        int[] groupSizeX;
        int[] groupSizeY;
        int[] groupSizeZ;

        public ThreadBlockDispatcher(int[] groupSizeX, int[] groupSizeY, int[] groupSizeZ) {
            this.groupSizeX = groupSizeX;
            this.groupSizeY = groupSizeY;
            this.groupSizeZ = groupSizeZ;
        }
    }

    private int setThreadSuggestionFromLevelZero(LevelZeroKernel levelZeroKernel, ZeKernelHandle kernel, int[] groupSizeX, int[] groupSizeY, int[] groupSizeZ) {
        int result = levelZeroKernel.zeKernelSuggestGroupSize(kernel.getPtrZeKernelHandle(), (int) threadScheduling.globalWork[0], (int) threadScheduling.globalWork[1],
                (int) threadScheduling.globalWork[2], groupSizeX, groupSizeY, groupSizeZ);
        LevelZeroUtils.errorLog("zeKernelSuggestGroupSize", result);
        result = levelZeroKernel.zeKernelSetGroupSize(kernel.getPtrZeKernelHandle(), groupSizeX, groupSizeY, groupSizeZ);
        LevelZeroUtils.errorLog("zeKernelSetGroupSize", result);
        return result;
    }

    private ThreadBlockDispatcher suggestThreadSchedulingToLevelZeroDriver(DeviceThreadScheduling threadScheduling, LevelZeroKernel levelZeroKernel, ZeKernelHandle kernel, TaskMetaData meta) {

        // Prepare kernel for launch
        // A) Suggest scheduling parameters to level-zero
        int[] groupSizeX = new int[] { (int) threadScheduling.localWork[0] };
        int[] groupSizeY = new int[] { (int) threadScheduling.localWork[1] };
        int[] groupSizeZ = new int[] { (int) threadScheduling.localWork[2] };

        if (!meta.isWorkerGridAvailable()) {
            int result = levelZeroKernel.zeKernelSuggestGroupSize(kernel.getPtrZeKernelHandle(), (int) threadScheduling.globalWork[0], (int) threadScheduling.globalWork[1],
                    (int) threadScheduling.globalWork[2], groupSizeX, groupSizeY, groupSizeZ);
            LevelZeroUtils.errorLog("zeKernelSuggestGroupSize", result);
        }

        int result = levelZeroKernel.zeKernelSetGroupSize(kernel.getPtrZeKernelHandle(), groupSizeX, groupSizeY, groupSizeZ);
        LevelZeroUtils.errorLog("zeKernelSetGroupSize", result);

        if (result == ZeResult.ZE_RESULT_ERROR_INVALID_GROUP_SIZE_DIMENSION) {
            // At this point, we can only get a ZE_RESULT_ERROR_INVALID_GROUP_SIZE_DIMENSION
            // only when using the GridScheduler API to bypass the thread scheduler
            // suggestions of Level Zero. In this case, we call the suggestions and set up
            // the right thread block sizes.
            System.out.println(WARNING_THREAD_LOCAL);
            setThreadSuggestionFromLevelZero(levelZeroKernel, kernel, groupSizeX, groupSizeY, groupSizeZ);
        }

        if (meta.isGridSchedulerEnabled()) {
            WorkerGrid grid = meta.getWorkerGrid(meta.getId());
            grid.setLocalWork(groupSizeX[0], groupSizeY[0], groupSizeZ[0]);
        }

        long[] localWorkAfterSuggestion = new long[] { groupSizeX[0], groupSizeY[0], groupSizeZ[0] };
        meta.setLocalWork(localWorkAfterSuggestion);

        return new ThreadBlockDispatcher(groupSizeX, groupSizeY, groupSizeZ);
    }

    private void launchKernelWithLevelZero(ZeKernelHandle kernel, DeviceThreadScheduling threadScheduling, ThreadBlockDispatcher dispatcher) {
        // Dispatch SPIR-V Kernel
        ZeGroupDispatch dispatch = new ZeGroupDispatch();
        dispatch.setGroupCountX(threadScheduling.globalWork[0] / dispatcher.groupSizeX[0]);
        dispatch.setGroupCountY(threadScheduling.globalWork[1] / dispatcher.groupSizeY[0]);
        dispatch.setGroupCountZ(threadScheduling.globalWork[2] / dispatcher.groupSizeZ[0]);

        SPIRVLevelZeroCommandQueue commandQueue = (SPIRVLevelZeroCommandQueue) deviceContext.getSpirvContext().getCommandQueueForDevice(deviceContext.getDeviceIndex());
        LevelZeroCommandList commandList = commandQueue.getCommandList();

        if (TornadoOptions.isProfilerEnabled()) {
            kernelTimeStamp = new LevelZeroKernelTimeStamp(deviceContext, commandList, commandQueue);
            kernelTimeStamp.createEventTimer();
        }

        ZeEventHandle kernelEventTimer = kernelTimeStamp != null ? kernelTimeStamp.getKernelEventTimer() : null;

        // Launch the kernel on the Intel Integrated GPU
        int result = commandList.zeCommandListAppendLaunchKernel(commandList.getCommandListHandlerPtr(), kernel.getPtrZeKernelHandle(), dispatch, kernelEventTimer, 0, null);
        LevelZeroUtils.errorLog("zeCommandListAppendLaunchKernel", result);

        result = commandList.zeCommandListAppendBarrier(commandList.getCommandListHandlerPtr(), null, 0, null);
        LevelZeroUtils.errorLog("zeCommandListAppendBarrier", result);
    }

    @Override
    public int launchWithoutDependencies(CallStack stack, ObjectBuffer atomicSpace, TaskMetaData meta, long batchThreads) {
        SPIRVLevelZeroModule module = (SPIRVLevelZeroModule) spirvModule;
        LevelZeroKernel levelZeroKernel = module.getKernel();
        ZeKernelHandle kernel = levelZeroKernel.getKernelHandle();

        if (!stack.isOnDevice()) {
            setKernelArgs((SPIRVByteBuffer) stack, null, meta);
        }

        if (threadScheduling == null || dispatcher == null || meta.isWorkerGridAvailable()) {
            // if the worker grid is available, the user can update the number of threads to
            // run at any point during runtime.
            threadScheduling = calculateGlobalAndLocalBlockOfThreads(meta, batchThreads);
            dispatcher = suggestThreadSchedulingToLevelZeroDriver(threadScheduling, levelZeroKernel, kernel, meta);
        }

        if (meta.isThreadInfoEnabled()) {
            meta.printThreadDims();
        }

        launchKernelWithLevelZero(kernel, threadScheduling, dispatcher);

        if (TornadoOptions.isProfilerEnabled()) {
            kernelTimeStamp.solveEvent(meta);
        }

        return 0;
    }

    private void calculateLocalWork(TaskMetaData meta) {
        final long[] localWork = meta.initLocalWork();

        switch (meta.getDims()) {
            case 3:
                localWork[2] = 1;
                localWork[1] = calculateGroupSize(calculateEffectiveMaxWorkItemSizes(meta)[1], meta.getGlobalWork()[1]);
                localWork[0] = calculateGroupSize(calculateEffectiveMaxWorkItemSizes(meta)[0], meta.getGlobalWork()[0]);
                break;
            case 2:
                localWork[1] = calculateGroupSize(calculateEffectiveMaxWorkItemSizes(meta)[1], meta.getGlobalWork()[1]);
                localWork[0] = calculateGroupSize(calculateEffectiveMaxWorkItemSizes(meta)[0], meta.getGlobalWork()[0]);
                break;
            case 1:
                localWork[0] = calculateGroupSize(calculateEffectiveMaxWorkItemSizes(meta)[0], meta.getGlobalWork()[0]);
                break;
            default:
                break;
        }
    }

    private int calculateGroupSize(long maxBlockSize, long globalWorkSize) {
        if (maxBlockSize == globalWorkSize) {
            maxBlockSize /= 4;
        }

        int value = (int) Math.min(maxBlockSize, globalWorkSize);
        if (value == 0) {
            return 1;
        }
        while (globalWorkSize % value != 0) {
            value--;
        }
        return value;
    }

    private long[] calculateEffectiveMaxWorkItemSizes(TaskMetaData metaData) {
        long[] intermediates = new long[] { 1, 1, 1 };

        long[] maxWorkItemSizes = deviceContext.getDevice().getDeviceMaxWorkItemSizes();

        switch (metaData.getDims()) {
            case 3:
                intermediates[2] = (long) Math.sqrt(maxWorkItemSizes[2]);
                intermediates[1] = (long) Math.sqrt(maxWorkItemSizes[1]);
                intermediates[0] = (long) Math.sqrt(maxWorkItemSizes[0]);
                break;
            case 2:
                intermediates[1] = (long) Math.sqrt(maxWorkItemSizes[1]);
                intermediates[0] = (long) Math.sqrt(maxWorkItemSizes[0]);
                break;
            case 1:
                intermediates[0] = maxWorkItemSizes[0];
                break;
            default:
                break;

        }
        return intermediates;
    }

    private void calculateGlobalWork(TaskMetaData meta, long batchThreads) {
        final long[] globalWork = meta.getGlobalWork();

        for (int i = 0; i < meta.getDims(); i++) {
            long value = (batchThreads <= 0) ? (long) (meta.getDomain().get(i).cardinality()) : batchThreads;
            if (ADJUST_IRREGULAR && (value % WARP_SIZE != 0)) {
                value = ((value / WARP_SIZE) + 1) * WARP_SIZE;
            }
            globalWork[i] = value;
        }
    }

    private void checkLocalWorkGroupFitsOnDevice(final TaskMetaData meta) {
        WorkerGrid grid = meta.getWorkerGrid(meta.getId());
        long[] local = grid.getLocalWork();
        if (local != null) {
            LevelZeroGridInfo gridInfo = new LevelZeroGridInfo(deviceContext, local);
            boolean checkedDimensions = gridInfo.checkGridDimensions();
            if (!checkedDimensions) {
                System.out.println(WARNING_THREAD_LOCAL);
                grid.setLocalWorkToNull();
                grid.setNumberOfWorkgroupsToNull();
            }
        }
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void invalidate() {
        valid = false;
    }
}
