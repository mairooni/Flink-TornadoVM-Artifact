/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
 * Copyright (c) 2018, 2019 APT Group, School of Computer Science,
 * The University of Manchester. All rights reserved.
 * Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
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
 * Authors: Juan Fumero
 *
 */
package uk.ac.manchester.tornado.drivers.opencl.graal.snippets;

import jdk.vm.ci.meta.RawConstant;
import org.graalvm.compiler.api.replacements.Snippet;
import org.graalvm.compiler.api.replacements.SnippetReflectionProvider;
import org.graalvm.compiler.core.common.type.StampFactory;
import org.graalvm.compiler.debug.DebugHandlersFactory;
import org.graalvm.compiler.graph.Node;
import org.graalvm.compiler.nodes.ConstantNode;
import org.graalvm.compiler.nodes.StructuredGraph;
import org.graalvm.compiler.nodes.ValueNode;
import org.graalvm.compiler.nodes.java.LoadIndexedNode;
import org.graalvm.compiler.nodes.java.NewArrayNode;
import org.graalvm.compiler.nodes.spi.LoweringTool;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.util.Providers;
import org.graalvm.compiler.replacements.SnippetTemplate;
import org.graalvm.compiler.replacements.SnippetTemplate.AbstractTemplates;
import org.graalvm.compiler.replacements.SnippetTemplate.Arguments;
import org.graalvm.compiler.replacements.SnippetTemplate.SnippetInfo;
import org.graalvm.compiler.replacements.Snippets;

import jdk.vm.ci.code.TargetDescription;
import jdk.vm.ci.meta.JavaKind;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.drivers.opencl.builtins.OpenCLIntrinsics;
import uk.ac.manchester.tornado.drivers.opencl.graal.nodes.GlobalThreadSizeNode;
import uk.ac.manchester.tornado.drivers.opencl.graal.nodes.OCLFPBinaryIntrinsicNode;
import uk.ac.manchester.tornado.drivers.opencl.graal.nodes.OCLIntBinaryIntrinsicNode;
import uk.ac.manchester.tornado.runtime.FlinkCompilerInfoIntermediate;
import uk.ac.manchester.tornado.runtime.graal.nodes.TornadoReduceAddNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.TornadoReduceAverageNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.TornadoReduceMulNode;
import uk.ac.manchester.tornado.runtime.graal.nodes.StoreAtomicIndexedNode;

/**
 * Tornado-Graal snippets for GPUs reductions using OpenCL semantics.
 */
public class ReduceGPUSnippets implements Snippets {

    /**
     * Dummy value for local memory allocation. The actual value to be allocated is
     * replaced in later stages of the JIT compiler.
     */
    private static int LOCAL_WORK_GROUP_SIZE = 223;

    @Snippet
    public static void partialReduceIntAdd(int[] inputArray, int[] outputArray, int gidx) {
        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntAddCarrierValue(int[] inputArray, int[] outputArray, int gidx, int value) {

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        localArray[localIdx] = value;

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }
        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongAdd(long[] inputArray, long[] outputArray, int gidx) {
        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            // here
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongAddFlink(long[] inputArray, long[] outputArray, int gidx) {
        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongAddCarrierValue(long[] inputArray, long[] outputArray, int gidx, long value) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int myID = localIdx + (localGroupSize * groupID);

        inputArray[myID] = value;

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                inputArray[myID] += inputArray[myID + stride];

            }
        }
        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = inputArray[myID];
        }
    }

    @Snippet
    public static void partialReduceFloatAdd(float[] inputArray, float[] outputArray, int gidx) {
        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatAddCarrierValue(float[] inputArray, float[] outputArray, int gidx, float value) {

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int myID = localIdx + (localGroupSize * groupID);
        localArray[localIdx] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleAdd(double[] inputArray, double[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleAddCarrierValue(double[] inputArray, double[] outputArray, int gidx, double value) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    // ------------- Tuple2 --------------------
    @Snippet
    public static void partialReduceTuple2IntAdd(int[] inputArray, int[] outputArray, int gidx, int tupleFieldNum) {
        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);
        int field = inputArray[tupleFieldNum + (2 * gidx)];
        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + 2 * groupID] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple4IntAdd(int[] inputArray, int[] outputArray, int gidx, int tupleFieldNum) {
        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);
        int field = inputArray[tupleFieldNum + (4 * gidx)];
        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + 4 * groupID] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple3NestedLongField(long[] inputArray, long[] outputArray, int gidx, int tupleFieldNum) {
        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long field = inputArray[tupleFieldNum + (4 * gidx)];

        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + (4 * groupID)] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple3NestedDoubleField(double[] inputArray, double[] outputArray, int gidx, int tupleFieldNum) {
        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double field = inputArray[tupleFieldNum + (4 * gidx)];

        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + (4 * groupID)] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple4DoubleMin(double[] inputArray, double[] outputArray, int gidx, int tupleFieldNum) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double field = inputArray[tupleFieldNum + (4 * gidx)];
        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + (4 * groupID)] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple4DoubleMax(double[] inputArray, double[] outputArray, int gidx, int tupleFieldNum) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double field = inputArray[tupleFieldNum + (4 * gidx)];
        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + (4 * groupID)] = localArray[localIdx];
        }
    }

    @Snippet
    public static void partialReduceTuple4DoubleAdd(double[] inputArray, double[] outputArray, int gidx, int tupleFieldNum) {
        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);
        double field = inputArray[tupleFieldNum + (4 * gidx)];
        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + 4 * groupID] = localArray[localIdx];
        }
    }

    // array field
    @Snippet
    public static void partialReduceTuple3Array(int[] inputArray, int[] outputArray, int gidx, int tupleFieldNum) {

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        if (gidx == 0) {
            double in;
            for (int i = 0; i < 83; i++) {
                in = inputArray[(8 * i)];
                outputArray[(8 * i)] = (int) in;
            }
            int f1 = inputArray[83 + (85 * gidx)];
            outputArray[83 + (85 * gidx)] = f1;
        }

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);
        int field = inputArray[84 + (85 * gidx)];
        localArray[localIdx] = field;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] += localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[84 + 85 * groupID] = localArray[localIdx];
        }
    }

    // avg
    @Snippet
    public static void partialReduceTuple4Avg(double[] inputArray, double[] outputArray, int gidx, int tupleFieldNum, int tupleFieldNum2) {
        double[] localArray1 = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);
        long[] localArray2 = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);
        double field1 = inputArray[tupleFieldNum + (4 * gidx)];
        localArray1[localIdx] = field1;
        long field2 = (long) inputArray[tupleFieldNum2 + (4 * gidx)];
        localArray2[localIdx] = field2;
        OpenCLIntrinsics.localBarrier();

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray1[localIdx] += (localArray1[localIdx + stride] * localArray2[localIdx + stride]);
                localArray2[localIdx] += localArray2[localIdx];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[tupleFieldNum + 4 * groupID] = (localArray1[localIdx] / localArray2[localIdx]);
        }
    }

    // ------------------------------
    @Snippet
    public static void partialReduceIntMult(int[] inputArray, int[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntMultCarrierValue(int[] inputArray, int[] outputArray, int gidx, int value) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMult(long[] inputArray, long[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMultCarrierValue(long[] inputArray, long[] outputArray, int gidx, long value) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int myID = localIdx + (localGroupSize * groupID);

        inputArray[myID] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                inputArray[myID] *= inputArray[myID + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = inputArray[myID];
        }
    }

    @Snippet
    public static void partialReduceFloatMult(float[] inputArray, float[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatMultCarrierValue(float[] inputArray, float[] outputArray, int gidx, float value) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];

            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMult(double[] inputArray, double[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMultCarrierValue(double[] inputArray, double[] outputArray, int gidx, double value) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = value;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] *= localArray[localIdx + stride];

            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntMax(int[] inputArray, int[] outputArray, int gidx) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntMaxCarrierValue(int[] inputArray, int[] outputArray, int gidx, int extra) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMax(long[] inputArray, long[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMaxCarrierValue(long[] inputArray, long[] outputArray, int gidx, long extra) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatMax(float[] inputArray, float[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatMaxCarrierValue(float[] inputArray, float[] outputArray, int gidx, float extra) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMax(double[] inputArray, double[] outputArray, int gidx) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMaxCarrierValue(double[] inputArray, double[] outputArray, int gidx, double extra) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.max(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntMin(int[] inputArray, int[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceIntMinCarrierValue(int[] inputArray, int[] outputArray, int gidx, int extra) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        int[] localArray = (int[]) NewArrayNode.newUninitializedArray(int.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMin(long[] inputArray, long[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceLongMinCarrierValue(long[] inputArray, long[] outputArray, int gidx, long extra) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        long[] localArray = (long[]) NewArrayNode.newUninitializedArray(long.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;

        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatMin(float[] inputArray, float[] outputArray, int gidx) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceFloatMinCarrierValue(float[] inputArray, float[] outputArray, int gidx, float extra) {

        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        float[] localArray = (float[]) NewArrayNode.newUninitializedArray(float.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMin(double[] inputArray, double[] outputArray, int gidx) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = inputArray[gidx];
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    @Snippet
    public static void partialReduceDoubleMinCarrierValue(double[] inputArray, double[] outputArray, int gidx, double extra) {
        int localIdx = OpenCLIntrinsics.get_local_id(0);
        int localGroupSize = OpenCLIntrinsics.get_local_size(0);
        int groupID = OpenCLIntrinsics.get_group_id(0);

        double[] localArray = (double[]) NewArrayNode.newUninitializedArray(double.class, LOCAL_WORK_GROUP_SIZE);

        localArray[localIdx] = extra;
        for (int stride = (localGroupSize / 2); stride > 0; stride /= 2) {
            OpenCLIntrinsics.localBarrier();
            if (localIdx < stride) {
                localArray[localIdx] = TornadoMath.min(localArray[localIdx], localArray[localIdx + stride]);
            }
        }

        OpenCLIntrinsics.globalBarrier();
        if (localIdx == 0) {
            outputArray[groupID + 1] = localArray[0];
        }
    }

    public static class Templates extends AbstractTemplates implements TornadoSnippetTypeInference {

        // Add
        private final SnippetInfo partialReduceIntSnippet = snippet(ReduceGPUSnippets.class, "partialReduceIntAdd");
        private final SnippetInfo partialReduceIntSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceIntAddCarrierValue");
        private final SnippetInfo partialReduceLongSnippet = snippet(ReduceGPUSnippets.class, "partialReduceLongAdd");
        private final SnippetInfo partialReduceLongSnippetFlink = snippet(ReduceGPUSnippets.class, "partialReduceLongAddFlink");
        private final SnippetInfo partialReduceLongSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceLongAddCarrierValue");
        private final SnippetInfo partialReduceAddFloatSnippet = snippet(ReduceGPUSnippets.class, "partialReduceFloatAdd");
        private final SnippetInfo partialReduceAddFloatSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceFloatAddCarrierValue");
        private final SnippetInfo partialReduceAddDoubleSnippet = snippet(ReduceGPUSnippets.class, "partialReduceDoubleAdd");
        private final SnippetInfo partialReduceAddDoubleSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceDoubleAddCarrierValue");
        private final SnippetInfo partialReduceTuple2IntSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple2IntAdd");
        private final SnippetInfo partialReduceTuple4IntSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple4IntAdd");
        private final SnippetInfo partialReduceTuple3NestedDoubleFieldSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple3NestedDoubleField");
        private final SnippetInfo partialReduceTuple3NestedLongFieldSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple3NestedLongField");
        private final SnippetInfo partialReduceTuple4DoubleSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple4DoubleAdd");
        private final SnippetInfo partialReduceTuple3ArraySnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple3Array");

        // Mul
        private final SnippetInfo partialReduceIntMultSnippet = snippet(ReduceGPUSnippets.class, "partialReduceIntMult");
        private final SnippetInfo partialReduceIntMultSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceIntMultCarrierValue");
        private final SnippetInfo partialReduceLongMultSnippet = snippet(ReduceGPUSnippets.class, "partialReduceLongMult");
        private final SnippetInfo partialReduceLongMultSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceLongMultCarrierValue");
        private final SnippetInfo partialReduceFloatMultSnippet = snippet(ReduceGPUSnippets.class, "partialReduceFloatMult");
        private final SnippetInfo partialReduceFloatMultSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceFloatMultCarrierValue");
        private final SnippetInfo partialReduceDoubleMultSnippet = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMult");
        private final SnippetInfo partialReduceDoubleMultSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMultCarrierValue");

        // Max
        private final SnippetInfo partialReduceIntMaxSnippet = snippet(ReduceGPUSnippets.class, "partialReduceIntMax");
        private final SnippetInfo partialReduceIntMaxSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceIntMaxCarrierValue");
        private final SnippetInfo partialReduceLongMaxSnippet = snippet(ReduceGPUSnippets.class, "partialReduceLongMax");
        private final SnippetInfo partialReduceLongMaxSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceLongMaxCarrierValue");
        private final SnippetInfo partialReduceMaxFloatSnippet = snippet(ReduceGPUSnippets.class, "partialReduceFloatMax");
        private final SnippetInfo partialReduceMaxFloatSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceFloatMaxCarrierValue");
        private final SnippetInfo partialReduceMaxDoubleSnippet = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMax");
        private final SnippetInfo partialReduceMaxDoubleSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMaxCarrierValue");
        private final SnippetInfo partialReduceTuple4DoubleMaxSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple4DoubleMax");

        // Min
        private final SnippetInfo partialReduceIntMinSnippet = snippet(ReduceGPUSnippets.class, "partialReduceIntMin");
        private final SnippetInfo partialReduceIntMinSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceIntMinCarrierValue");
        private final SnippetInfo partialReduceLongMinSnippet = snippet(ReduceGPUSnippets.class, "partialReduceLongMin");
        private final SnippetInfo partialReduceLongMinSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceLongMinCarrierValue");
        private final SnippetInfo partialReduceMinFloatSnippet = snippet(ReduceGPUSnippets.class, "partialReduceFloatMin");
        private final SnippetInfo partialReduceMinFloatSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceFloatMinCarrierValue");
        private final SnippetInfo partialReduceMinDoubleSnippet = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMin");
        private final SnippetInfo partialReduceMinDoubleSnippetCarrierValue = snippet(ReduceGPUSnippets.class, "partialReduceDoubleMinCarrierValue");
        private final SnippetInfo partialReduceTuple4DoubleMinSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple4DoubleMin");

        // avg
        private final SnippetInfo partialReduceTuple4AvgSnippet = snippet(ReduceGPUSnippets.class, "partialReduceTuple4Avg");

        public Templates(OptionValues options, Iterable<DebugHandlersFactory> debugHandlersFactories, Providers providers, SnippetReflectionProvider snippetReflection, TargetDescription target) {
            super(options, debugHandlersFactories, providers, snippetReflection, target);
        }

        private SnippetInfo getSnippetFromOCLBinaryNodeInteger(OCLIntBinaryIntrinsicNode value, ValueNode extra) {
            switch (value.operation()) {
                case MAX:
                    return (extra == null) ? partialReduceIntMaxSnippet : partialReduceIntMaxSnippetCarrierValue;
                case MIN:
                    return (extra == null) ? partialReduceIntMinSnippet : partialReduceIntMinSnippetCarrierValue;
                default:
                    throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
        }

        private SnippetInfo getSnippetFromOCLBinaryNodeLong(OCLIntBinaryIntrinsicNode value, ValueNode extra) {
            switch (value.operation()) {
                case MAX:
                    return (extra == null) ? partialReduceLongMaxSnippet : partialReduceLongMaxSnippetCarrierValue;
                case MIN:
                    return (extra == null) ? partialReduceLongMinSnippet : partialReduceLongMinSnippetCarrierValue;
                default:
                    throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
        }

        @Override
        public SnippetInfo inferIntSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet;
            if (value instanceof TornadoReduceAddNode) {
                snippet = (extra == null) ? partialReduceIntSnippet : partialReduceIntSnippetCarrierValue;
            } else if (value instanceof TornadoReduceMulNode) {
                // operation = ATOMIC_OPERATION.MUL;
                snippet = (extra == null) ? partialReduceIntMultSnippet : partialReduceIntMultSnippetCarrierValue;
            } else if (value instanceof OCLIntBinaryIntrinsicNode) {
                OCLIntBinaryIntrinsicNode op = (OCLIntBinaryIntrinsicNode) value;
                snippet = getSnippetFromOCLBinaryNodeInteger(op, extra);
            } else {
                throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
            return snippet;
        }

        @Override
        public SnippetInfo inferLongSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet;

            if (value instanceof TornadoReduceAddNode) {
                snippet = (extra == null) ? partialReduceLongSnippet : partialReduceLongSnippetCarrierValue;
            } else if (value instanceof TornadoReduceMulNode) {
                snippet = (extra == null) ? partialReduceLongMultSnippet : partialReduceLongMultSnippetCarrierValue;
            } else if (value instanceof OCLIntBinaryIntrinsicNode) {
                OCLIntBinaryIntrinsicNode op = (OCLIntBinaryIntrinsicNode) value;
                snippet = getSnippetFromOCLBinaryNodeLong(op, extra);
            } else {
                throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
            return snippet;
        }

        private SnippetInfo getSnippetFromOCLBinaryNodeInteger(OCLFPBinaryIntrinsicNode value, ValueNode extra) {
            switch (value.operation()) {
                case FMAX:
                    return extra == null ? partialReduceMaxFloatSnippet : partialReduceMaxFloatSnippetCarrierValue;
                case FMIN:
                    return extra == null ? partialReduceMinFloatSnippet : partialReduceMinFloatSnippetCarrierValue;
                default:
                    throw new RuntimeException("OCLFPBinaryIntrinsicNode operation not supported yet");
            }
        }

        @Override
        public SnippetInfo inferFloatSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet;
            if (value instanceof TornadoReduceAddNode) {
                snippet = (extra == null) ? partialReduceAddFloatSnippet : partialReduceAddFloatSnippetCarrierValue;
            } else if (value instanceof TornadoReduceMulNode) {
                snippet = (extra == null) ? partialReduceFloatMultSnippet : partialReduceFloatMultSnippetCarrierValue;
            } else if (value instanceof OCLFPBinaryIntrinsicNode) {
                snippet = getSnippetFromOCLBinaryNodeInteger((OCLFPBinaryIntrinsicNode) value, extra);
            } else {
                throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
            return snippet;
        }

        public SnippetInfo inferTuple2IntSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = partialReduceTuple2IntSnippet;
            }
            return snippet;
        }

        public SnippetInfo inferTuple4IntSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = partialReduceTuple4IntSnippet;
            }
            return snippet;
        }

        public SnippetInfo inferTuple4DoubleSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = partialReduceTuple4DoubleSnippet;
            }
            return snippet;
        }

        public SnippetInfo inferTuple3NestedDoubleSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = partialReduceTuple3NestedDoubleFieldSnippet;
            }
            return snippet;
        }

        public SnippetInfo inferTuple3NestedLongSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = partialReduceTuple3NestedLongFieldSnippet;
            }
            return snippet;
        }

        public SnippetInfo inferTuple4AvgSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = partialReduceTuple4AvgSnippet;
            return snippet;
        }

        public SnippetInfo inferTuple3ArraySnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = partialReduceTuple3ArraySnippet;
            return snippet;
        }

        private SnippetInfo getSnippetFromOCLBinaryNodeDouble(OCLFPBinaryIntrinsicNode value, ValueNode extra) {
            switch (value.operation()) {
                case FMAX:
                    return extra == null ? partialReduceMaxDoubleSnippet : partialReduceMaxDoubleSnippetCarrierValue;
                case FMIN:
                    return extra == null ? partialReduceMinDoubleSnippet : partialReduceMinDoubleSnippetCarrierValue;
                default:
                    throw new RuntimeException("OCLFPBinaryIntrinsicNode operation not supported yet");
            }
        }

        @Override
        public SnippetInfo inferDoubleSnippet(ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            if (value instanceof TornadoReduceAddNode) {
                snippet = (extra == null) ? partialReduceAddDoubleSnippet : partialReduceAddDoubleSnippet;
            } else if (value instanceof TornadoReduceMulNode) {
                snippet = (extra == null) ? partialReduceDoubleMultSnippet : partialReduceDoubleMultSnippetCarrierValue;
            } else if (value instanceof OCLFPBinaryIntrinsicNode) {
                snippet = getSnippetFromOCLBinaryNodeDouble((OCLFPBinaryIntrinsicNode) value, extra);
            } else {
                throw new RuntimeException("Reduce Operation no supported yet: snippet not installed");
            }
            return snippet;
        }

        @Override
        public SnippetInfo getSnippetInstance(JavaKind elementKind, ValueNode value, ValueNode extra) {
            SnippetInfo snippet = null;
            FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
            if (fcomp != null && fcomp.getHasTuples()) {
                // if there is no array field
                if (!fcomp.getArrayField()) {
                    // Tuple2
                    if (fcomp.getFieldTypes().size() == 2) {
                        // if both fields are integers
                        if (fcomp.getFieldTypes().get(0).equals("int") && fcomp.getFieldTypes().get(1).equals("int")) {
                            snippet = inferTuple2IntSnippet(value, extra);
                        }
                    } else if (fcomp.getFieldTypes().size() == 4) {
                        if (value instanceof OCLFPBinaryIntrinsicNode) {
                            OCLFPBinaryIntrinsicNode op = (OCLFPBinaryIntrinsicNode) value;
                            switch (op.operation()) {
                                case FMIN:
                                    return partialReduceTuple4DoubleMinSnippet;
                                case FMAX:
                                    return partialReduceTuple4DoubleMaxSnippet;
                                default:
                                    System.out.println("No fmin? Hmmm");

                            }
                        } else if (value instanceof TornadoReduceAverageNode) {
                            // System.out.println("AVG lowering snippet!!");
                            snippet = inferTuple4AvgSnippet(value, extra);
                        } else {
                            if (fcomp.getFieldTypes().get(0).equals("int") && fcomp.getFieldTypes().get(1).equals("int") && fcomp.getFieldTypes().get(2).equals("int")
                                    && fcomp.getFieldTypes().get(3).equals("int")) {
                                snippet = inferTuple4IntSnippet(value, extra);
                            } else if (fcomp.getNestedTuples()) {
                                if (elementKind == JavaKind.Double) {
                                    snippet = inferTuple3NestedDoubleSnippet(value, extra);
                                } else if (elementKind == JavaKind.Long) {
                                    snippet = inferTuple3NestedLongSnippet(value, extra);
                                }
                            } else {
                                snippet = inferTuple4DoubleSnippet(value, extra);
                            }
                        }
                    }
                } else {
                    snippet = inferTuple3ArraySnippet(value, extra);
                }
            } else if (elementKind == JavaKind.Int) {
                snippet = inferIntSnippet(value, extra);
            } else if (elementKind == JavaKind.Long) {
                snippet = inferLongSnippet(value, extra);
            } else if (elementKind == JavaKind.Float) {
                snippet = inferFloatSnippet(value, extra);
            } else if (elementKind == JavaKind.Double) {
                snippet = inferDoubleSnippet(value, extra);
            }
            return snippet;
        }

        public void lower(StoreAtomicIndexedNode storeAtomicIndexed, ValueNode globalId, GlobalThreadSizeNode globalSize, LoweringTool tool) {
            // System.out.println(">> made it to lowering!!");
            StructuredGraph graph = storeAtomicIndexed.graph();
            JavaKind elementKind = storeAtomicIndexed.elementKind();
            ValueNode value = storeAtomicIndexed.value();
            ValueNode extra = storeAtomicIndexed.getExtraOperation();
            FlinkCompilerInfo fcomp = FlinkCompilerInfoIntermediate.getFlinkCompilerInfo();
            SnippetInfo snippet = getSnippetInstance(elementKind, value, extra);

            // Sets the guard stage to AFTER_FSA because we want to avoid any frame state
            // assignment for the snippet (see SnippetTemplate::assignNecessaryFrameStates)
            // This is needed because we have nodes in the snippet which have multiple side
            // effects and this is not allowed (see
            // SnippetFrameStateAssignment.NodeStateAssignment.INVALID)
            Arguments args = new Arguments(snippet, StructuredGraph.GuardsStage.AFTER_FSA, tool.getLoweringStage());
            args.add("inputData", storeAtomicIndexed.getInputArray());
            args.add("outputArray", storeAtomicIndexed.array());
            args.add("gidx", globalId);
            boolean isAvg = false;
            if (storeAtomicIndexed.inputs().filter(TornadoReduceAverageNode.class).isNotEmpty()) {
                isAvg = true;
                args.add("tupleFieldNum", new ConstantNode(new RawConstant(1), StampFactory.positiveInt()));
                args.add("tupleFieldNum2", new ConstantNode(new RawConstant(3), StampFactory.positiveInt()));
                // TornadoReduceAverageNode avg =
                // storeAtomicIndexed.inputs().filter(TornadoReduceAverageNode.class).first();
                // int field1 = -1;
                // int field2 = -1;
                // for (LoadIndexedNode ld : avg.inputs().filter(LoadIndexedNode.class)) {
                // if (field1 == -1) {
                // Node idx = ld.index();
                // if (idx instanceof ConstantNode) {
                // ConstantNode c = (ConstantNode) idx;
                //
                // }
                // } else {
                //
                // }
                // }

            }

            if (fcomp != null && fcomp.getHasTuples() && !isAvg) {
                args.add("tupleFieldNum", storeAtomicIndexed.index());
            }
            if (extra != null) {
                args.add("value", extra);
            }

            SnippetTemplate template = template(storeAtomicIndexed, args);
            template.instantiate(providers.getMetaAccess(), storeAtomicIndexed, SnippetTemplate.DEFAULT_REPLACER, args);
        }
    }
}
