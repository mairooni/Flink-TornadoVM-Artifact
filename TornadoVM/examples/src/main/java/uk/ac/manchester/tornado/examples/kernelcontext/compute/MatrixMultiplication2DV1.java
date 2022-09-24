/*
 * Copyright (c) 2021, APT Group, Department of Computer Science,
 * The University of Manchester.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package uk.ac.manchester.tornado.examples.kernelcontext.compute;

import java.util.stream.IntStream;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid2D;

/**
 * Example of Matrix Multiplication for square matrices written in Java. This
 * implementation follows the OpenCL implementation description provided in
 * https://github.com/cnugteren/myGEMM.
 *
 * In detail, it applies the following optimization: (i) Thread attributes to
 * utilize two dimensions.
 *
 * How to run:
 *
 * <code>
 *     $ tornado --debug uk.ac.manchester.tornado.examples.kernelcontext.compute.MatrixMultiplication2DV1
 * </code>
 */
public class MatrixMultiplication2DV1 {

    private static final int WARMING_UP_ITERATIONS = 15;

    public static void matrixMultiplication(KernelContext context, final float[] A, final float[] B, final float[] C, final int size) {
        int globalRow = context.globalIdx;
        int globalCol = context.globalIdy;
        float sum = 0;

        for (int k = 0; k < size; k++) {
            sum += A[(k * size) + globalRow] * B[(globalCol * size) + k];
        }
        C[(globalCol * size) + globalRow] = sum;
    }

    private static void matrixMultiplication(final float[] A, final float[] B, final float[] C, final int size) {
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                float sum = 0.0f;
                for (int k = 0; k < size; k++) {
                    sum += A[(i * size) + k] * B[(k * size) + j];
                }
                C[(i * size) + j] = sum;
            }
        }
    }

    public static void main(String[] args) {

        int size = 512;
        if (args.length >= 1) {
            try {
                size = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignored) {
            }
        }

        System.out.println("Computing MxM of " + size + "x" + size);

        float[] matrixA = new float[size * size];
        float[] matrixB = new float[size * size];
        float[] matrixC = new float[size * size];
        float[] resultSeq = new float[size * size];

        IntStream.range(0, size * size).parallel().forEach(idx -> {
            matrixA[idx] = 2.5f;
            matrixB[idx] = 3.5f;
        });

        WorkerGrid workerGrid = new WorkerGrid2D(size, size);
        GridScheduler gridScheduler = new GridScheduler("s0.t0", workerGrid);
        KernelContext context = new KernelContext();
        // The local work group is configured to be 32x32
        workerGrid.setLocalWork(32, 32, 1);

        //@formatter:off        
        TaskSchedule t = new TaskSchedule("s0") //
                .task("t0", MatrixMultiplication2DV1::matrixMultiplication, context, matrixA, matrixB, matrixC, size) //
                .streamOut(matrixC);
        //@formatter:on

        // 1. Warm up Tornado
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            t.execute(gridScheduler);
        }

        // 2. Run parallel on the GPU with Tornado
        long start = System.currentTimeMillis();
        t.execute(gridScheduler);
        long end = System.currentTimeMillis();

        // Run sequential
        // 1. Warm up sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            matrixMultiplication(matrixA, matrixB, resultSeq, size);
        }

        // 2. Run the sequential code
        long startSequential = System.currentTimeMillis();
        matrixMultiplication(matrixA, matrixB, resultSeq, size);
        long endSequential = System.currentTimeMillis();

        // Compute GigaFlops and performance
        long msecTornadoVMElapsedTime = (end - start);
        long msecSequentialElaptedTime = (endSequential - startSequential);
        double flops = 2 * Math.pow(size, 3);
        double tornadoVMGigaFlops = (1.0E-9 * flops) / (msecTornadoVMElapsedTime / 1000.0f);
        double sequentialGigaFlops = (1.0E-9 * flops) / (msecSequentialElaptedTime / 1000.0f);
        double speedup = (double) (endSequential - startSequential) / (double) (end - start);

        String formatTornadoVMGFlops = String.format("%.2f", tornadoVMGigaFlops);
        String formatSequentialGFlops = String.format("%.2f", sequentialGigaFlops);

        System.out.println("\tSequential Execution: " + formatSequentialGFlops + " GFlops, Total time = " + (endSequential - startSequential) + " ms");
        System.out.println("\tTornadoVM Execution: " + formatTornadoVMGFlops + " GFlops, Total Time = " + (end - start) + " ms");
        System.out.println("\tSpeedup: " + speedup + "x");
        System.out.println("\tVerification " + verify(matrixC, resultSeq, size));
    }

    private static boolean verify(float[] par, float[] seq, int size) {
        boolean check = true;
        for (int i = 0; i < size * size; i++) {
            if (Math.abs(par[i] - seq[i]) > 0.01f) {
                check = false;
                break;
            }
        }
        return check;
    }
}
