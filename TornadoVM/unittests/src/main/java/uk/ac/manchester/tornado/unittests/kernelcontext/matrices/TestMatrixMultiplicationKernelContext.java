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
package uk.ac.manchester.tornado.unittests.kernelcontext.matrices;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.stream.IntStream;

import org.junit.Test;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.WorkerGrid2D;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

/**
 * The unit-tests in this class implement the Matrix Multiplication to check the
 * functional operation of some {@link KernelContext} features, such as global
 * thread identifiers, local thread identifiers, barriers and allocation of
 * local memory.
 */
public class TestMatrixMultiplicationKernelContext extends TornadoTestBase {

    private static final int TS = 4;

    public static void matrixMultiplicationJava(float[] a, float[] b, float[] c, int size) {
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                float sum = 0.0f;
                for (int k = 0; k < size; k++) {
                    sum += a[i * size + k] * b[k * size + j];
                }
                c[i * size + j] = sum;
            }
        }
    }

    public static void matrixMultiplication1D(KernelContext context, float[] a, float[] b, float[] c, int size) {
        int idx = context.globalIdx;

        for (int jdx = 0; jdx < size; jdx++) {
            float sum = 0.0f;
            for (int k = 0; k < size; k++) {
                sum += a[(idx * size) + k] * b[(k * size) + jdx];
            }
            c[(idx * size) + jdx] = sum;
        }
    }

    @Test
    public void mxm1DKernelContext() {
        final int size = 16;
        float[] a = new float[size * size];
        float[] b = new float[size * size];
        float[] cJava = new float[size * size];
        float[] cTornado = new float[size * size];

        Random r = new Random();
        IntStream.range(0, size * size).forEach(i -> {
            a[i] = r.nextFloat();
            b[i] = r.nextFloat();
        });

        WorkerGrid worker = new WorkerGrid1D(size);
        GridScheduler gridScheduler = new GridScheduler("s0.t0", worker);
        KernelContext context = new KernelContext();

        TaskSchedule s0 = new TaskSchedule("s0") //
                .streamIn(a, b) //
                .task("t0", TestMatrixMultiplicationKernelContext::matrixMultiplication1D, context, a, b, cTornado, size) //
                .streamOut(cTornado);
        s0.execute(gridScheduler);

        matrixMultiplicationJava(a, b, cJava, size);

        for (int i = 0; i < size * size; i++) {
            assertEquals(cJava[i], cTornado[i], 0.01f);
        }
    }

    public static void matrixMultiplication2D01(KernelContext context, float[] a, float[] b, float[] c, int size) {
        int idx = context.globalIdx;
        int jdx = context.globalIdy;
        float sum = 0.0f;

        for (int k = 0; k < size; k++) {
            sum += a[(k * size) + idx] * b[(jdx * size) + k];
        }
        c[(idx * size) + jdx] = sum;
    }

    @Test
    public void mxm2DKernelContext01() {
        final int size = 16;
        float[] a = new float[size * size];
        float[] b = new float[size * size];
        float[] cJava = new float[size * size];
        float[] cTornado = new float[size * size];

        Random r = new Random();
        IntStream.range(0, size * size).forEach(i -> {
            a[i] = r.nextFloat();
            b[i] = r.nextFloat();
        });

        WorkerGrid worker = new WorkerGrid2D(size, size);
        GridScheduler gridScheduler = new GridScheduler();
        gridScheduler.setWorkerGrid("s0.t0", worker);
        KernelContext context = new KernelContext();

        TaskSchedule s0 = new TaskSchedule("s0") //
                .streamIn(a, b) //
                .task("t0", TestMatrixMultiplicationKernelContext::matrixMultiplication2D01, context, a, b, cTornado, size) //
                .streamOut(cTornado);
        s0.execute(gridScheduler);

        matrixMultiplicationJava(a, b, cJava, size);

        for (int i = 0; i < size * size; i++) {
            assertEquals(cJava[i], cTornado[i], 0.01f);
        }
    }

    public static void matrixMultiplication2D02(KernelContext context, final float[] A, final float[] B, final float[] C, final int size) {
        int row = context.localIdx;
        int col = context.localIdy;
        int globalRow = TS * context.groupIdx + row;
        int globalCol = TS * context.groupIdy + col;

        float[] aSub = context.allocateFloatLocalArray(TS * TS);
        float[] bSub = context.allocateFloatLocalArray(TS * TS);

        float sum = 0.0f;

        // Loop over all tiles
        int numTiles = size / TS;
        for (int tileIndex = 0; tileIndex < numTiles; tileIndex++) {

            // Load one tile of A and B into local memory
            int tiledRow = TS * tileIndex + row;
            int tiledCol = TS * tileIndex + col;
            aSub[col * TS + row] = A[tiledCol * size + globalRow];
            bSub[col * TS + row] = B[globalCol * size + tiledRow];

            // Synchronise to make sure the tile is loaded
            context.localBarrier();

            // Perform the computation for a single tile
            for (int k = 0; k < TS; k++) {
                sum += aSub[k * TS + row] * bSub[col * TS + k];
            }
            // Synchronise before loading the next tile
            context.localBarrier();
        }

        // Store the final result in C
        C[(globalCol * size) + globalRow] = sum;
    }

    @Test
    public void mxm2DKernelContext02() {
        final int size = 16;
        float[] a = new float[size * size];
        float[] b = new float[size * size];
        float[] cJava = new float[size * size];
        float[] cTornado = new float[size * size];

        Random r = new Random();
        IntStream.range(0, size * size).forEach(i -> {
            a[i] = r.nextFloat();
            b[i] = r.nextFloat();
        });

        WorkerGrid worker = new WorkerGrid2D(size, size);
        GridScheduler gridScheduler = new GridScheduler();
        gridScheduler.setWorkerGrid("s0.t0", worker);
        KernelContext context = new KernelContext();

        TaskSchedule s0 = new TaskSchedule("s0") //
                .streamIn(a, b) //
                .task("t0", TestMatrixMultiplicationKernelContext::matrixMultiplication2D02, context, a, b, cTornado, size) //
                .streamOut(cTornado);
        worker.setLocalWork(TS, TS, 1);
        s0.execute(gridScheduler);

        matrixMultiplicationJava(a, b, cJava, size);

        for (int i = 0; i < size * size; i++) {
            assertEquals(cJava[i], cTornado[i], 0.1f);
        }
    }
}