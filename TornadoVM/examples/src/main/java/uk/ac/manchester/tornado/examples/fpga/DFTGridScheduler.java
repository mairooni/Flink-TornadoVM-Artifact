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

package uk.ac.manchester.tornado.examples.fpga;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

public class DFTGridScheduler {

    private static boolean CHECK_RESULT = true;

    private static void computeDft(float[] inreal, float[] inimag, float[] outreal, float[] outimag, int[] inputSize) {
        int n = inreal.length;
        for (@Parallel int k = 0; k < n; k++) { // For each output element
            float sumreal = 0;
            float sumimag = 0;
            for (int t = 0; t < n; t++) { // For each input element
                float angle = ((2 * TornadoMath.floatPI() * t * k) / (float) n);
                sumreal += (inreal[t] * (TornadoMath.floatCos(angle)) + inimag[t] * (TornadoMath.floatSin(angle)));
                sumimag += -(inreal[t] * (TornadoMath.floatSin(angle)) + inimag[t] * (TornadoMath.floatCos(angle)));
            }
            outreal[k] = sumreal;
            outimag[k] = sumimag;
        }
    }

    private static boolean validate(int size, float[] inReal, float[] inImag, float[] outReal, float[] outImag, int[] inputSize) {
        boolean val = true;
        float[] outRealTor = new float[size];
        float[] outImagTor = new float[size];

        computeDft(inReal, inImag, outRealTor, outImagTor, inputSize);

        for (int i = 0; i < size; i++) {
            if (Math.abs(outImagTor[i] - outImag[i]) > 0.1) {
                System.out.println(outImagTor[i] + " vs " + outImag[i] + "\n");
                val = false;
                break;
            }
            if (Math.abs(outReal[i] - outRealTor[i]) > 0.1) {
                System.out.println(outReal[i] + " vs " + outRealTor[i] + "\n");
                val = false;
                break;
            }
        }
        System.out.println("Is valid?: " + val + "\n");
        return val;
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: <size> <iterations>");
            System.exit(-1);
        }

        final int size = Integer.parseInt(args[0]);
        int iterations = Integer.parseInt(args[1]);

        long end,start;

        TaskSchedule graph;
        float[] inReal;
        float[] inImag;
        float[] outReal;
        float[] outImag;
        int[] inputSize;

        inReal = new float[size];
        inImag = new float[size];
        outReal = new float[size];
        outImag = new float[size];
        inputSize = new int[1];

        inputSize[0] = size;

        for (int i = 0; i < size; i++) {
            inReal[i] = 1 / (float) (i + 2);
            inImag[i] = 1 / (float) (i + 2);
        }

        WorkerGrid worker = new WorkerGrid1D(size);
        GridScheduler gridScheduler = new GridScheduler();
        gridScheduler.setWorkerGrid("s0.t0", worker);
        worker.setLocalWork(size / 4, 1, 1);

        long startInit = System.nanoTime();
        graph = new TaskSchedule("s0");
        graph.task("t0", DFTGridScheduler::computeDft, inReal, inImag, outReal, outImag, inputSize).streamOut(outReal, outImag);
        long stopInit = System.nanoTime();

        System.out.println("Initialization time:  " + (stopInit - startInit) + " ns" + "\n");

        for (int i = 0; i < iterations; i++) {
            start = System.nanoTime();
            graph.execute(gridScheduler);
            end = System.nanoTime();
            System.out.println("Total time:  " + (end - start) + " ns" + " \n");
        }

        if (CHECK_RESULT) {
            if (validate(size, inReal, inImag, outReal, outImag, inputSize)) {
                System.out.println("Validation: " + "SUCCESS " + "\n");
            } else {
                System.out.println("Validation: " + " FAIL " + "\n");
            }
        }
    }
}