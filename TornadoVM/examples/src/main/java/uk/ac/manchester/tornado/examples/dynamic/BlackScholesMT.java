/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
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

package uk.ac.manchester.tornado.examples.dynamic;

import java.util.Random;

import uk.ac.manchester.tornado.api.Policy;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

/**
 * BlackScholes implementation adapted from AMD-OpenCL examples and Marawacc
 * compiler framework.
 */
public class BlackScholesMT {

    private static void blackScholesKernel(float[] input, float[] callResult, float[] putResult) {
        for (@Parallel int idx = 0; idx < callResult.length; idx++) {
            float rand = input[idx];
            final float S_LOWER_LIMIT = 10.0f;
            final float S_UPPER_LIMIT = 100.0f;
            final float K_LOWER_LIMIT = 10.0f;
            final float K_UPPER_LIMIT = 100.0f;
            final float T_LOWER_LIMIT = 1.0f;
            final float T_UPPER_LIMIT = 10.0f;
            final float R_LOWER_LIMIT = 0.01f;
            final float R_UPPER_LIMIT = 0.05f;
            final float SIGMA_LOWER_LIMIT = 0.01f;
            final float SIGMA_UPPER_LIMIT = 0.10f;
            final float S = S_LOWER_LIMIT * rand + S_UPPER_LIMIT * (1.0f - rand);
            final float K = K_LOWER_LIMIT * rand + K_UPPER_LIMIT * (1.0f - rand);
            final float T = T_LOWER_LIMIT * rand + T_UPPER_LIMIT * (1.0f - rand);
            final float r = R_LOWER_LIMIT * rand + R_UPPER_LIMIT * (1.0f - rand);
            final float v = SIGMA_LOWER_LIMIT * rand + SIGMA_UPPER_LIMIT * (1.0f - rand);

            float d1 = (float) ((float) (TornadoMath.log(S / K) + ((r + (v * v / 2)) * T)) / v * TornadoMath.sqrt(T));
            float d2 = (float) ((float) d1 - (v * TornadoMath.sqrt(T)));
            callResult[idx] = (float) (S * cnd(d1) - K * TornadoMath.exp(T * (-1) * r) * cnd(d2));
            putResult[idx] = (float) (K * TornadoMath.exp(T * -r) * cnd(-d2) - S * cnd(-d1));
        }
    }

    private static void blackScholesKernelThreaded(float[] input, float[] callResult, float[] putResult, int threads, Thread[] th) throws InterruptedException {
        int balk = callResult.length / threads;
        for (int i = 0; i < threads; i++) {
            final int current = i;
            int lowBound = current * balk;
            int upperBound = (current + 1) * balk;
            if(current==threads-1) {
                upperBound = callResult.length;
            }
            int finalUpperBound = upperBound;
            th[i] = new Thread(() -> {
                for (int idx = lowBound; idx < finalUpperBound; idx++) {
                    float rand = input[idx];
                    final float S_LOWER_LIMIT = 10.0f;
                    final float S_UPPER_LIMIT = 100.0f;
                    final float K_LOWER_LIMIT = 10.0f;
                    final float K_UPPER_LIMIT = 100.0f;
                    final float T_LOWER_LIMIT = 1.0f;
                    final float T_UPPER_LIMIT = 10.0f;
                    final float R_LOWER_LIMIT = 0.01f;
                    final float R_UPPER_LIMIT = 0.05f;
                    final float SIGMA_LOWER_LIMIT = 0.01f;
                    final float SIGMA_UPPER_LIMIT = 0.10f;
                    final float S = S_LOWER_LIMIT * rand + S_UPPER_LIMIT * (1.0f - rand);
                    final float K = K_LOWER_LIMIT * rand + K_UPPER_LIMIT * (1.0f - rand);
                    final float T = T_LOWER_LIMIT * rand + T_UPPER_LIMIT * (1.0f - rand);
                    final float r = R_LOWER_LIMIT * rand + R_UPPER_LIMIT * (1.0f - rand);
                    final float v = SIGMA_LOWER_LIMIT * rand + SIGMA_UPPER_LIMIT * (1.0f - rand);

                    float d1 = (float) ((Math.log(S / K) + ((r + (v * v / 2)) * T)) / v * Math.sqrt(T));
                    float d2 = (float) (d1 - (v * Math.sqrt(T)));
                    callResult[idx] = (float) (S * cnd(d1) - K * Math.exp(T * (-1) * r) * cnd(d2));
                    putResult[idx] = (float) (K * Math.exp(T * -r) * cnd(-d2) - S * cnd(-d1));
                }
            });
        }

        for (int i = 0; i < threads; i++) {
            th[i].start();
        }
        for (int i = 0; i < threads; i++) {
            th[i].join();
        }
    }

    private static float cnd(float X) {
        final float c1 = 0.319381530f;
        final float c2 = -0.356563782f;
        final float c3 = 1.781477937f;
        final float c4 = -1.821255978f;
        final float c5 = 1.330274429f;
        final float zero = 0.0f;
        final float one = 1.0f;
        final float two = 2.0f;
        final float temp4 = 0.2316419f;
        final float oneBySqrt2pi = 0.398942280f;
        float absX = TornadoMath.abs(X);
        float t = one / (one + temp4 * absX);
        float y = (float) (one - oneBySqrt2pi * TornadoMath.exp(-X * X / two) * t * (c1 + t * (c2 + t * (c3 + t * (c4 + t * c5)))));
        return (X < zero) ? (one - y) : y;
    }

    private static boolean checkResult(float[] call, float[] put, float[] callPrice, float[] putPrice) {
        double delta = 1.8;
        for (int i = 0; i < call.length; i++) {
            if (Math.abs(call[i] - callPrice[i]) > delta) {
                System.out.println("call: " + call[i] + " vs gpu " + callPrice[i]);
                return false;
            }
            if (Math.abs(put[i] - putPrice[i]) > delta) {
                System.out.println("put: " + put[i] + " vs gpu " + putPrice[i]);
                return false;
            }
        }
        return true;
    }

    public static void blackScholes(int size, int iterations, String executionType) throws InterruptedException {

        Random random = new Random();
        float[] input = new float[size];
        float[] callPrice = new float[size];
        float[] putPrice = new float[size];
        float[] seqCall = new float[size];
        float[] seqPut = new float[size];
        TaskSchedule graph = new TaskSchedule("s0");
        long start,end;

        for (int i = 0; i < size; i++) {
            input[i] = random.nextFloat();
        }
        int maxThreadCount = Runtime.getRuntime().availableProcessors();

        Thread[] th = new Thread[maxThreadCount];

        if (executionType.equals("multi") || executionType.equals("sequential")) {
            ;
        } else {
            long startInit = System.nanoTime();
            graph.task("t0", BlackScholesMT::blackScholesKernel, input, callPrice, putPrice).streamOut(callPrice, putPrice);
            long stopInit = System.nanoTime();
            System.out.println("Initialization time:  " + (stopInit - startInit) + " ns" + "\n");
        }

        for (int i = 0; i < iterations; i++) {
            System.gc();
            switch (executionType) {
                case "performance":
                    start = System.nanoTime();
                    graph.executeWithProfilerSequential(Policy.PERFORMANCE);
                    end = System.nanoTime();
                    break;
                case "end":
                    start = System.nanoTime();
                    graph.executeWithProfilerSequential(Policy.END_2_END);
                    end = System.nanoTime();
                    break;
                case "sequential":
                    start = System.nanoTime();
                    blackScholesKernel(input, seqCall, seqPut);
                    end = System.nanoTime();
                    break;
                case "multi":
                    start = System.nanoTime();
                    blackScholesKernelThreaded(input, callPrice, putPrice, maxThreadCount, th);
                    end = System.nanoTime();
                    break;
                default:
                    start = System.nanoTime();
                    graph.execute();
                    end = System.nanoTime();
            }
            System.out.println("Total time:  " + (end - start) + " ns");
        }
        blackScholesKernel(input, seqCall, seqPut);
        boolean results = checkResult(seqCall, seqPut, callPrice, putPrice);
        System.out.println("Validation " + results + " \n");
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 3) {
            System.out.println("Usage: <size> <mode:performance|end|sequential|multi> <iterations>");
            System.exit(-1);
        }

        int size = Integer.parseInt(args[0]);
        int iterations = Integer.parseInt(args[2]);
        String executionType = args[1];

        System.out.println("Version running: " + executionType + " ! ");
        blackScholes(size, iterations, executionType);
    }
}
