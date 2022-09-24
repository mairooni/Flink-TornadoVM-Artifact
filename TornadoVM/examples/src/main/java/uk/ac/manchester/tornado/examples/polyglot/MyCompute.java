/*
 * Copyright (c) 2020, APT Group, Department of Computer Science,
 * School of Engineering, The University of Manchester. All rights reserved.
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
package uk.ac.manchester.tornado.examples.polyglot;

import java.util.stream.IntStream;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;

public class MyCompute {
    private static void mxm(float[] a, float[] b, float[] c, int N) {
        for (@Parallel int i = 0; i < N; i++) {
            for (@Parallel int j = 0; j < N; j++) {
                float sum = 0.0f;
                for (int k = 0; k < N; k++) {
                    sum += a[i * N + k] + b[k + N + j];
                }
                c[i * N + j] = sum;
            }
        }
    }

    public static float[] compute() {
        final int N = 256;
        float[] a = new float[N * N];
        float[] b = new float[N * N];
        float[] c = new float[N * N];

        IntStream.range(0, N * N).sequential().forEach(i -> {
            a[i] = 2.0f;
            b[i] = 1.4f;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", MyCompute::mxm, a, b, c, N)
                .streamOut(c)
                .execute();
        //@formatter:on
        return c;
    }

}
