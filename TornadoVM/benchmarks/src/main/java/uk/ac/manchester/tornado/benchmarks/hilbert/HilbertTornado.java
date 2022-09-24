/*
 * Copyright (c) 2013-2020, APT Group, Department of Computer Science,
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
package uk.ac.manchester.tornado.benchmarks.hilbert;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.benchmarks.BenchmarkDriver;
import uk.ac.manchester.tornado.benchmarks.ComputeKernels;

public class HilbertTornado extends BenchmarkDriver {

    private int size;
    private float[] hilbertMatrix;

    public HilbertTornado(int size, int iterations) {
        super(iterations);
        this.size = size;
    }

    @Override
    public void setUp() {
        hilbertMatrix = new float[size * size];
        // @formatter:off
        ts = new TaskSchedule("s0")
                .task("t0", ComputeKernels::hilbertComputation, hilbertMatrix, size, size)
                .streamOut(hilbertMatrix);
        // @formatter:on
        ts.warmup();
    }

    @Override
    public void tearDown() {
        ts.dumpProfiles();
        hilbertMatrix = null;
        ts.getDevice().reset();
        super.tearDown();
    }

    @Override
    public boolean validate(TornadoDevice device) {
        boolean val = true;
        float[] testData = new float[size * size];
        // @formatter:off
        TaskSchedule check = new TaskSchedule("s0")
                .task("t0", ComputeKernels::hilbertComputation, testData, size, size)
                .streamOut(testData);
        // @formatter:on
        check.mapAllTo(device);
        check.execute();
        float[] seq = new float[size * size];
        ComputeKernels.hilbertComputation(seq, size, size);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (Math.abs(testData[i * size + j] - seq[i * size + j]) > 0.01f) {
                    val = false;
                    break;
                }
            }
        }
        return val;
    }

    @Override
    public void benchmarkMethod(TornadoDevice device) {
        ts.mapAllTo(device);
        ts.execute();
    }
}
