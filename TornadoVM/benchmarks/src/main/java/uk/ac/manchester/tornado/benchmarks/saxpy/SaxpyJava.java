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
package uk.ac.manchester.tornado.benchmarks.saxpy;

import static uk.ac.manchester.tornado.benchmarks.LinearAlgebraArrays.saxpy;

import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.benchmarks.BenchmarkDriver;

public class SaxpyJava extends BenchmarkDriver {

    private final int numElements;

    private float[] x;
    private float[] y;
    private final float alpha = 2f;

    public SaxpyJava(int iterations, int numElements) {
        super(iterations);
        this.numElements = numElements;
    }

    @Override
    public void setUp() {
        x = new float[numElements];
        y = new float[numElements];

        for (int i = 0; i < numElements; i++) {
            x[i] = i;
        }

    }

    @Override
    public void tearDown() {
        x = null;
        y = null;
        super.tearDown();
    }

    @Override
    public void benchmarkMethod(TornadoDevice device) {
        saxpy(alpha, x, y);
    }

    @Override
    public void barrier() {

    }

    @Override
    public boolean validate(TornadoDevice device) {
        return true;
    }

    public void printSummary() {
        System.out.printf("id=java-serial, elapsed=%f, per iteration=%f\n", getElapsed(), getElapsedPerIteration());
    }

}
