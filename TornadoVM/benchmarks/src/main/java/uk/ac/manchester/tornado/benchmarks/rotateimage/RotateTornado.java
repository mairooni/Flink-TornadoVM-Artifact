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
package uk.ac.manchester.tornado.benchmarks.rotateimage;

import static uk.ac.manchester.tornado.api.collections.types.FloatOps.findMaxULP;
import static uk.ac.manchester.tornado.benchmarks.GraphicsKernels.rotateImage;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.collections.types.Float3;
import uk.ac.manchester.tornado.api.collections.types.ImageFloat3;
import uk.ac.manchester.tornado.api.collections.types.Matrix4x4Float;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.benchmarks.BenchmarkDriver;
import uk.ac.manchester.tornado.benchmarks.GraphicsKernels;

public class RotateTornado extends BenchmarkDriver {

    private final int numElementsX;
    private final int numElementsY;
    private ImageFloat3 input;
    private ImageFloat3 output;
    private Matrix4x4Float m;

    public RotateTornado(int iterations, int numElementsX, int numElementsY) {
        super(iterations);
        this.numElementsX = numElementsX;
        this.numElementsY = numElementsY;
    }

    @Override
    public void setUp() {
        input = new ImageFloat3(numElementsX, numElementsY);
        output = new ImageFloat3(numElementsX, numElementsY);

        m = new Matrix4x4Float();
        m.identity();

        final Float3 value = new Float3(1f, 2f, 3f);
        for (int i = 0; i < input.Y(); i++) {
            for (int j = 0; j < input.X(); j++) {
                input.set(j, i, value);
            }
        }

        ts = new TaskSchedule("benchmark");
        ts.streamIn(input);
        ts.task("rotateImage", GraphicsKernels::rotateImage, output, m, input);
        ts.streamOut(output);
        ts.warmup();
    }

    @Override
    public void tearDown() {
        ts.dumpProfiles();

        input = null;
        output = null;
        m = null;

        ts.getDevice().reset();
        super.tearDown();
    }

    @Override
    public void benchmarkMethod(TornadoDevice device) {
        ts.mapAllTo(device);
        ts.execute();
    }

    @Override
    public boolean validate(TornadoDevice device) {

        final ImageFloat3 result = new ImageFloat3(numElementsX, numElementsY);

        benchmarkMethod(device);
        ts.syncObjects(output);
        ts.clearProfiles();

        rotateImage(result, m, input);

        float maxULP = 0f;
        for (int i = 0; i < input.Y(); i++) {
            for (int j = 0; j < input.X(); j++) {
                final float ulp = findMaxULP(output.get(j, i), result.get(j, i));

                if (ulp > maxULP) {
                    maxULP = ulp;
                }
            }
        }
        return Float.compare(maxULP, MAX_ULP) <= 0;
    }

    public void printSummary() {
        if (isValid()) {
            System.out.printf("id=%s, elapsed=%f, per iteration=%f\n", TornadoRuntime.getProperty("benchmark.device"), getElapsed(), getElapsedPerIteration());
        } else {
            System.out.printf("id=%s produced invalid result\n", TornadoRuntime.getProperty("benchmark.device"));
        }
    }

}
