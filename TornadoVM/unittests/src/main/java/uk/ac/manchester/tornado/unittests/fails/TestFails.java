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

package uk.ac.manchester.tornado.unittests.fails;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.TornadoDriver;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.exceptions.TornadoCompilationException;
import uk.ac.manchester.tornado.api.exceptions.TornadoFailureException;
import uk.ac.manchester.tornado.api.exceptions.TornadoRuntimeException;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

/**
 * Test bad uses of the TornadoVM API. It should throw exceptions when possible
 * with the concrete problem.
 */
public class TestFails extends TornadoTestBase {

    private void reset() {
        for (int i = 0; i < TornadoRuntime.getTornadoRuntime().getNumDrivers(); i++) {
            final TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(i);
            for (int j = 0; j < driver.getDeviceCount(); j++) {
                driver.getDevice(j).reset();
            }
        }
    }

    @Test(expected = TornadoFailureException.class)
    public void test01() {
        // =============================================================================
        // Call reset after warm-up. This is not legal in TornadoVM. WarmUP will
        // initialize the heap and the code cache. If reset is called, it will clean all
        // state.
        // This is a different case of calling reset and then execute, because it will
        // reset the internal state of variables if needed, meanwhile warmup skip many
        // of those steps.
        // =============================================================================

        float[] x = new float[100];
        float[] y = new float[100];

        TaskSchedule ts = new TaskSchedule("s0").streamIn(x).task("s0", (a, b) -> {
            for (int i = 0; i < 100; i++) {

            }
        }, x, y).streamOut(y);

        // How to provoke the failure
        ts.warmup();
        reset();
        ts.execute();
    }

    private static void kernel(float[] a, float[] b) {
        for (@Parallel int i = 0; i < a.length; i++) {
            b[i] = a[i];
        }
    }

    @Test(expected = TornadoRuntimeException.class)
    public void test02() {
        // This test fails because the Java method's name to be accelerated corresponds
        // to an OpenCL token.

        float[] x = new float[100];
        float[] y = new float[100];

        // @formatter:off
        TaskSchedule ts = new TaskSchedule("s0")
                .streamIn(x)
                .task("s0", TestFails::kernel, x, y)
                .streamOut(y);
        // @formatter:on

        // How to provoke the failure
        ts.execute();
    }

}
