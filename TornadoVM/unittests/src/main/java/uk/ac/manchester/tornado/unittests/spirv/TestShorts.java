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
package uk.ac.manchester.tornado.unittests.spirv;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

/**
 * Run:
 * 
 * <code>
 *     tornado-test.py -V uk.ac.manchester.tornado.unittests.spirv.TestShorts
 * </code>
 */
public class TestShorts extends TornadoTestBase {

    @Test
    public void testShortAdd() {
        final int numElements = 256;
        short[] a = new short[numElements];
        short[] b = new short[numElements];
        short[] c = new short[numElements];

        Arrays.fill(b, (short) 1);
        Arrays.fill(c, (short) 3);

        short[] expectedResult = new short[numElements];
        Arrays.fill(expectedResult, (short) 4);

        new TaskSchedule("s0") //
                .task("t0", TestKernels::vectorSumShortCompute, a, b, c) //
                .streamOut(a) //
                .execute(); //

        assertArrayEquals(expectedResult, a);
    }

}
