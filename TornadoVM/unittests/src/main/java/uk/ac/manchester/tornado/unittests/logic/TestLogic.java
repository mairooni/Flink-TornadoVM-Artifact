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
package uk.ac.manchester.tornado.unittests.logic;

import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

public class TestLogic extends TornadoTestBase {

    public static void logic01(int[] data, int[] output) {
        for (@Parallel int i = 0; i < data.length; i++) {
            output[i] = data[i] & data[i] - 1;
        }
    }

    public static void logic02(int[] data, int[] output) {
        for (@Parallel int i = 0; i < data.length; i++) {
            output[i] = data[i] | data[i] - 1;
        }
    }

    public static void logic03(int[] data, int[] output) {
        for (@Parallel int i = 0; i < data.length; i++) {
            output[i] = data[i] ^ data[i] - 1;
        }
    }

    public static void logic04(int[] data, int[] output) {
        for (@Parallel int i = 0; i < data.length; i++) {
            int value = data[i];
            if ((value & (value - 1)) != 0) {

                int condition = (value & (value - 1));
                while (condition != 0) {
                    value &= value - 1;
                    condition = (value & (value - 1));
                }
            }
            output[i] = value;
        }
    }

    @Test
    public void testLogic01() {
        final int N = 1024;
        int[] data = new int[N];
        int[] output = new int[N];
        int[] sequential = new int[N];

        IntStream.range(0, data.length).sequential().forEach(i -> data[i] = i);

        TaskSchedule s0 = new TaskSchedule("s0");

        // @formatter:off
        s0.task("t0", TestLogic::logic01, data, output)
          .streamOut(output);
        // @formatter:on
        s0.execute();

        logic01(data, sequential);

        for (int i = 0; i < data.length; i++) {
            assertEquals(sequential[i], output[i]);
        }

    }

    @Test
    public void testLogic02() {
        final int N = 1024;
        int[] data = new int[N];
        int[] output = new int[N];
        int[] sequential = new int[N];

        IntStream.range(0, data.length).sequential().forEach(i -> data[i] = i);

        TaskSchedule s0 = new TaskSchedule("s0");

        // @formatter:off
        s0.task("t0", TestLogic::logic02, data, output)
          .streamOut(output);
        // @formatter:on
        s0.execute();

        logic02(data, sequential);

        for (int i = 0; i < data.length; i++) {
            assertEquals(sequential[i], output[i]);
        }
    }

    @Test
    public void testLogic03() {
        final int N = 1024;
        int[] data = new int[N];
        int[] output = new int[N];
        int[] sequential = new int[N];

        IntStream.range(0, data.length).sequential().forEach(i -> data[i] = i);

        TaskSchedule s0 = new TaskSchedule("s0");

        // @formatter:off
        s0.task("t0", TestLogic::logic03, data, output)
                .streamOut(output);
        // @formatter:on
        s0.execute();

        logic03(data, sequential);

        for (int i = 0; i < data.length; i++) {
            assertEquals(sequential[i], output[i]);
        }
    }

    @Ignore
    public void testLogic04() {
        final int N = 1024;
        int[] data = new int[N];
        int[] output = new int[N];
        int[] sequential = new int[N];

        IntStream.range(0, data.length).sequential().forEach(i -> data[i] = i);

        TaskSchedule s0 = new TaskSchedule("s0");

        // @formatter:off
        s0.task("t0", TestLogic::logic04, data, output)
          .streamOut(output);
        // @formatter:on
        s0.execute();

        logic04(data, sequential);

        for (int i = 0; i < data.length; i++) {
            assertEquals(sequential[i], output[i]);
        }

    }

}
