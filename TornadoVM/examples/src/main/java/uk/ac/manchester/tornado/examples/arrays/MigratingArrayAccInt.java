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

package uk.ac.manchester.tornado.examples.arrays;

import java.util.Arrays;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.TornadoDriver;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

public class MigratingArrayAccInt {

    public static void acc(int[] a, int value) {
        for (@Parallel int i = 0; i < a.length; i++) {
            a[i] += value;
        }
    }

    public static void main(String[] args) {

        final int numElements = 8;
        final int numKernels = 8;
        int[] a = new int[numElements];

        Arrays.fill(a, 0);

        //@formatter:off
        TaskSchedule s0 = new TaskSchedule("s0");
        for (int i = 0; i < numKernels; i++) {
            s0.task("t" + i, MigratingArrayAccInt::acc, a, 1);
        }
        s0.streamOut(a);
        //@formatter:on

        TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(0);
        s0.mapAllTo(driver.getDevice(0));
        s0.execute();

        System.out.println("a: " + Arrays.toString(a));
        System.out.println("migrating devices...");
        s0.mapAllTo(driver.getDevice(1));
        s0.execute();

        s0.dumpEvents();
        System.out.println("a: " + Arrays.toString(a));
    }

}
