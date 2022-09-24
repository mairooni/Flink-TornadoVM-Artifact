/*
 * Copyright (c) 2013-2019, APT Group, School of Computer Science,
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

package uk.ac.manchester.tornado.examples;

import java.math.BigDecimal;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.examples.common.Messages;

/**
 * Run with:
 * 
 * tornado uk.ac.manchester.tornado.examples.Init <size>
 * 
 */
public class Copy {

    public static void compute(int[] array, int[] o) {
        for (@Parallel int i = 0; i < array.length; i++) {
            o[i] = array[i];
        }
    }

    public static void main(String[] args) {
        int size = 256;
        int[] array = new int[size];
        int[] output = new int[size];
        TaskSchedule ts = new TaskSchedule("s0");
        ts.task("t0", Copy::compute, array, output).streamOut(output);
        ts.execute();
    }
}
