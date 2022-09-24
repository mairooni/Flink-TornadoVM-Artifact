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

package uk.ac.manchester.tornado.unittests.instances;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;

/**
 * Execute:
 * 
 * <code>
 * tornado-test.py -V -pk --debug -J"-Dtornado.ignore.nullchecks=True" uk.ac.manchester.tornado.unittests.instances.LocalVariableInstance
 * </code>
 * 
 */
public class LocalVariableInstance {

    public static class FlinkMapUDF {
        public int map(int value) {
            int x = value - 2 + 9;
            return value * x;
        }
    }

    public static abstract class MiddleMap {
        public abstract int mymapintint(int i);
    }

    public static class MyMap extends MiddleMap {
        @Override
        public int mymapintint(int i) {
            FlinkMapUDF fudf = new FlinkMapUDF();
            return fudf.map(i);
        }
    }

    public static class MapSkeleton {

        public MiddleMap mdm;

        MapSkeleton(MiddleMap mdm) {
            this.mdm = mdm;
        }

        public void map(int[] input, int[] output) {
            for (@Parallel int i = 0; i < input.length; i++) {
                output[i] = mdm.mymapintint(i);
            }
        }

    }

    public int[] sequential(int in[]) {
        int[] out = new int[in.length];
        int x;
        for (int i = 0; i < in.length; i++) {
            x = i - 2 + 9;
            out[i] = i * x;
        }
        return out;
    }

    @Test
    public void testLocalVariable() {

        int[] in = new int[5];
        int[] out = new int[5];

        for (int i = 0; i < in.length; i++) {
            in[i] = i;
        }

        MyMap mm = new MyMap();
        MapSkeleton msk = new MapSkeleton((MiddleMap) mm);

        // @formatter:off
        TaskSchedule task = new TaskSchedule("s0")
                .streamIn(in)
                .task("t0", msk::map, in, out)
                .streamOut(out);
        // @formatter:on

        task.execute();

        int[] seq = sequential(in);

        assertArrayEquals(seq, out);
    }

}