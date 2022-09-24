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

package uk.ac.manchester.tornado.unittests.atomics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.TornadoVM_Intrinsics;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.enums.TornadoVMBackendType;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;
import uk.ac.manchester.tornado.unittests.common.TornadoNotSupported;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

public class TestAtomics extends TornadoTestBase {

    /**
     * Approach using a compiler-instrinsic in TornadoVM.
     * 
     * @param a
     *            Input array. It stores the addition with an atomic variable.
     */
    public static void atomic03(int[] a) {
        final int SIZE = 100;
        for (@Parallel int i = 0; i < a.length; i++) {
            int j = i % SIZE;
            a[j] = TornadoVM_Intrinsics.atomic_add(a, j, 1);
        }
    }

    @TornadoNotSupported
    public void testAtomic03() {
        final int size = 1024;
        int[] a = new int[size];
        int[] b = new int[size];

        Arrays.fill(a, 1);
        Arrays.fill(b, 1);

        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic03, a) //
                .streamOut(a) //
                .execute();

        atomic03(b);
        for (int i = 0; i < a.length; i++) {
            assertEquals(b[i], a[i]);
        }
    }

    /**
     * Approach using an API for Atomics. This provides atomics using the Java
     * semantics (block a single elements). Note that, in OpenCL, this single
     * elements has to be present in the device's global memory.
     * 
     * @param input
     *            input array
     */
    public static void atomic04(int[] input) {
        AtomicInteger tai = new AtomicInteger(200);
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = tai.incrementAndGet();
        }
    }

    @Test
    public void testAtomic04() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        TaskSchedule ts = new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic04, a) //
                .streamOut(a); //

        ts.execute();

        if (!ts.isFinished()) {
            assertTrue(false);
        }

        // On GPUs and FPGAs, threads within the same work-group run in parallel.
        // Increments will be performed atomically when using TornadoAtomicInteger.
        // However the order is not guaranteed. For this test, we need to check that
        // there are not repeated values in the output array.
        boolean repeated = isValueRepeated(a);
        assertTrue(!repeated);
    }

    /**
     * How to test?
     * 
     * <code>    
     * $ tornado-test.py -V -pk --debug -J"-Ddevice=0" uk.ac.manchester.tornado.unittests.atomics.TestAtomics#testAtomic05_precompiled
     * </code>
     */
    @Test
    public void testAtomic05_precompiled() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        int[] b = new int[1];
        Arrays.fill(a, 0);

        String deviceToRun = System.getProperties().getProperty("device", "0");
        int deviceNumber = Integer.parseInt(deviceToRun);

        TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(deviceNumber);
        String tornadoSDK = System.getenv("TORNADO_SDK");

        // @formatter:off
        new TaskSchedule("s0")
                .prebuiltTask("t0",
                        "add",
                        tornadoSDK + "/examples/generated/atomics.cl",
                        new Object[] { a, b },
                        new Access[] { Access.WRITE, Access.WRITE },
                        defaultDevice,
                        new int[] { 32 }, 
                        new int[]{155}     // Atomics - Initial Value
                        )
                .streamOut(a)
                .execute();
        // @formatter:on

        boolean repeated = isValueRepeated(a);
        assertTrue(!repeated);
    }

    public static void atomic06(int[] a, int[] b) {
        AtomicInteger taiA = new AtomicInteger(200);
        AtomicInteger taiB = new AtomicInteger(100);
        for (@Parallel int i = 0; i < a.length; i++) {
            a[i] = taiA.incrementAndGet();
            b[i] = taiB.incrementAndGet();
        }
    }

    @Test
    public void testAtomic06() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 2048;
        int[] a = new int[size];
        int[] b = new int[size];
        Arrays.fill(a, 1);
        Arrays.fill(b, 1);

        TaskSchedule ts = new TaskSchedule("s0") //
                .streamIn(a, b) //
                .task("t0", TestAtomics::atomic06, a, b) //
                .streamOut(a, b); //

        ts.execute();

        if (!ts.isFinished()) {
            assertTrue(false);
        }

        boolean repeated = isValueRepeated(a);
        repeated &= isValueRepeated(a);

        assertTrue(!repeated);
    }

    public static void atomic07(int[] input) {
        AtomicInteger ai = new AtomicInteger(200);
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = ai.incrementAndGet();
        }
    }

    @Test
    public void testAtomic07() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        TaskSchedule ts = new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic07, a) //
                .streamOut(a); //

        ts.execute();

        if (!ts.isFinished()) {
            assertTrue(false);
        }
        boolean repeated = isValueRepeated(a);
        assertTrue(!repeated);
    }

    public static void atomic08(int[] input) {
        AtomicInteger ai = new AtomicInteger(200);
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = ai.decrementAndGet();
        }
    }

    @Test
    public void testAtomic08() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        TaskSchedule ts = new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic08, a) //
                .streamOut(a); //

        ts.execute();

        if (!ts.isFinished()) {
            assertTrue(false);
        }
        boolean repeated = isValueRepeated(a);
        assertTrue(!repeated);
    }

    private boolean isValueRepeated(int[] array) {
        HashSet<Integer> set = new HashSet<>();
        boolean repeated = false;
        for (int j : array) {
            if (!set.contains(j)) {
                set.add(j);
            } else {
                repeated = true;
                break;
            }
        }
        return repeated;
    }

    public static int callAtomic(int[] input, int i, AtomicInteger ai) {
        return input[i] + ai.incrementAndGet();
    }

    public static void atomic09(int[] input, AtomicInteger ai) {
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = callAtomic(input, i, ai);
        }
    }

    @Test
    public void testAtomic09() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValue = 311;

        AtomicInteger ai = new AtomicInteger(initialValue);

        new TaskSchedule("s0") //
                .streamIn(a, ai) //
                .task("t0", TestAtomics::atomic09, a, ai) //
                .streamOut(a, ai) //
                .execute();

        boolean repeated = isValueRepeated(a);

        int lastValue = ai.get();
        assertTrue(!repeated);
        assertEquals(initialValue + size, lastValue);
    }

    @Test
    public void testAtomic10() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValue = 311;

        AtomicInteger ai = new AtomicInteger(initialValue);

        // We force a COPY_IN instead of STREAM_IN
        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic09, a, ai) //
                .streamOut(a, ai) //
                .execute();

        boolean repeated = isValueRepeated(a);

        int lastValue = ai.get();
        assertTrue(!repeated);
        assertEquals(initialValue + size, lastValue);
    }

    @Test
    public void testAtomic11() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValue = 311;

        AtomicInteger ai = new AtomicInteger(initialValue);

        // We force a COPY_IN instead of STREAM_IN
        // Also, the atomic uses COPY_OUT non blocking call
        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic09, a, ai) //
                .streamOut(ai, a) //
                .execute();

        boolean repeated = isValueRepeated(a);

        int lastValue = ai.get();
        assertTrue(!repeated);
        assertEquals(initialValue + size, lastValue);
    }

    public static void atomic10(int[] input, AtomicInteger ai, AtomicInteger bi) {
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = input[i] + ai.incrementAndGet() + bi.incrementAndGet();
        }
    }

    @Test
    public void testAtomic12() {
        // Calling multiple atomics
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValueA = 311;
        final int initialValueB = 500;

        AtomicInteger ai = new AtomicInteger(initialValueA);
        AtomicInteger bi = new AtomicInteger(initialValueB);

        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic10, a, ai, bi) //
                .streamOut(ai, a, bi) //
                .execute();

        boolean repeated = isValueRepeated(a);

        int lastValue = ai.get();
        assertTrue(!repeated);
        assertEquals(initialValueA + size, lastValue);

        lastValue = bi.get();
        assertEquals(initialValueB + size, lastValue);

    }

    public static void atomic13(int[] input, AtomicInteger ai) {
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = input[i] + ai.decrementAndGet();
        }
    }

    @Test
    public void testAtomic13() {
        // Calling multiple atomics
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValueA = 311;
        AtomicInteger ai = new AtomicInteger(initialValueA);

        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic13, a, ai) //
                .streamOut(ai, a) //
                .execute();

        boolean repeated = isValueRepeated(a);

        int lastValue = ai.get();
        assertTrue(!repeated);
        assertEquals(initialValueA - size, lastValue);
    }

    public static void atomic14(int[] input, AtomicInteger ai, AtomicInteger bi) {
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = input[i] + ai.incrementAndGet();
            input[i] = input[i] + bi.decrementAndGet();
        }
    }

    @Test
    public void testAtomic14() {
        // Calling multiple atomics
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValueA = 311;
        final int initialValueB = 50;
        AtomicInteger ai = new AtomicInteger(initialValueA);
        AtomicInteger bi = new AtomicInteger(initialValueB);

        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic14, a, ai, bi) //
                .streamOut(ai, a, bi) //
                .execute();

        int lastValue = ai.get();
        assertEquals(initialValueA + size, lastValue);

        lastValue = bi.get();
        assertEquals(initialValueB - size, lastValue);
    }

    /**
     * This example combines an atomic created inside the compute kernel with an
     * atomic passed as an argument.
     * 
     * @param input
     *            Input array
     * @param ai
     *            Atomic Integer stored in Global Memory (atomic-region)
     */
    public static void atomic15(int[] input, AtomicInteger ai) {
        AtomicInteger bi = new AtomicInteger(500);
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = input[i] + ai.incrementAndGet();
            input[i] = input[i] + bi.incrementAndGet();
        }
    }

    @Test
    public void testAtomic15() {
        // Calling multiple atomics
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValueA = 311;
        AtomicInteger ai = new AtomicInteger(initialValueA);

        new TaskSchedule("s0") //
                .task("t0", TestAtomics::atomic15, a, ai) //
                .streamOut(ai, a) //
                .execute();

        int lastValue = ai.get();
        assertEquals(initialValueA + size, lastValue);

        boolean repeated = isValueRepeated(a);
        assertTrue(!repeated);
    }

    public static void atomic16(int[] input, AtomicInteger ai) {
        for (@Parallel int i = 0; i < input.length; i++) {
            input[i] = input[i] + ai.incrementAndGet();
        }
    }

    @Test
    public void testAtomic16() {
        // Calling multiple atomics
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        final int size = 32;
        int[] a = new int[size];
        Arrays.fill(a, 1);

        final int initialValueA = 311;
        AtomicInteger ai = new AtomicInteger(initialValueA);

        TaskSchedule ts = new TaskSchedule("s0") //
                .streamIn(ai) //
                .task("t0", TestAtomics::atomic16, a, ai) //
                .streamOut(ai, a);

        final int iterations = 50;
        IntStream.range(0, iterations).forEach(i -> {
            ts.execute();
        });

        int lastValue = ai.get();
        assertEquals(initialValueA + (iterations * size), lastValue);
    }

}
