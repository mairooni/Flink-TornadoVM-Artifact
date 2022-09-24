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
package uk.ac.manchester.tornado.unittests.virtual;

import static uk.ac.manchester.tornado.unittests.virtual.TestVirtualDeviceFeatureExtraction.performComparison;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;
import uk.ac.manchester.tornado.api.enums.TornadoVMBackendType;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

public class TestVirtualDeviceKernel extends TornadoTestBase {

    private static final String SOURCE_DIR = System.getProperty("tornado.print.kernel.dir");

    @After
    public void after() {
        // make sure the source file generated is deleted
        File fileLog = new File(SOURCE_DIR);
        if (fileLog.exists()) {
            fileLog.delete();
        }
    }

    private static final int SIZE = 8192;

    private static void maxReduction(float[] input, @Reduce float[] result) {
        for (@Parallel int i = 0; i < input.length; i++) {
            result[0] = Math.max(result[0], input[i]);
        }
    }

    private void testVirtualDeviceKernel(String expectedCodeFile) {
        float[] input = new float[SIZE];
        float[] result = new float[1];
        IntStream.range(0, SIZE).forEach(idx -> {
            input[idx] = idx;
        });

        Arrays.fill(result, Float.MIN_VALUE);

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(input)
                .task("t0", TestVirtualDeviceKernel::maxReduction, input, result)
                .streamOut(result)
                .execute();
        //@formatter:on

        String tornadoSDK = System.getenv("TORNADO_SDK");
        String filePath = tornadoSDK + "/examples/generated/virtualDevice/" + expectedCodeFile;

        File fileLog = new File(SOURCE_DIR);
        File expectedKernelFile = new File(filePath);
        byte[] inputBytes = null;
        byte[] expectedBytes = null;
        try {
            inputBytes = Files.readAllBytes(fileLog.toPath());
            expectedBytes = Files.readAllBytes(expectedKernelFile.toPath());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        boolean fileEquivalent = performComparison(inputBytes, expectedBytes);
        Assert.assertTrue(fileEquivalent);
    }

    @Test
    public void testVirtualDeviceKernelGPU() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        testVirtualDeviceKernel("virtualDeviceKernelGPU.cl");
    }

    @Test
    public void testVirtualDeviceKernelCPU() {
        assertNotBackend(TornadoVMBackendType.PTX);
        assertNotBackend(TornadoVMBackendType.SPIRV);

        testVirtualDeviceKernel("virtualDeviceKernelCPU.cl");
    }

}
