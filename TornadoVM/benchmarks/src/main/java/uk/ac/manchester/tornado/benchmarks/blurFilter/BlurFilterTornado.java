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

package uk.ac.manchester.tornado.benchmarks.blurFilter;

import java.util.Random;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.benchmarks.BenchmarkDriver;
import uk.ac.manchester.tornado.benchmarks.ComputeKernels;

public class BlurFilterTornado extends BenchmarkDriver {

    private int size;
    public static final int FILTER_WIDTH = 31;
    int[] redChannel;
    int[] greenChannel;
    int[] blueChannel;
    int[] alphaChannel;
    int[] redFilter;
    int[] greenFilter;
    int[] blueFilter;
    float[] filter;

    public BlurFilterTornado(int iterations, int size) {
        super(iterations);
        this.size = size;
    }

    @Override
    public void setUp() {
        int w = size;
        int h = size;

        redChannel = new int[w * h];
        greenChannel = new int[w * h];
        blueChannel = new int[w * h];
        alphaChannel = new int[w * h];

        greenFilter = new int[w * h];
        redFilter = new int[w * h];
        blueFilter = new int[w * h];

        filter = new float[w * h];
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                filter[i * h + j] = 1.f / (FILTER_WIDTH * FILTER_WIDTH);
            }
        }

        Random r = new Random();
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                int rgb = r.nextInt(255);
                alphaChannel[i * h + j] = (rgb >> 24) & 0xFF;
                redChannel[i * h + j] = (rgb >> 16) & 0xFF;
                greenChannel[i * h + j] = (rgb >> 8) & 0xFF;
                blueChannel[i * h + j] = (rgb & 0xFF);
            }
        }

        ts = new TaskSchedule("blur") //
                .streamIn(redChannel, greenChannel, blueChannel) //
                .task("red", ComputeKernels::channelConvolution, redChannel, redFilter, w, h, filter, FILTER_WIDTH) //
                .task("green", ComputeKernels::channelConvolution, greenChannel, greenFilter, w, h, filter, FILTER_WIDTH) //
                .task("blue", ComputeKernels::channelConvolution, blueChannel, blueFilter, w, h, filter, FILTER_WIDTH) //
                .streamOut(redFilter, greenFilter, blueFilter) //
                .useDefaultThreadScheduler(true);
    }

    @Override
    public void tearDown() {
        ts.dumpProfiles();
        redChannel = null;
        greenChannel = null;
        blueChannel = null;
        alphaChannel = null;
        greenFilter = null;
        redFilter = null;
        blueFilter = null;
        super.tearDown();
    }

    @Override
    public boolean validate(TornadoDevice device) {
        boolean valid = true;
        int w = size;
        int h = size;

        int[] redChannel = new int[w * h];
        int[] greenChannel = new int[w * h];
        int[] blueChannel = new int[w * h];
        int[] alphaChannel = new int[w * h];

        int[] greenFilter = new int[w * h];
        int[] redFilter = new int[w * h];
        int[] blueFilter = new int[w * h];

        int[] greenFilterSeq = new int[w * h];
        int[] redFilterSeq = new int[w * h];
        int[] blueFilterSeq = new int[w * h];

        float[] filter = new float[w * h];
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                filter[i * h + j] = 1.f / (FILTER_WIDTH * FILTER_WIDTH);
            }
        }

        Random r = new Random();
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                int rgb = r.nextInt(255);
                alphaChannel[i * h + j] = (rgb >> 24) & 0xFF;
                redChannel[i * h + j] = (rgb >> 16) & 0xFF;
                greenChannel[i * h + j] = (rgb >> 8) & 0xFF;
                blueChannel[i * h + j] = (rgb & 0xFF);
            }
        }

        TaskSchedule parallelFilter = new TaskSchedule("blur") //
                .streamIn(redChannel, greenChannel, blueChannel) //
                .task("red", ComputeKernels::channelConvolution, redChannel, redFilter, w, h, filter, FILTER_WIDTH) //
                .task("green", ComputeKernels::channelConvolution, greenChannel, greenFilter, w, h, filter, FILTER_WIDTH) //
                .task("blue", ComputeKernels::channelConvolution, blueChannel, blueFilter, w, h, filter, FILTER_WIDTH) //
                .streamOut(redFilter, greenFilter, blueFilter);

        parallelFilter.execute();

        // Sequential
        ComputeKernels.channelConvolution(redChannel, redFilterSeq, size, size, filter, FILTER_WIDTH);
        ComputeKernels.channelConvolution(greenChannel, greenFilterSeq, size, size, filter, FILTER_WIDTH);
        ComputeKernels.channelConvolution(blueChannel, blueFilterSeq, size, size, filter, FILTER_WIDTH);

        for (int i = 0; i < redFilter.length; i++) {
            if (redFilter[i] != redFilterSeq[i]) {
                valid = false;
                break;
            }
            if (greenFilter[i] != greenFilterSeq[i]) {
                valid = false;
                break;
            }
            if (blueFilter[i] != blueFilterSeq[i]) {
                valid = false;
                break;
            }
        }

        return valid;
    }

    @Override
    public void benchmarkMethod(TornadoDevice device) {
        ts.mapAllTo(device);
        ts.execute();
    }
}
