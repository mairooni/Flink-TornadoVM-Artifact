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
package uk.ac.manchester.tornado.benchmarks.blurFilter;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.benchmarks.ComputeKernels;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class JMHBlurFilter {

    @State(Scope.Thread)
    public static class BenchmarkSetup {
        int size = Integer.parseInt(System.getProperty("x", "512"));
        public static final int FILTER_WIDTH = 31;
        int[] redChannel;
        int[] greenChannel;
        int[] blueChannel;
        int[] alphaChannel;
        int[] redFilter;
        int[] greenFilter;
        int[] blueFilter;
        float[] filter;
        TaskSchedule ts;

        @Setup(Level.Trial)
        public void doSetup() {
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
            ts.warmup();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void blurFilterJava(BenchmarkSetup state) {
        ComputeKernels.channelConvolution(state.redChannel, state.redFilter, state.size, state.size, state.filter, state.FILTER_WIDTH);
        ComputeKernels.channelConvolution(state.greenChannel, state.greenFilter, state.size, state.size, state.filter, state.FILTER_WIDTH);
        ComputeKernels.channelConvolution(state.blueChannel, state.blueFilter, state.size, state.size, state.filter, state.FILTER_WIDTH);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void blurFilterTornado(BenchmarkSetup state, Blackhole blackhole) {
        TaskSchedule t = state.ts;
        t.execute();
        blackhole.consume(t);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(JMHBlurFilter.class.getName() + ".*") //
                .mode(Mode.AverageTime) //
                .timeUnit(TimeUnit.NANOSECONDS) //
                .warmupTime(TimeValue.seconds(60)) //
                .warmupIterations(2) //
                .measurementTime(TimeValue.seconds(30)) //
                .measurementIterations(5) //
                .forks(1) //
                .build();
        new Runner(opt).run();
    }
}
