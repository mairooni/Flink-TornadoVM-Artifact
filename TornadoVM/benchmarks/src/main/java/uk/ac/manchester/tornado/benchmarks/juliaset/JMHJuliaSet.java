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
package uk.ac.manchester.tornado.benchmarks.juliaset;

import java.util.concurrent.TimeUnit;

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
import uk.ac.manchester.tornado.benchmarks.GraphicsKernels;

public class JMHJuliaSet {

    @State(Scope.Thread)
    public static class BenchmarkSetup {

        int size = Integer.parseInt(System.getProperty("x", "8192"));
        float[] hue;
        float[] brightness;
        TaskSchedule ts;

        @Setup(Level.Trial)
        public void doSetup() {
            hue = new float[size * size];
            brightness = new float[size * size];

            ts = new TaskSchedule("benchmark") //
                    .streamIn(hue, brightness) //
                    .task("juliaset", GraphicsKernels::juliaSetTornado, size, hue, brightness) //
                    .streamOut(hue, brightness);
            ts.warmup();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void juliaSetJava(JMHJuliaSet.BenchmarkSetup state) {
        GraphicsKernels.juliaSetTornado(state.size, state.hue, state.brightness);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void juliaSetTornado(JMHJuliaSet.BenchmarkSetup state, Blackhole blackhole) {
        TaskSchedule t = state.ts;
        t.execute();
        blackhole.consume(t);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(JMHJuliaSet.class.getName() + ".*") //
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
