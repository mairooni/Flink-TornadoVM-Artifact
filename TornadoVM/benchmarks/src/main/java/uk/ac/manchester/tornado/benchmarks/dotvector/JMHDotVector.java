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
package uk.ac.manchester.tornado.benchmarks.dotvector;

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
import uk.ac.manchester.tornado.api.collections.types.Float3;
import uk.ac.manchester.tornado.api.collections.types.VectorFloat3;
import uk.ac.manchester.tornado.benchmarks.GraphicsKernels;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static uk.ac.manchester.tornado.benchmarks.GraphicsKernels.dotVector;

public class JMHDotVector {

    @State(Scope.Thread)
    public static class BenchmarkSetup {
        int numElements = Integer.parseInt(System.getProperty("x", "4194304"));
        private VectorFloat3 a;
        private VectorFloat3 b;
        private float[] c;
        TaskSchedule ts;

        @Setup(Level.Trial)
        public void doSetup() {
            a = new VectorFloat3(numElements);
            b = new VectorFloat3(numElements);
            c = new float[numElements];

            Random r = new Random();
            for (int i = 0; i < numElements; i++) {
                float[] ra = new float[3];
                IntStream.range(0, ra.length).forEach(x -> ra[x] = r.nextFloat());
                float[] rb = new float[3];
                IntStream.range(0, rb.length).forEach(x -> rb[x] = r.nextFloat());
                a.set(i, new Float3(ra));
                b.set(i, new Float3(rb));
            }
            ts = new TaskSchedule("benchmark")//
                    .streamIn(a, b) //
                    .task("dotVector", GraphicsKernels::dotVector, a, b, c) //
                    .streamOut(c);
            ts.warmup();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void dotVectorJava(BenchmarkSetup state) {
        dotVector(state.a, state.b, state.c);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void dotVectorTornado(BenchmarkSetup state, Blackhole blackhole) {
        TaskSchedule t = state.ts;
        t.execute();
        blackhole.consume(t);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(JMHDotVector.class.getName() + ".*") //
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
