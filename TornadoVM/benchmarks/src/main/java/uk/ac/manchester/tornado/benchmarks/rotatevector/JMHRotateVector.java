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
package uk.ac.manchester.tornado.benchmarks.rotatevector;

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
import uk.ac.manchester.tornado.api.collections.types.Matrix4x4Float;
import uk.ac.manchester.tornado.api.collections.types.VectorFloat3;
import uk.ac.manchester.tornado.benchmarks.GraphicsKernels;

import java.util.concurrent.TimeUnit;

import static uk.ac.manchester.tornado.benchmarks.GraphicsKernels.rotateVector;

public class JMHRotateVector {
    @State(Scope.Thread)
    public static class BenchmarkSetup {

        private final int numElements = Integer.parseInt(System.getProperty("y", "4194304"));
        private VectorFloat3 input;
        private VectorFloat3 output;
        private Matrix4x4Float m;
        private TaskSchedule ts;

        @Setup(Level.Trial)
        public void doSetup() {
            input = new VectorFloat3(numElements);
            output = new VectorFloat3(numElements);

            m = new Matrix4x4Float();
            m.identity();

            final Float3 value = new Float3(1f, 2f, 3f);
            for (int i = 0; i < numElements; i++) {
                input.set(i, value);
            }

            ts = new TaskSchedule("benchmark") //
                    .streamIn(input) //
                    .task("rotateVector", GraphicsKernels::rotateVector, output, m, input) //
                    .streamOut(output);
            ts.warmup();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void rotateVectorJava(BenchmarkSetup state) {
        rotateVector(state.output, state.m, state.input);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void rotateVectorTornado(BenchmarkSetup state, Blackhole blackhole) {
        TaskSchedule t = state.ts;
        t.execute();
        blackhole.consume(t);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(JMHRotateVector.class.getName() + ".*") //
                .mode(Mode.AverageTime) //
                .timeUnit(TimeUnit.SECONDS) //
                .warmupTime(TimeValue.seconds(60)) //
                .warmupIterations(2) //
                .measurementTime(TimeValue.seconds(30)) //
                .measurementIterations(5) //
                .forks(1) //
                .build();
        new Runner(opt).run();
    }
}
