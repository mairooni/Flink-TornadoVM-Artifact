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
package uk.ac.manchester.tornado.benchmarks.nbody;

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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static uk.ac.manchester.tornado.benchmarks.ComputeKernels.nBody;

public class JMHNBody {
    @State(Scope.Thread)
    public static class BenchmarkSetup {

        int numBodies = Integer.parseInt(System.getProperty("x", "16384"));
        float delT;
        float espSqr;
        float[] velSeq;
        private float[] posSeq;

        private TaskSchedule ts;

        @Setup(Level.Trial)
        public void doSetup() {
            delT = 0.005f;
            espSqr = 500.0f;

            float[] auxPositionRandom = new float[numBodies * 4];
            float[] auxVelocityZero = new float[numBodies * 3];

            for (int i = 0; i < auxPositionRandom.length; i++) {
                auxPositionRandom[i] = (float) Math.random();
            }

            Arrays.fill(auxVelocityZero, 0.0f);

            posSeq = new float[numBodies * 4];
            velSeq = new float[numBodies * 4];

            if (auxPositionRandom.length >= 0) {
                System.arraycopy(auxPositionRandom, 0, posSeq, 0, auxPositionRandom.length);
            }

            if (auxVelocityZero.length >= 0) {
                System.arraycopy(auxVelocityZero, 0, velSeq, 0, auxVelocityZero.length);
            }

            ts = new TaskSchedule("benchmark") //
                    .streamIn(velSeq, posSeq) //
                    .task("t0", ComputeKernels::nBody, numBodies, posSeq, velSeq, delT, espSqr);
            ts.warmup();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void nbodyJava(BenchmarkSetup state) {
        nBody(state.numBodies, state.posSeq, state.velSeq, state.delT, state.espSqr);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Fork(1)
    public void nbodyTornado(BenchmarkSetup state, Blackhole blackhole) {
        TaskSchedule t = state.ts;
        t.execute();
        blackhole.consume(t);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
                .include(JMHNBody.class.getName() + ".*") //
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
