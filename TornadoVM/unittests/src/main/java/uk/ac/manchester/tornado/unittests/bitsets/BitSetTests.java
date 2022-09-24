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

package uk.ac.manchester.tornado.unittests.bitsets;

import org.apache.lucene.util.LongBitSet;
import org.junit.Test;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test accelerating the Lucene library.
 */
public class BitSetTests extends TornadoTestBase {

    public static void intersectionCount(int numWords, LongBitSet a, LongBitSet b, long[] result) {
        final long[] aBits = a.getBits();
        final long[] bBits = b.getBits();
        for (@Parallel int i = 0; i < numWords; i++) {
            result[i] = Long.bitCount(aBits[i] & bBits[i]);
        }
    }

    @Test
    public void test01() {

        final int numWords = 8192;
        final Random rand = new Random(7);
        final long[] aBits = new long[numWords];
        final long[] bBits = new long[numWords];

        final LongBitSet a = new LongBitSet(aBits, numWords * 8);
        final LongBitSet b = new LongBitSet(bBits, numWords * 8);
        long[] result = new long[numWords];
        long[] seq = new long[numWords];

        for (int i = 0; i < aBits.length; i++) {
            aBits[i] = rand.nextLong();
            bBits[i] = rand.nextLong();
        }

        TaskSchedule ts = new TaskSchedule("s0") //
                .task("t0", BitSetTests::intersectionCount, numWords, a, b, result) //
                .streamOut(result);
        ts.execute();

        intersectionCount(numWords, a, b, seq);

        for (int i = 0; i < numWords; i++) {
            assertEquals(seq[i], result[i], 0.1f);
        }
    }

}
