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
package uk.ac.manchester.tornado.benchmarks;

import java.util.Random;

import uk.ac.manchester.tornado.api.collections.types.ImageFloat;

public final class BenchmarkUtils {

    public static void createFilter(final float[] filter, final int width, final int height) {
        float filterSum = 0.0f;
        final Random rand = new Random();

        for (int x = 0; x < height; x++) {
            for (int y = 0; y < width; y++) {
                final float f = rand.nextFloat();
                filterSum += f;
                filter[(y * width) + x] = f;
            }
        }

        for (int x = 0; x < height; x++) {
            for (int y = 0; y < width; y++) {
                filter[(y * width) + x] /= filterSum;
            }
        }
    }

    public static void createFilter(final ImageFloat filter) {
        createFilter(filter.asBuffer().array(), filter.X(), filter.Y());
    }

    public static void createImage(final float[] image, final int width, final int height) {
        final Random rand = new Random();
        rand.setSeed(7);
        for (int x = 0; x < height; x++) {
            for (int y = 0; y < width; y++) {
                image[(y * width) + x] = rand.nextInt(256);
            }
        }
    }

    public static void createImage(final ImageFloat image) {
        createImage(image.asBuffer().array(), image.X(), image.Y());
    }
}
