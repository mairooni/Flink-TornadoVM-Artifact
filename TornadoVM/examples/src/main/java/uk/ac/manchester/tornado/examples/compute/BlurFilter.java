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
package uk.ac.manchester.tornado.examples.compute;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.JFrame;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

/**
 * It applies a Blur filter to an input image. Algorithm taken from CUDA course
 * CS344 in Udacity.
 * 
 * Example borrowed from the Marawacc parallel programming framework with the
 * permission from the author.
 * 
 * 
 * How to run?
 * 
 * <code>
 * $ tornado uk.ac.manchester.tornado.examples.compute.BlurFilter 
 * </code>
 * 
 *
 */
public class BlurFilter {

    @SuppressWarnings("serial")
    public static class BlurFilterImage extends Component {

        private BufferedImage image;

        public static final boolean PARALLEL_COMPUTATION = Boolean.parseBoolean(System.getProperty("run:parallel", "True"));
        public static final int FILTER_WIDTH = 31;

        private static final String IMAGE_FILE = "/tmp/image.jpg";

        public BlurFilterImage() {
            loadImage();
        }

        public void loadImage() {
            try {
                image = ImageIO.read(new File(IMAGE_FILE));
            } catch (IOException e) {
                throw new RuntimeException("Input file not found: " + IMAGE_FILE);
            }
        }

        private static void channelConvolutionSequential(int[] channel, int[] channelBlurred, final int numRows, final int numCols, float[] filter, final int filterWidth) {
            // Dealing with an even width filter is trickier
            assert (filterWidth % 2 == 1);

            // For every pixel in the image
            for (int r = 0; r < numRows; ++r) {
                for (int c = 0; c < numCols; ++c) {
                    float result = 0.0f;
                    // For every value in the filter around the pixel (c, r)
                    for (int filter_r = -filterWidth / 2; filter_r <= filterWidth / 2; ++filter_r) {
                        for (int filter_c = -filterWidth / 2; filter_c <= filterWidth / 2; ++filter_c) {
                            // Find the global image position for this filter
                            // position
                            // clamp to boundary of the image
                            int image_r = Math.min(Math.max(r + filter_r, 0), (numRows - 1));
                            int image_c = Math.min(Math.max(c + filter_c, 0), (numCols - 1));

                            float image_value = (channel[image_r * numCols + image_c]);
                            float filter_value = filter[(filter_r + filterWidth / 2) * filterWidth + filter_c + filterWidth / 2];

                            result += image_value * filter_value;
                        }
                    }
                    channelBlurred[r * numCols + c] = result > 255 ? 255 : (int) result;
                }
            }
        }

        private static void compute(int[] channel, int[] channelBlurred, final int numRows, final int numCols, float[] filter, final int filterWidth) {
            // For every pixel in the image
            assert (filterWidth % 2 == 1);

            for (@Parallel int r = 0; r < numRows; r++) {
                for (@Parallel int c = 0; c < numCols; c++) {
                    float result = 0.0f;
                    for (int filter_r = -filterWidth / 2; filter_r <= filterWidth / 2; filter_r++) {
                        for (int filter_c = -filterWidth / 2; filter_c <= filterWidth / 2; filter_c++) {
                            int image_r = Math.min(Math.max(r + filter_r, 0), (numRows - 1));
                            int image_c = Math.min(Math.max(c + filter_c, 0), (numCols - 1));
                            float image_value = (channel[image_r * numCols + image_c]);
                            float filter_value = filter[(filter_r + filterWidth / 2) * filterWidth + filter_c + filterWidth / 2];
                            result += image_value * filter_value;
                        }
                    }
                    channelBlurred[r * numCols + c] = result > 255 ? 255 : (int) result;
                }
            }
        }

        private void parallelCompute() {

            int w = image.getWidth();
            int h = image.getHeight();

            int[] redChannel = new int[w * h];
            int[] greenChannel = new int[w * h];
            int[] blueChannel = new int[w * h];
            int[] alphaChannel = new int[w * h];

            int[] redFilter = new int[w * h];
            int[] greenFilter = new int[w * h];
            int[] blueFilter = new int[w * h];

            float[] filter = new float[w * h];
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    filter[i * h + j] = 1.f / (FILTER_WIDTH * FILTER_WIDTH);
                }
            }

            // data initialisation
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    int rgb = image.getRGB(i, j);
                    alphaChannel[i * h + j] = (rgb >> 24) & 0xFF;
                    redChannel[i * h + j] = (rgb >> 16) & 0xFF;
                    greenChannel[i * h + j] = (rgb >> 8) & 0xFF;
                    blueChannel[i * h + j] = (rgb & 0xFF);
                }
            }

            long start = System.nanoTime();
            TornadoDevice device = TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(0);

            TaskSchedule parallelFilter = new TaskSchedule("blur") //
                    .task("red", BlurFilterImage::compute, redChannel, redFilter, w, h, filter, FILTER_WIDTH) //
                    .task("green", BlurFilterImage::compute, greenChannel, greenFilter, w, h, filter, FILTER_WIDTH) //
                    .task("blue", BlurFilterImage::compute, blueChannel, blueFilter, w, h, filter, FILTER_WIDTH) //
                    .streamOut(redFilter, greenFilter, blueFilter) //
                    .useDefaultThreadScheduler(true);

            parallelFilter.mapAllTo(device);
            parallelFilter.execute();

            // now recombine into the output image - Alpha is 255 for no
            // transparency
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    Color c = new Color(redFilter[i * h + j], greenFilter[i * h + j], blueFilter[i * h + j], alphaChannel[i * h + j]);
                    image.setRGB(i, j, c.getRGB());
                }
            }
            long end = System.nanoTime();
            System.out.println("Parallel Total time: \n\tns = " + (end - start) + "\n\tseconds = " + ((end - start) * 1e-9));

        }

        private void sequentialComputation() {

            int w = image.getWidth();
            int h = image.getHeight();

            int[] redChannel = new int[w * h];
            int[] greenChannel = new int[w * h];
            int[] blueChannel = new int[w * h];
            int[] alphaChannel = new int[w * h];

            int[] redFilter = new int[w * h];
            int[] greenFilter = new int[w * h];
            int[] blueFilter = new int[w * h];

            float[] filter = new float[w * h];
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    filter[i * h + j] = 1.f / (FILTER_WIDTH * FILTER_WIDTH);
                }
            }

            // data initialisation
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    int rgb = image.getRGB(i, j);
                    alphaChannel[i * h + j] = (rgb >> 24) & 0xFF;
                    redChannel[i * h + j] = (rgb >> 16) & 0xFF;
                    greenChannel[i * h + j] = (rgb >> 8) & 0xFF;
                    blueChannel[i * h + j] = (rgb & 0xFF);
                }
            }

            long start = System.nanoTime();
            channelConvolutionSequential(redChannel, redFilter, w, h, filter, FILTER_WIDTH);
            channelConvolutionSequential(greenChannel, greenFilter, w, h, filter, FILTER_WIDTH);
            channelConvolutionSequential(blueChannel, blueFilter, w, h, filter, FILTER_WIDTH);

            // now recombine into the output image - Alpha is 255 for no
            // transparency
            for (int i = 0; i < w; i++) {
                for (int j = 0; j < h; j++) {
                    Color c = new Color(redFilter[i * h + j], greenFilter[i * h + j], blueFilter[i * h + j], alphaChannel[i * h + j]);
                    image.setRGB(i, j, c.getRGB());
                }
            }
            long end = System.nanoTime();
            System.out.println("Sequential Total time: \n\tns = " + (end - start) + "\n\tseconds = " + ((end - start) * 1e-9));
        }

        @Override
        public void paint(Graphics g) {
            loadImage();
            if (PARALLEL_COMPUTATION) {
                parallelCompute();
            } else {
                sequentialComputation();
            }

            // draw the image
            g.drawImage(this.image, 0, 0, null);
        }

        @Override
        public Dimension getPreferredSize() {
            if (image == null) {
                return new Dimension(100, 100);
            } else {
                return new Dimension(image.getWidth(), image.getHeight());
            }
        }

    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("Blur Image Filter Example with TornadoVM");

        frame.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent event) {
                System.exit(0);
            }
        });

        frame.add(new BlurFilterImage());
        frame.pack();
        frame.setVisible(true);
    }

}
