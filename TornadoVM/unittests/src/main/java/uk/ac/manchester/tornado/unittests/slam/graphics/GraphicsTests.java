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

package uk.ac.manchester.tornado.unittests.slam.graphics;

import static org.junit.Assert.assertEquals;
import static uk.ac.manchester.tornado.api.collections.graphics.GraphicsMath.rigidTransform;
import static uk.ac.manchester.tornado.api.collections.math.TornadoMath.min;
import static uk.ac.manchester.tornado.api.collections.math.TornadoMath.sqrt;
import static uk.ac.manchester.tornado.api.collections.types.Float2.mult;
import static uk.ac.manchester.tornado.api.collections.types.Float3.add;
import static uk.ac.manchester.tornado.api.collections.types.Float3.length;
import static uk.ac.manchester.tornado.api.collections.types.Float3.normalise;

import java.util.Random;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.collections.graphics.GraphicsMath;
import uk.ac.manchester.tornado.api.collections.graphics.ImagingOps;
import uk.ac.manchester.tornado.api.collections.graphics.Renderer;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;
import uk.ac.manchester.tornado.api.collections.types.Byte3;
import uk.ac.manchester.tornado.api.collections.types.Byte4;
import uk.ac.manchester.tornado.api.collections.types.Float2;
import uk.ac.manchester.tornado.api.collections.types.Float3;
import uk.ac.manchester.tornado.api.collections.types.Float4;
import uk.ac.manchester.tornado.api.collections.types.Float6;
import uk.ac.manchester.tornado.api.collections.types.Float8;
import uk.ac.manchester.tornado.api.collections.types.FloatOps;
import uk.ac.manchester.tornado.api.collections.types.FloatSE3;
import uk.ac.manchester.tornado.api.collections.types.ImageByte3;
import uk.ac.manchester.tornado.api.collections.types.ImageByte4;
import uk.ac.manchester.tornado.api.collections.types.ImageFloat;
import uk.ac.manchester.tornado.api.collections.types.ImageFloat3;
import uk.ac.manchester.tornado.api.collections.types.ImageFloat4;
import uk.ac.manchester.tornado.api.collections.types.ImageFloat8;
import uk.ac.manchester.tornado.api.collections.types.Int2;
import uk.ac.manchester.tornado.api.collections.types.Int3;
import uk.ac.manchester.tornado.api.collections.types.Matrix4x4Float;
import uk.ac.manchester.tornado.api.collections.types.Short2;
import uk.ac.manchester.tornado.api.collections.types.VectorFloat3;
import uk.ac.manchester.tornado.api.collections.types.VectorFloat4;
import uk.ac.manchester.tornado.api.collections.types.VolumeOps;
import uk.ac.manchester.tornado.api.collections.types.VolumeShort2;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

public class GraphicsTests extends TornadoTestBase {

    private static void testPhiNode(ImageFloat3 vertices, ImageFloat depths, Matrix4x4Float invK) {
        final float depth = depths.get(0, 0);
        final Float3 pix = new Float3(0, 0, 1f);
        final Float3 vertex = (depth > 0) ? Float3.mult(rotate(invK, pix), depth) : new Float3(0f, 0f, 0f);
        vertices.set(0, 0, vertex);
    }

    private static void testPhiNode2(ImageFloat3 vertices, ImageFloat depths, Matrix4x4Float invK) {
        final float depth = depths.get(0, 0);
        final Float3 pix = new Float3(0, 0, 1f);
        final Float3 vertex = (depth > 0) ? Float3.mult(rotate(invK, pix), depth) : new Float3();
        vertices.set(0, 0, vertex);
    }

    @Test
    public void testMm2Meters() {
        int scaleFactor = 2;
        int srcSize = 100;
        int destSize = srcSize / scaleFactor;
        ImageFloat src = new ImageFloat(srcSize, srcSize);
        ImageFloat dest = new ImageFloat(destSize, destSize);

        ImageFloat destSeq = new ImageFloat(destSize, destSize);

        Random r = new Random();

        for (int i = 0; i < srcSize * srcSize; i++) {
            src.set(i, r.nextFloat());
        }

        // Sequential execution
        ImagingOps.mm2metersKernel(destSeq, src, scaleFactor);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", ImagingOps::mm2metersKernel, dest, src, scaleFactor)
                .streamOut(dest)
                .execute();
        // @formatter:on

        for (int i = 0; i < destSize * destSize; i++) {
            assertEquals(dest.get(i), destSeq.get(i), 0.001);
        }
    }

    private void generateGaussian(float[] gaussian, int radius, float delta) {
        for (int i = 0; i < gaussian.length; i++) {
            final int x = i - radius;
            gaussian[i] = (float) Math.exp(-(x * x) / (2 * delta * delta));
        }
    }

    @Test
    public void testBilateralFilter() {
        int scaleFactor = 2;
        int size = 100;
        int scaledSize = size / scaleFactor;
        ImageFloat src = new ImageFloat(scaledSize, scaledSize);
        ImageFloat dest = new ImageFloat(scaledSize, scaledSize);
        ImageFloat destSeq = new ImageFloat(scaledSize, scaledSize);

        float e_delta = 0.1f;
        int radius = 2;
        float delta = 4.0f;
        float[] gaussian = new float[(radius * 2) + 1];
        generateGaussian(gaussian, radius, delta);

        Random r = new Random();

        for (int i = 0; i < scaledSize * scaledSize; i++) {
            src.set(i, r.nextFloat());
        }

        // Sequential execution
        ImagingOps.bilateralFilter(destSeq, src, gaussian, e_delta, radius);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", ImagingOps::bilateralFilter, dest, src, gaussian, e_delta, radius)
                .streamOut(dest)
                .execute();
        // @formatter:on

        for (int i = 0; i < scaledSize * scaledSize; i++) {
            assertEquals("index = " + i, destSeq.get(i), dest.get(i), 0.001);
        }
    }

    @Test
    public void testResizeImage6() {
        int scaleFactor = 2;
        int size = 100;
        int scaledSize = size / scaleFactor;
        ImageFloat src = new ImageFloat(scaledSize, scaledSize);
        ImageFloat dest = new ImageFloat(scaledSize, scaledSize);
        ImageFloat destSeq = new ImageFloat(scaledSize, scaledSize);

        float e_delta = 0.1f;
        int radius = 2;
        float delta = 4.0f;
        float[] gaussian = new float[(radius * 2) + 1];
        generateGaussian(gaussian, radius, delta);

        Random r = new Random();

        for (int i = 0; i < scaledSize * scaledSize; i++) {
            src.set(i, r.nextFloat());
        }

        // Sequential execution
        ImagingOps.resizeImage6(destSeq, src, scaleFactor, e_delta * 3, radius);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", ImagingOps::resizeImage6, dest, src, scaleFactor, e_delta * 3, radius)
                .streamOut(dest)
                .execute();
        // @formatter:on

        for (int i = 0; i < scaledSize * scaledSize; i++) {
            assertEquals("index = " + i, destSeq.get(i), dest.get(i), 0.001);
        }
    }

    private static Float3 rotate(Matrix4x4Float m, Float3 v) {
        return new Float3(Float3.dot(m.row(0).asFloat3(), v), Float3.dot(m.row(1).asFloat3(), v), Float3.dot(m.row(2).asFloat3(), v));
    }

    private static void testRotate(Matrix4x4Float m, VectorFloat3 v, VectorFloat3 result) {
        for (@Parallel int i = 0; i < v.getLength(); i++) {
            Float3 r = rotate(m, v.get(i));
            result.set(i, r);
        }
    }

    @Test
    public void testRotate() {
        final int size = 4;
        Random r = new Random();

        Matrix4x4Float matrix4 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                matrix4.set(i, j, j + r.nextFloat());
            }
        }

        VectorFloat3 vector3 = new VectorFloat3(size);
        VectorFloat3 result = new VectorFloat3(size);
        VectorFloat3 sequential = new VectorFloat3(size);

        for (int i = 0; i < size; i++) {
            vector3.set(i, new Float3(1f, 2f, 3f));
        }

        // Sequential execution
        testRotate(matrix4, vector3, sequential);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::testRotate, matrix4, vector3, result)
            .streamOut(result)
            .execute();        
        // @formatter:on

        for (int i = 0; i < size; i++) {
            Float3 o = result.get(i);
            Float3 s = sequential.get(i);
            assertEquals(s.getS0(), o.getS0(), 0.001);
            assertEquals(s.getS1(), o.getS1(), 0.001);
            assertEquals(s.getS2(), o.getS2(), 0.001);
        }
    }

    @Test
    public void testDepth2Vertex() {

        final int size = 4;
        Random r = new Random();

        Matrix4x4Float matrix4 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                matrix4.set(i, j, j + r.nextFloat());
            }
        }

        ImageFloat3 vertext = new ImageFloat3(size, size);
        ImageFloat depth = new ImageFloat(size, size);

        ImageFloat3 sequential = new ImageFloat3(size, size);

        for (int i = 0; i < size; i++) {
            depth.set(i, r.nextFloat());
            for (int j = 0; j < size; j++) {
                vertext.set(i, j, new Float3(1f, 2f, 3f));
            }
        }

        // Sequential execution
        GraphicsMath.depth2vertex(sequential, depth, matrix4);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsMath::depth2vertex, vertext, depth, matrix4)
            .streamOut(vertext)
            .execute();        
        // @formatter:on

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Float3 o = vertext.get(i, j);
                Float3 s = sequential.get(i, j);
                assertEquals(s.getS0(), o.getS0(), 0.001);
                assertEquals(s.getS1(), o.getS1(), 0.001);
                assertEquals(s.getS2(), o.getS2(), 0.001);
            }
        }

    }

    @Test
    public void testVertex2Normal() {
        final int size = 100;

        ImageFloat3 pyramidNormals = new ImageFloat3(size, size);
        ImageFloat3 pyramidVertices = new ImageFloat3(size, size);

        ImageFloat3 sequentialNormals = new ImageFloat3(size, size);

        float min = -100;
        float max = 100;

        fillImageFloat3(pyramidVertices, min, max);

        // Sequential execution
        GraphicsMath.vertex2normal(sequentialNormals, pyramidVertices);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", GraphicsMath::vertex2normal, pyramidNormals, pyramidVertices)
                .streamOut(pyramidNormals)
                .execute();
        // @formatter:on

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Float3 o = pyramidNormals.get(i, j);
                Float3 s = sequentialNormals.get(i, j);
                assertEquals(s.getS0(), o.getS0(), 0.001);
                assertEquals(s.getS1(), o.getS1(), 0.001);
                assertEquals(s.getS2(), o.getS2(), 0.001);
            }
        }

    }

    private void fillImageFloat3(ImageFloat3 imageFloat3, float min, float max) {
        Random r = new Random();
        for (int i = 0; i < imageFloat3.X(); i++) {
            for (int j = 0; j < imageFloat3.Y(); j++) {
                float x = min + r.nextFloat() * (max - min);
                float y = min + r.nextFloat() * (max - min);
                float z = min + r.nextFloat() * (max - min);
                imageFloat3.set(i, j, new Float3(x, y, z));
            }
        }
    }

    private void fillMatrix4x4Float(Matrix4x4Float matrix, float min, float max) {
        Random r = new Random();
        for (int i = 0; i < matrix.N(); i++) {
            for (int j = 0; j < matrix.M(); j++) {
                float x = min + r.nextFloat() * (max - min);
                matrix.set(i, j, x);
            }
        }
    }

    public final class Constants {
        private Constants() {
        }

        public static final int X = 0;
        public static final int Y = 1;
        public static final int Z = 2;
        public static final int W = 3;

        public static final float INVALID = -2f;

        public static final int BLACK = -1;
        public static final int RED = -2;
        public static final int GREEN = -3;
        public static final int BLUE = -4;
        public static final int YELLOW = -5;
        public static final int GREY = 1;
    }

    public static void trackPose(final ImageFloat8 results, final ImageFloat3 verticies, final ImageFloat3 normals, final ImageFloat3 referenceVerticies, final ImageFloat3 referenceNormals,
            final Matrix4x4Float currentPose, final Matrix4x4Float view, final float distanceThreshold, final float normalThreshold) {

        final Float8 NO_INPUT = new Float8(0f, 0f, 0f, 0f, 0f, 0f, 0f, Constants.BLACK);
        final Float8 NOT_IN_IMAGE = new Float8(0f, 0f, 0f, 0f, 0f, 0f, 0f, Constants.RED);
        final Float8 NO_CORRESPONDENCE = new Float8(0f, 0f, 0f, 0f, 0f, 0f, 0f, Constants.GREEN);
        final Float8 TOO_FAR = new Float8(0f, 0f, 0f, 0f, 0f, 0f, 0f, Constants.BLUE);
        final Float8 WRONG_NORMAL = new Float8(0f, 0f, 0f, 0f, 0f, 0f, 0f, Constants.YELLOW);

        for (@Parallel int y = 0; y < results.Y(); y++) {
            for (@Parallel int x = 0; x < results.X(); x++) {

                if (normals.get(x, y).getX() == Constants.INVALID) {
                    results.set(x, y, NO_INPUT);
                } else {

                    // rotate + translate projected vertex
                    final Float3 projectedVertex = GraphicsMath.rigidTransform(currentPose, verticies.get(x, y));

                    // rotate + translate projected position
                    final Float3 projectedPos = GraphicsMath.rigidTransform(view, projectedVertex);

                    final Float2 projectedPixel = Float2.add(Float2.mult(projectedPos.asFloat2(), 1f / projectedPos.getZ()), 0.5f);

                    boolean isNotInImage = (projectedPixel.getX() < 0) || (projectedPixel.getX() > (referenceVerticies.X() - 1)) || (projectedPixel.getY() < 0)
                            || (projectedPixel.getY() > (referenceVerticies.Y() - 1));

                    if (isNotInImage) {
                        results.set(x, y, NOT_IN_IMAGE);
                    } else {

                        final Int2 refPixel = new Int2((int) projectedPixel.getX(), (int) projectedPixel.getY());

                        final Float3 referenceNormal = referenceNormals.get(refPixel.getX(), refPixel.getY());

                        if (referenceNormal.getX() == Constants.INVALID) {
                            results.set(x, y, NO_CORRESPONDENCE);
                        } else {

                            final Float3 diff = Float3.sub(referenceVerticies.get(refPixel.getX(), refPixel.getY()), projectedVertex);

                            if (Float3.length(diff) > distanceThreshold) {
                                results.set(x, y, TOO_FAR);
                            } else {

                                final Float3 projectedNormal = GraphicsMath.rotate(currentPose, normals.get(x, y));

                                if (Float3.dot(projectedNormal, referenceNormal) < normalThreshold) {
                                    results.set(x, y, WRONG_NORMAL);
                                } else {

                                    final Float3 b = Float3.cross(projectedVertex, referenceNormal);

                                    final Float8 tracking = new Float8(referenceNormal.getX(), referenceNormal.getY(), referenceNormal.getZ(), b.getX(), b.getY(), b.getZ(),
                                            Float3.dot(referenceNormal, diff), (float) Constants.GREY);

                                    results.set(x, y, tracking);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testTrackPose() {
        final int size = 100;

        ImageFloat8 pyramidTrackingResults = new ImageFloat8(size, size);
        ImageFloat3 pyramidNormals = new ImageFloat3(size, size);
        ImageFloat3 pyramidVertices = new ImageFloat3(size, size);
        ImageFloat3 referenceViewVertices = new ImageFloat3(size * 2, size * 2);
        ImageFloat3 referenceViewNormals = new ImageFloat3(size * 2, size * 2);
        Matrix4x4Float pyramidPose = new Matrix4x4Float();
        Matrix4x4Float projectReference = new Matrix4x4Float();
        float distanceThreshold = 0.1f;
        float normalThreshold = 0.8f;

        ImageFloat8 sequantialPyramidTrackingResults = new ImageFloat8(size, size);

        float min = -100;
        float max = 100;

        fillImageFloat3(pyramidNormals, min, max);
        fillImageFloat3(pyramidVertices, min, max);
        fillImageFloat3(referenceViewNormals, min, max);
        fillImageFloat3(referenceViewVertices, min, max);
        fillMatrix4x4Float(pyramidPose, min, max);
        fillMatrix4x4Float(projectReference, min, max);

        // Sequential execution
        trackPose(sequantialPyramidTrackingResults, pyramidVertices, pyramidNormals, referenceViewVertices, referenceViewNormals, pyramidPose, projectReference, distanceThreshold, normalThreshold);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", GraphicsTests::trackPose, pyramidTrackingResults, pyramidVertices, pyramidNormals,
                        referenceViewVertices, referenceViewNormals, pyramidPose,
                        projectReference, distanceThreshold, normalThreshold)
                .streamOut(pyramidTrackingResults)
                .execute();
        // @formatter:on

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Float8 o = pyramidTrackingResults.get(i);
                Float8 s = sequantialPyramidTrackingResults.get(i);
                assertEquals(s.getS0(), o.getS0(), 0.001);
                assertEquals(s.getS1(), o.getS1(), 0.001);
                assertEquals(s.getS2(), o.getS2(), 0.001);
                assertEquals(s.getS3(), o.getS3(), 0.001);
                assertEquals(s.getS4(), o.getS4(), 0.001);
                assertEquals(s.getS5(), o.getS5(), 0.001);
                assertEquals(s.getS6(), o.getS6(), 0.001);
                assertEquals(s.getS7(), o.getS7(), 0.001);
            }
        }
    }

    @Test
    public void testPhiNode() {

        final int size = 4;
        Random r = new Random();

        Matrix4x4Float matrix4 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                matrix4.set(i, j, j + r.nextFloat());
            }
        }

        ImageFloat3 vertices = new ImageFloat3(size, size);
        ImageFloat3 verticesSeq = new ImageFloat3(size, size);
        ImageFloat depth = new ImageFloat(size, size);

        for (int i = 0; i < size; i++) {
            depth.set(i, r.nextFloat());
            for (int j = 0; j < size; j++) {
                vertices.set(i, j, new Float3(1f, 2f, 3f));
            }
        }

        GraphicsTests.testPhiNode(verticesSeq, depth, matrix4);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::testPhiNode, vertices, depth, matrix4)
            .streamOut(vertices)
            .execute();        
        // @formatter:on

        Float3 o = vertices.get(0);
        Float3 s = verticesSeq.get(0);
        Assert.assertEquals(s.getS0(), o.getS0(), 0.01);
        Assert.assertEquals(s.getS1(), o.getS1(), 0.01);
        Assert.assertEquals(s.getS2(), o.getS2(), 0.01);

    }

    @Test
    public void testPhiNode2() {

        final int size = 4;
        Random r = new Random();

        Matrix4x4Float matrix4 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                matrix4.set(i, j, j + r.nextFloat());
            }
        }

        ImageFloat3 vertices = new ImageFloat3(size, size);
        ImageFloat3 verticesSeq = new ImageFloat3(size, size);
        ImageFloat depth = new ImageFloat(size, size);

        for (int i = 0; i < size; i++) {
            depth.set(i, r.nextFloat());
            for (int j = 0; j < size; j++) {
                vertices.set(i, j, new Float3(1f, 2f, 3f));
            }
        }

        GraphicsTests.testPhiNode2(verticesSeq, depth, matrix4);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::testPhiNode2, vertices, depth, matrix4)
            .streamOut(vertices)
            .execute();        
        // @formatter:on

        Float3 o = vertices.get(0);
        Float3 s = verticesSeq.get(0);
        Assert.assertEquals(s.getS0(), o.getS0(), 0.01);
        Assert.assertEquals(s.getS1(), o.getS1(), 0.01);
        Assert.assertEquals(s.getS2(), o.getS2(), 0.01);

    }

    public static void computeRigidTransform(Matrix4x4Float matrix, VectorFloat3 points, VectorFloat3 output) {
        for (@Parallel int i = 0; i < points.getLength(); i++) {
            Float3 p = GraphicsMath.rigidTransform(matrix, points.get(i));
            output.set(i, p);
        }
    }

    @Test
    public void testRigidTrasform() {

        final int size = 4;
        Random r = new Random();

        Matrix4x4Float matrix4 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                matrix4.set(i, j, j + r.nextFloat());
            }
        }

        VectorFloat3 point = new VectorFloat3(size);
        for (int i = 0; i < 4; i++) {
            point.set(i, new Float3(r.nextFloat(), r.nextFloat(), r.nextFloat()));
        }

        VectorFloat3 sequential = new VectorFloat3(size);
        VectorFloat3 output = new VectorFloat3(size);

        // Sequential execution
        computeRigidTransform(matrix4, point, sequential);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::computeRigidTransform, matrix4, point, output)
            .streamOut(output)
            .execute();        
        // @formatter:on

        for (int i = 0; i < size; i++) {
            Float3 o = output.get(i);
            Float3 s = sequential.get(i);
            assertEquals(s.getS0(), o.getS0(), 0.001);
            assertEquals(s.getS1(), o.getS1(), 0.001);
            assertEquals(s.getS2(), o.getS2(), 0.001);
        }
    }

    public static void testNormaliseFunction(VectorFloat3 input, VectorFloat3 output) {
        for (@Parallel int i = 0; i < input.getLength(); i++) {
            final Float3 norm = Float3.normalise(input.get(i));
            final Float3 res = new Float3(norm.getX(), norm.getY(), norm.getZ());
            output.set(i, res);
        }
    }

    @Test
    public void testNormalise() {
        int size = 128;
        Random r = new Random();
        int min = -100;
        int max = 100;
        float x = min + r.nextFloat() * (max - min);
        float y = min + r.nextFloat() * (max - min);
        float z = min + r.nextFloat() * (max - min);

        VectorFloat3 input = new VectorFloat3(size);
        VectorFloat3 outSeq = new VectorFloat3(size);
        VectorFloat3 out = new VectorFloat3(size);

        for (int i = 0; i < size; i++) {
            input.set(i, new Float3(x, y, z));
        }

        // Sequential execution
        testNormaliseFunction(input, outSeq);

        // @formatter:off
        new TaskSchedule("s0")
                .task("t0", GraphicsTests::testNormaliseFunction, input, out)
                .streamOut(out)
                .execute();
        // @formatter:on

        for (int i = 0; i < size; i++) {
            Float3 o = out.get(i);
            Float3 s = outSeq.get(i);
            assertEquals(s.getS0(), o.getS0(), 0.001);
            assertEquals(s.getS1(), o.getS1(), 0.001);
            assertEquals(s.getS2(), o.getS2(), 0.001);
        }
    }

    // Ray Cast testing

    private static final float INVALID = -2;

    public static final void raycast(ImageFloat3 verticies, ImageFloat3 normals, VolumeShort2 volume, Float3 volumeDims, Matrix4x4Float view, float nearPlane, float farPlane, float largeStep,
            float smallStep) {

        // use volume model to generate a reference view by raycasting ...
        for (@Parallel int y = 0; y < verticies.Y(); y++) {
            for (@Parallel int x = 0; x < verticies.X(); x++) {
                final Float4 hit = GraphicsMath.raycastPoint(volume, volumeDims, x, y, view, nearPlane, farPlane, smallStep, largeStep);

                final Float3 normal;
                final Float3 position;
                if (hit.getW() > 0f) {
                    position = hit.asFloat3();

                    final Float3 surfNorm = VolumeOps.grad(volume, volumeDims, position);

                    // final Float3 surfNorm = new Float3(0f, 0f, 0f);

                    if (length(surfNorm) != 0) {
                        normal = normalise(surfNorm);
                    } else {
                        normal = new Float3(INVALID, 0f, 0f);
                    }
                } else {
                    normal = new Float3(INVALID, 0f, 0f);
                    position = new Float3();
                }

                verticies.set(x, y, position);
                normals.set(x, y, normal);

            }
        }
    }

    @Test
    public void raycastTest() {

        final int size = 128;
        Random r = new Random();

        Matrix4x4Float view = new Matrix4x4Float();
        ImageFloat3 verticies = new ImageFloat3(size, size);
        ImageFloat3 normals = new ImageFloat3(size, size);
        VolumeShort2 volume = new VolumeShort2(size, size, size);
        Float3 volumeDims = new Float3(r.nextFloat(), r.nextFloat(), r.nextFloat());

        float nearPlane = r.nextFloat();
        float farPlane = r.nextFloat();
        float largeStep = r.nextFloat();
        float smallStep = r.nextFloat();

        ImageFloat3 verticiesSequential = new ImageFloat3(size, size);
        ImageFloat3 normalsSequential = new ImageFloat3(size, size);

        float min = -100;
        float max = 100;

        fillMatrix4x4Float(view, min, max);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    volume.set(i, j, k, new Short2((short) (r.nextInt() + i + j + k), (short) (r.nextInt() + i + j - k)));
                }
            }
        }

        GraphicsTests.raycast(verticiesSequential, normalsSequential, volume, volumeDims, view, nearPlane, farPlane, largeStep, smallStep);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::raycast, verticies, normals, volume, volumeDims, view, nearPlane, farPlane, largeStep, smallStep)
            .streamOut(verticies, normals)
            .execute();        
        // @formatter:on

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Float3 o = verticies.get(i, j);
                Float3 s = verticiesSequential.get(i, j);
                assertEquals(s.getS0(), o.getS0(), 0.001);
                assertEquals(s.getS1(), o.getS1(), 0.001);
                assertEquals(s.getS2(), o.getS2(), 0.001);
            }
        }

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Float3 o = normals.get(i, j);
                Float3 s = normalsSequential.get(i, j);
                assertEquals(s.getS0(), o.getS0(), 0.001);
                assertEquals(s.getS1(), o.getS1(), 0.001);
                assertEquals(s.getS2(), o.getS2(), 0.001);
            }
        }
    }

    public static final void testRayCastPointIsolation(ImageFloat4 output, ImageFloat3 verticies, VolumeShort2 volume, Float3 volumeDims, Matrix4x4Float view, float nearPlane, float farPlane,
            float largeStep, float smallStep) {

        // use volume model to generate a reference view by raycasting ...
        for (@Parallel int i = 0; i < verticies.X(); i++) {
            for (@Parallel int j = 0; j < verticies.Y(); j++) {
                final Float4 hit = GraphicsMath.raycastPoint(volume, volumeDims, i, j, view, nearPlane, farPlane, smallStep, largeStep);
                output.set(i, j, hit);
            }
        }
    }

    @Test
    public void testRaycastPoint() {

        final int size = 128;
        Random r = new Random();

        Matrix4x4Float view = new Matrix4x4Float();
        ImageFloat3 verticies = new ImageFloat3(size, size);
        VolumeShort2 volume = new VolumeShort2(size, size, size);
        Float3 volumeDims = new Float3(r.nextFloat(), r.nextFloat(), r.nextFloat());

        float nearPlane = r.nextFloat();
        float farPlane = r.nextFloat();
        float largeStep = r.nextFloat();
        float smallStep = r.nextFloat();

        ImageFloat4 output = new ImageFloat4(size, size);
        ImageFloat4 outputSeq = new ImageFloat4(size, size);

        float min = -100;
        float max = 100;

        fillMatrix4x4Float(view, min, max);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    volume.set(i, j, k, new Short2((short) (r.nextInt() + i + j + k), (short) (r.nextInt() + i + j - k)));
                }
            }
        }

        GraphicsTests.testRayCastPointIsolation(outputSeq, verticies, volume, volumeDims, view, nearPlane, farPlane, largeStep, smallStep);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::testRayCastPointIsolation, output, verticies, volume, volumeDims, view, nearPlane, farPlane, largeStep, smallStep)
            .streamOut(output)
            .execute();        
        // @formatter:on

        for (int i = 0; i < output.X(); i++) {
            for (int j = 0; j < output.Y(); j++) {
                Float4 o = output.get(i, j);
                Float4 s = outputSeq.get(i, j);
                assertEquals(s.getX(), o.getX(), 0.001);
                assertEquals(s.getY(), o.getY(), 0.001);
                assertEquals(s.getZ(), o.getZ(), 0.001);
                assertEquals(s.getW(), o.getW(), 0.001);
            }
        }
    }

    public static void integrate(ImageFloat filteredDepthImage, Matrix4x4Float invTrack, Matrix4x4Float K, Float3 volumeDims, VolumeShort2 volume, float mu, float maxWeight) {
        final Float3 tmp = new Float3(0f, 0f, volumeDims.getZ() / volume.Z());

        final Float3 integrateDelta = rotate(invTrack, tmp);
        final Float3 cameraDelta = rotate(K, integrateDelta);

        for (@Parallel int y = 0; y < volume.Y(); y++) {
            for (@Parallel int x = 0; x < volume.X(); x++) {

                final Int3 pix = new Int3(x, y, 0);
                Float3 pos = rigidTransform(invTrack, pos(volume, volumeDims, pix));
                Float3 cameraX = rigidTransform(K, pos);

                for (int z = 0; z < volume.Z(); z++, pos = add(pos, integrateDelta), cameraX = add(cameraX, cameraDelta)) {

                    if (pos.getZ() < 0.0001f) // arbitrary near plane constant
                    {
                        continue;
                    }

                    final Float2 pixel = new Float2((cameraX.getX() / cameraX.getZ()) + 0.5f, (cameraX.getY() / cameraX.getZ()) + 0.5f);

                    if ((pixel.getX() < 0) || (pixel.getX() > (filteredDepthImage.X() - 1)) || (pixel.getY() < 0) || (pixel.getY() > (filteredDepthImage.Y() - 1))) {
                        continue;
                    }

                    final Int2 px = new Int2((int) pixel.getX(), (int) pixel.getY());

                    final float depth = filteredDepthImage.get(px.getX(), px.getY());

                    if (depth == 0) {
                        continue;
                    }

                    final float diff = (depth - cameraX.getZ()) * sqrt(1f + FloatOps.sq(pos.getX() / pos.getZ()) + FloatOps.sq(pos.getY() / pos.getZ()));

                    if (diff > -mu) {

                        final float sdf = min(1f, diff / mu);

                        final Short2 inputValue = volume.get(x, y, z);
                        final Float2 constantValue1 = new Float2(0.00003051944088f, 1f);
                        final Float2 constantValue2 = new Float2(32766.0f, 1f);

                        final Float2 data = mult(new Float2(inputValue.getX(), inputValue.getY()), constantValue1);

                        final float dx = TornadoMath.clamp(((data.getY() * data.getX()) + sdf) / (data.getY() + 1f), -1f, 1f);
                        final float dy = min(data.getY() + 1f, maxWeight);

                        final Float2 floatValue = mult(new Float2(dx, dy), constantValue2);
                        final Short2 outputValue = new Short2((short) floatValue.getX(), (short) floatValue.getY());

                        volume.set(x, y, z, outputValue);
                    }
                }
            }
        }
    }

    private static Float3 pos(final VolumeShort2 volume, final Float3 volumeDims, final Int3 p) {
        return new Float3(((p.getX() + 0.5f) * volumeDims.getX()) / volume.X(), ((p.getY() + 0.5f) * volumeDims.getY()) / volume.Y(), ((p.getZ() + 0.5f) * volumeDims.getZ()) / volume.Z());
    }

    private float random(Random r) {
        return r.nextFloat() * 10;
    }

    private float random(Random r, float min, float max) {
        return min + r.nextFloat() * (max - min);
    }

    @Test
    public void testIntegrate() {

        final int size = 100;
        Random r = new Random();

        Matrix4x4Float invTrack = new Matrix4x4Float();
        Matrix4x4Float m2 = new Matrix4x4Float();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                invTrack.set(i, j, j + random(r));
                m2.set(i, j, j + random(r));
            }
        }

        ImageFloat filteredDepthImage = new ImageFloat(size, size);
        VolumeShort2 volume = new VolumeShort2(size, size, size);
        VolumeShort2 sequential = new VolumeShort2(size, size, size);
        Float3 volumeDims = new Float3(random(r), random(r), random(r));

        float mu = random(r);
        float maxW = random(r);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                filteredDepthImage.set(i, j, random(r));
            }
        }

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    volume.set(i, j, k, new Short2((short) 2, (short) 3));
                    sequential.set(i, j, k, new Short2((short) 2, (short) 3));
                }
            }
        }

        integrate(filteredDepthImage, invTrack, m2, volumeDims, sequential, mu, maxW);

        // @formatter:off
        TaskSchedule task = new TaskSchedule("s0")
            .task("t0", GraphicsTests::integrate, filteredDepthImage, invTrack, m2, volumeDims, volume, mu, maxW)
            .streamOut(volume);        
        // @formatter:on

        int c = 0;
        while (c++ < 10) {
            task.execute();
        }

        // Check result
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    Short2 got = volume.get(i, j, k);
                    Short2 expected = sequential.get(i, j, k);
                    assertEquals(expected.getX(), got.getX());
                    assertEquals(expected.getY(), got.getY());
                }
            }
        }
    }

    @Test
    public void testRenderTrack() {
        final int size = 4;
        Random r = new Random();

        ImageByte3 output = new ImageByte3(size, size);
        ImageByte3 sequential = new ImageByte3(size, size);
        ImageFloat8 track = new ImageFloat8(size, size);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                track.set(i, j, new Float8(random(r), random(r), random(r), random(r), random(r), random(r), random(r), random(r)));
            }
        }

        Renderer.renderTrack(sequential, track);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", Renderer::renderTrack, output, track)
            .streamOut(output)
            .execute();        
        // @formatter:on

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                Byte3 o = output.get(i, j);
                Byte3 expected = sequential.get(i, j);
                assertEquals(expected.getX(), o.getX());
                assertEquals(expected.getY(), o.getY());
                assertEquals(expected.getZ(), o.getZ());
            }
        }
    }

    public static void volumeOps(VectorFloat3 output, VolumeShort2 volume, final Float3 dim, final Float3 point) {
        for (@Parallel int i = 0; i < output.getLength(); i++) {
            Float3 f = VolumeOps.grad(volume, dim, point);
            output.set(i, f);
        }
    }

    @Test
    public void testVolumeGrad() {
        final int size = 128;
        Random r = new Random();

        float min = 1;
        float max = 2;

        VolumeShort2 volume = new VolumeShort2(size, size, size);
        VectorFloat3 output = new VectorFloat3(size * size);
        VectorFloat3 outputSeq = new VectorFloat3(size * size);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    volume.set(i, j, k, new Short2((short) random(r, min, max), (short) random(r, min, max)));
                }
            }
        }

        Float3 dim = new Float3(random(r, min, max), random(r, min, max), random(r, min, max));
        Float3 point = new Float3(random(r, 0, 1), random(r, 0, 1), random(r, 0, 1));

        GraphicsTests.volumeOps(outputSeq, volume, dim, point);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::volumeOps, output, volume, dim, point)
            .streamOut(output)
            .execute();
        // @formatter:on

        for (int i = 0; i < output.getLength(); i++) {
            Float3 o = output.get(i);
            Float3 s = outputSeq.get(i);
            Assert.assertEquals("difference on index " + i + " s0", s.getS0(), o.getS0(), 0.01f);
            Assert.assertEquals("difference on index " + i + " s1", s.getS1(), o.getS1(), 0.01f);
            Assert.assertEquals("difference on index " + i + " s2", s.getS2(), o.getS2(), 0.01f);
        }

    }

    /**
     * * Creates a 4x4 matrix representing the intrinsic camera matrix
     *
     * @param k
     *            - camera parameters {f_x,f_y,x_0,y_0} where {f_x,f_y} specifies
     *            the focal length of the camera and {x_0,y_0} the principle point
     * @param m
     *            - returned matrix
     */
    public static void getCameraMatrix(Float4 k, Matrix4x4Float m) {
        m.fill(0f);

        // focal length - f_x
        m.set(0, 0, k.getX());
        // focal length - f_y
        m.set(1, 1, k.getY());

        // principle point - x_0
        m.set(0, 2, k.getZ());

        // principle point - y_0
        m.set(1, 2, k.getW());

        m.set(2, 2, 1);
        m.set(3, 3, 1);
    }

    @Test
    public void testCameraMatrix() {

        Float4 f = new Float4(1f, 2f, 3f, 4f);
        Matrix4x4Float m = new Matrix4x4Float();
        Matrix4x4Float seq = new Matrix4x4Float();

        GraphicsTests.getCameraMatrix(f, seq);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::getCameraMatrix, f, m)
            .streamOut(m)
            .execute();        
        // @formatter:on

        for (int i = 0; i < m.N(); i++) {
            for (int j = 0; j < m.M(); j++) {
                Assert.assertEquals(seq.get(i, j), m.get(i, j), 0.01f);
            }
        }

    }

    private Matrix4x4Float getInitPose(Float3 volumeDims) {
        Float6 pos = new Float6();
        Float3 float3Pos = Float3.mult(new Float3(0.5f, 0.5f, 0), volumeDims);
        pos.setS0(float3Pos.getX());
        pos.setS1(float3Pos.getY());
        pos.setS2(float3Pos.getZ());
        Matrix4x4Float view = new FloatSE3(pos).toMatrix4();
        return view;
    }

    @Test
    public void testRenderVolume() {
        final int size = 128;
        Random r = new Random();

        Int3 volumeSize = new Int3(256, 256, 256);
        ImageByte4 output = new ImageByte4(size, size);
        ImageByte4 outputSeq = new ImageByte4(size, size);

        VolumeShort2 volume = new VolumeShort2(volumeSize.getX(), volumeSize.getY(), volumeSize.getZ());
        VolumeShort2 volumeSeq = new VolumeShort2(volumeSize.getX(), volumeSize.getY(), volumeSize.getZ());
        Float3 volumeDims = new Float3(5.0f, 5.0f, 5.0f);
        Float3 volumeDimsSeq = new Float3(5.0f, 5.0f, 5.0f);
        Matrix4x4Float scenePose = getInitPose(volumeDims);
        Matrix4x4Float scenePoseSeq = getInitPose(volumeDimsSeq);
        float nearPlane = 0.4f;
        float farPlane = 4.0f * 2;
        float largeStep = 0.75f * 0.1f;
        float smallStep = Float3.min(volumeDims) / Int3.max(volumeSize);
        Float3 light = new Float3(1.0f, 1.0f, -1.0f);
        Float3 ambient = new Float3(0.1f, 0.1f, 0.1f);

        for (int i = 0; i < volume.X(); i++) {
            for (int j = 0; j < volume.Y(); j++) {
                for (int z = 0; z < volume.Z(); z++) {
                    short r1 = (short) r.nextInt();
                    short r2 = (short) r.nextInt();
                    volume.set(i, j, z, new Short2(r1, r2));
                    volumeSeq.set(i, j, z, new Short2(r1, r2));
                }
            }
        }

        Renderer.renderVolume(outputSeq, volumeSeq, volumeDimsSeq, scenePoseSeq, nearPlane, farPlane * 2f, smallStep, largeStep, light, ambient);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", Renderer::renderVolume, output, volume, volumeDims, scenePose, nearPlane, farPlane * 2f, smallStep, largeStep, light, ambient)
            .streamOut(output)
            .execute();        
        // @formatter:on

        for (int i = 0; i < output.X(); i++) {
            for (int j = 0; j < output.Y(); j++) {
                Byte4 o = output.get(i, j);
                Byte4 s = outputSeq.get(i, j);
                Assert.assertEquals("index = " + i + ", " + j, s.getX(), o.getX());
                Assert.assertEquals("index = " + i + ", " + j, s.getY(), o.getY());
                Assert.assertEquals("index = " + i + ", " + j, s.getZ(), o.getZ());
                Assert.assertEquals("index = " + i + ", " + j, s.getW(), o.getW());
            }
        }
    }

    public static void reduceValues(final float[] sums, final int startIndex, final ImageFloat8 trackingResults, int resultIndex) {

        final int jtj = startIndex + 7;
        final int info = startIndex + 28;

        Float8 value = trackingResults.get(resultIndex);
        final int result = (int) value.getS7();
        final float error = value.getS6();

        if (result < 1) {
            sums[info + 1] += (result == -4) ? 1 : 0;
            sums[info + 2] += (result == -5) ? 1 : 0;
            sums[info + 3] += (result > -4) ? 1 : 0;
            return;
        }

        // float base[0] += error^2
        sums[startIndex] += (error * error);

        // Float6 base(+1) += row.scale(error)
        // for (int i = 0; i < 6; i++) {
        // sums[startIndex + i + 1] += error * value.get(i);
        // sums[startIndex + i + 1] = value.get(i);
        // }

        sums[startIndex + 0 + 1] += (error * value.getS0());
        sums[startIndex + 1 + 1] += (error * value.getS1());
        sums[startIndex + 2 + 1] += (error * value.getS2());
        sums[startIndex + 3 + 1] += (error * value.getS3());
        sums[startIndex + 4 + 1] += (error * value.getS4());
        sums[startIndex + 5 + 1] += (error * value.getS5());

        // is this jacobian transpose jacobian?
        sums[jtj + 0] += (value.getS0() * value.getS0());
        sums[jtj + 1] += (value.getS0() * value.getS1());
        sums[jtj + 2] += (value.getS0() * value.getS2());
        sums[jtj + 3] += (value.getS0() * value.getS3());
        sums[jtj + 4] += (value.getS0() * value.getS4());
        sums[jtj + 5] += (value.getS0() * value.getS5());

        sums[jtj + 6] += (value.getS1() * value.getS1());
        sums[jtj + 7] += (value.getS1() * value.getS2());
        sums[jtj + 8] += (value.getS1() * value.getS3());
        sums[jtj + 9] += (value.getS1() * value.getS4());
        sums[jtj + 10] += (value.getS1() * value.getS5());

        sums[jtj + 11] += (value.getS2() * value.getS2());
        sums[jtj + 12] += (value.getS2() * value.getS3());
        sums[jtj + 13] += (value.getS2() * value.getS4());
        sums[jtj + 14] += (value.getS2() * value.getS5());

        sums[jtj + 15] += (value.getS3() * value.getS3());
        sums[jtj + 16] += (value.getS3() * value.getS4());
        sums[jtj + 17] += (value.getS3() * value.getS5());

        sums[jtj + 18] += (value.getS4() * value.getS4());
        sums[jtj + 19] += (value.getS4() * value.getS5());

        sums[jtj + 20] += (value.getS5() * value.getS5());

        sums[info]++;
    }

    public static void mapReduce(final float[] output, final ImageFloat8 input) {
        final int numThreads = output.length / 32;
        final int numElements = input.X() * input.Y();

        for (@Parallel int i = 0; i < numThreads; i++) {
            final int startIndex = i * 32;
            for (int j = 0; j < 32; j++) {
                output[startIndex + j] = 0f;
            }

            for (int j = i; j < numElements; j += numThreads) {
                reduceValues(output, startIndex, input, j);
            }
        }
    }

    public static void mapReduce2(float[] output, ImageFloat8 input) {
        int startIndex = 0 * 32;
        reduceValues(output, startIndex, input, 0);
    }

    public static void mapReduce3(VectorFloat4 output, final VectorFloat4 input) {
        for (@Parallel int i = 0; i < input.getLength(); i++) {
            Float4 f = input.get(i);
            Float4 ff = new Float4(f.get(3), f.get(2), f.get(1), f.get(0));
            output.set(i, ff);
        }
    }

    private Float8 createFloat8() {
        Random r = new Random();
        return new Float8(random(r), random(r), random(r), random(r), random(r), random(r), random(r), random(r));
    }

    private Float4 createFloat4() {
        Random r = new Random();
        return new Float4(random(r), random(r), random(r), random(r));
    }

    @Test
    public void testMapReduceSlam() {

        final int size = 64;
        float[] output = new float[size];
        float[] outputSeq = new float[size];

        ImageFloat8 image = new ImageFloat8(size, size);

        for (int i = 0; i < image.X(); i++) {
            for (int j = 0; j < image.X(); j++) {
                Float8 f = createFloat8();
                image.set(i, j, f);
            }
        }

        GraphicsTests.mapReduce(outputSeq, image);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::mapReduce, output, image)
            .streamOut(output)
            .execute();        
        // @formatter:on

        Assert.assertArrayEquals(outputSeq, output, 0.1f);
    }

    @Test
    public void testMapReduceSlam2() {

        final int size = 64;
        float[] output = new float[size];
        float[] outputSeq = new float[size];

        ImageFloat8 image = new ImageFloat8(size, size);
        for (int i = 0; i < image.X(); i++) {
            for (int j = 0; j < image.X(); j++) {
                Float8 f = createFloat8();
                image.set(i, j, f);
            }
        }

        GraphicsTests.mapReduce2(outputSeq, image);

        // @formatter:off
        TaskSchedule ts = new TaskSchedule("s0")
            .task("t0", GraphicsTests::mapReduce2, output, image)
            .streamOut(output);
        // @formatter:on

        ts.execute();

        Assert.assertArrayEquals(outputSeq, output, 0.01f);
    }

    @Ignore
    public void testMapReduceSlam3() {

        final int size = 16;

        VectorFloat4 input = new VectorFloat4(size);
        VectorFloat4 output = new VectorFloat4(size);

        for (int i = 0; i < input.getLength(); i++) {
            Float4 f = createFloat4();
            input.set(i, f);
        }

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::mapReduce3, output, input)
            .streamOut(output)
            .execute();        
        // @formatter:on
    }

    public static void testVSKernel(int[] x, int[] y, int[] z, VolumeShort2 v, float[] output) {
        for (@Parallel int i = 0; i < x.length; i++) {
            output[i] = VolumeOps.vs1(x[i], y[i], z[i], v);
        }
    }

    @Test
    public void testVS() {

        int size = 256;
        int[] x = new int[size];
        int[] y = new int[size];
        int[] z = new int[size];

        float[] output = new float[size];
        float[] seq = new float[size];

        IntStream.range(0, size).parallel().forEach(i -> {
            x[i] = i;
            y[i] = i;
            z[i] = i;
        });

        VolumeShort2 volume = new VolumeShort2(size, size, size);

        for (int i = 0; i < volume.X(); i++) {
            for (int j = 0; j < volume.Y(); j++) {
                for (int k = 0; k < volume.Z(); k++) {
                    volume.set(i, j, k, new Short2((short) 1, (short) 2));
                }
            }
        }

        testVSKernel(x, y, z, volume, seq);

        // @formatter:off
        new TaskSchedule("s0")
            .task("t0", GraphicsTests::testVSKernel, x, y, z, volume, output)
            .streamOut(output)
            .execute();        
        // @formatter:on

        for (int i = 0; i < output.length; i++) {
            assertEquals(seq[i], output[i], 0.001f);
        }
    }

}
