package uk.ac.manchester.tornado.unittests.arrays;

import org.junit.Test;
import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.unittests.common.TornadoTestBase;

import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class TestBasicOperations extends TornadoTestBase {

    public static void vectorAddDouble(double[] a, double[] b, double[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] + b[i];
        }
    }

    public static void vectorSubDouble(double[] a, double[] b, double[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] - b[i];
        }
    }

    public static void vectorMulDouble(double[] a, double[] b, double[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] * b[i];
        }
    }

    public static void vectorDivDouble(double[] a, double[] b, double[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] / b[i];
        }
    }

    public static void vectorAddFloat(float[] a, float[] b, float[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] + b[i];
        }
    }

    public static void vectorSubFloat(float[] a, float[] b, float[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] - b[i];
        }
    }

    public static void vectorMulFloat(float[] a, float[] b, float[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] * b[i];
        }
    }

    public static void vectorDivFloat(float[] a, float[] b, float[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] / b[i];
        }
    }

    public static void vectorAddInteger(int[] a, int[] b, int[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] + b[i];
        }
    }

    public static void vectorSubInteger(int[] a, int[] b, int[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] - b[i];
        }
    }

    public static void vectorMulInteger(int[] a, int[] b, int[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] * b[i];
        }
    }

    public static void vectorDivInteger(int[] a, int[] b, int[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] / b[i];
        }
    }

    public static void vectorAddLong(long[] a, long[] b, long[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] + b[i];
        }
    }

    public static void vectorSubLong(long[] a, long[] b, long[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] - b[i];
        }
    }

    public static void vectorMulLong(long[] a, long[] b, long[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] * b[i];
        }
    }

    public static void vectorDivLong(long[] a, long[] b, long[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = a[i] / b[i];
        }
    }

    public static void vectorAddShort(short[] a, short[] b, short[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = (short) (a[i] + b[i]);
        }
    }

    public static void vectorSubShort(short[] a, short[] b, short[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = (short) (a[i] - b[i]);
        }
    }

    public static void vectorMulShort(short[] a, short[] b, short[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = (short) (a[i] * b[i]);
        }
    }

    public static void vectorDivShort(short[] a, short[] b, short[] c) {
        for (@Parallel int i = 0; i < c.length; i++) {
            c[i] = (short) (a[i] / b[i]);
        }
    }

    public static void vectorAddChar(char[] a, char[] b, char[] c) {
        for (@Parallel int i = 0; i < a.length; i++) {
            c[i] = (char) (a[i] + b[i]);
        }
    }

    public static void vectorSubChar(char[] a, char[] b, char[] c) {
        for (@Parallel int i = 0; i < a.length; i++) {
            c[i] = (char) (a[i] - b[i]);
        }
    }

    public static void vectorMulChar(char[] a, char[] b, char[] c) {
        for (@Parallel int i = 0; i < a.length; i++) {
            c[i] = (char) (a[i] * b[i]);
        }
    }

    public static void vectorDivChar(char[] a, char[] b, char[] c) {
        for (@Parallel int i = 0; i < a.length; i++) {
            c[i] = (char) (a[i] / b[i]);
        }
    }

    @Test
    public void testVectorAdditionDouble() {
        final int numElements = 32;
        double[] a = new double[numElements];
        double[] b = new double[numElements];
        double[] c = new double[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddDouble, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] + b[i], c[i], 0.01);
        }
    }

    @Test
    public void testVectorSubtractionDouble() {
        final int numElements = 32;
        double[] a = new double[numElements];
        double[] b = new double[numElements];
        double[] c = new double[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubDouble, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] - b[i], c[i], 0.01);
        }
    }

    @Test
    public void testVectorMultiplicationDouble() {
        final int numElements = 32;
        double[] a = new double[numElements];
        double[] b = new double[numElements];
        double[] c = new double[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulDouble, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] * b[i], c[i], 0.01);
        }
    }

    @Test
    public void testVectorDivisionDouble() {
        final int numElements = 32;
        double[] a = new double[numElements];
        double[] b = new double[numElements];
        double[] c = new double[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivDouble, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] / b[i], c[i], 0.01);
        }
    }

    @Test
    public void testVectorAdditionFloat() {
        final int numElements = 32;
        float[] a = new float[numElements];
        float[] b = new float[numElements];
        float[] c = new float[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddFloat, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] + b[i], c[i], 0.01f);
        }
    }

    @Test
    public void testVectorSubtractionFloat() {
        final int numElements = 32;
        float[] a = new float[numElements];
        float[] b = new float[numElements];
        float[] c = new float[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubFloat, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] - b[i], c[i], 0.01f);
        }
    }

    @Test
    public void testVectorMultiplicationFloat() {
        final int numElements = 32;
        float[] a = new float[numElements];
        float[] b = new float[numElements];
        float[] c = new float[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulFloat, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] * b[i], c[i], 0.01f);
        }
    }

    @Test
    public void testVectorDivisionFloat() {
        final int numElements = 32;
        float[] a = new float[numElements];
        float[] b = new float[numElements];
        float[] c = new float[numElements];

        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = (float) Math.random();
            b[i] = (float) Math.random();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivFloat, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] / b[i], c[i], 0.01f);
        }
    }

    @Test
    public void testVectorAdditionInteger() {
        final int numElements = 32;
        int[] a = new int[numElements];
        int[] b = new int[numElements];
        int[] c = new int[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = r.nextInt();
            b[i] = r.nextInt();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddInteger, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] + b[i], c[i]);
        }
    }

    @Test
    public void testVectorSubtractionInteger() {
        final int numElements = 32;
        int[] a = new int[numElements];
        int[] b = new int[numElements];
        int[] c = new int[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = r.nextInt();
            b[i] = r.nextInt();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubInteger, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] - b[i], c[i]);
        }
    }

    @Test
    public void testVectorMultiplicationInteger() {
        final int numElements = 32;
        int[] a = new int[numElements];
        int[] b = new int[numElements];
        int[] c = new int[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = r.nextInt();
            b[i] = r.nextInt();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulInteger, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] * b[i], c[i]);
        }
    }

    @Test
    public void testVectorDivisionInteger() {
        final int numElements = 32;
        int[] a = new int[numElements];
        int[] b = new int[numElements];
        int[] c = new int[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).sequential().forEach(i -> {
            a[i] = r.nextInt();
            b[i] = r.nextInt();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivInteger, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] / b[i], c[i]);
        }
    }

    @Test
    public void testVectorAdditionLong() {
        final int numElements = 32;
        long[] a = new long[numElements];
        long[] b = new long[numElements];
        long[] c = new long[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).parallel().forEach(i -> {
            a[i] = r.nextLong();
            b[i] = r.nextLong();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddLong, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] + b[i], c[i]);
        }
    }

    @Test
    public void testVectorSubtractionLong() {
        final int numElements = 32;
        long[] a = new long[numElements];
        long[] b = new long[numElements];
        long[] c = new long[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).parallel().forEach(i -> {
            a[i] = r.nextLong();
            b[i] = r.nextLong();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubLong, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] - b[i], c[i]);
        }
    }

    @Test
    public void testVectorMultiplicationLong() {
        final int numElements = 32;
        long[] a = new long[numElements];
        long[] b = new long[numElements];
        long[] c = new long[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).parallel().forEach(i -> {
            a[i] = r.nextLong();
            b[i] = r.nextLong();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulLong, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] * b[i], c[i]);
        }
    }

    @Test
    public void testVectorDivisionLong() {
        final int numElements = 32;
        long[] a = new long[numElements];
        long[] b = new long[numElements];
        long[] c = new long[numElements];

        Random r = new Random();
        IntStream.range(0, numElements).parallel().forEach(i -> {
            a[i] = r.nextLong();
            b[i] = r.nextLong();
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivLong, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals(a[i] / b[i], c[i]);
        }
    }

    @Test
    public void testVectorAdditionShort() {
        final int numElements = 32;
        short[] a = new short[numElements];
        short[] b = new short[numElements];
        short[] c = new short[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddShort, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((short) (a[i] + b[i]), c[i]);
        }
    }

    @Test
    public void testVectorSubtractionShort() {
        final int numElements = 32;
        short[] a = new short[numElements];
        short[] b = new short[numElements];
        short[] c = new short[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubShort, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((short) (a[i] - b[i]), c[i]);
        }
    }

    @Test
    public void testVectorMultiplicationShort() {
        final int numElements = 32;
        short[] a = new short[numElements];
        short[] b = new short[numElements];
        short[] c = new short[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulShort, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((short) (a[i] * b[i]), c[i]);
        }
    }

    @Test
    public void testVectorDivisionShort() {
        final int numElements = 32;
        short[] a = new short[numElements];
        short[] b = new short[numElements];
        short[] c = new short[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivShort, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((short) (a[i] / b[i]), c[i]);
        }
    }

    @Test
    public void testVectorAdditionChar() {
        final int numElements = 32;
        char[] a = new char[numElements];
        char[] b = new char[numElements];
        char[] c = new char[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorAddChar, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((char) (a[i] + b[i]), c[i]);
        }
    }

    @Test
    public void testVectorSubtractionChar() {
        final int numElements = 32;
        char[] a = new char[numElements];
        char[] b = new char[numElements];
        char[] c = new char[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorSubChar, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((char) (a[i] - b[i]), c[i]);
        }
    }

    @Test
    public void testVectorMultiplicationChar() {
        final int numElements = 32;
        char[] a = new char[numElements];
        char[] b = new char[numElements];
        char[] c = new char[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorMulChar, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((char) (a[i] * b[i]), c[i]);
        }
    }

    @Test
    public void testVectorDivisionChar() {
        final int numElements = 32;
        char[] a = new char[numElements];
        char[] b = new char[numElements];
        char[] c = new char[numElements];

        IntStream.range(0, numElements).parallel().forEach(idx -> {
            a[idx] = 20;
            b[idx] = 34;
        });

        //@formatter:off
        new TaskSchedule("s0")
                .streamIn(a, b)
                .task("t0", TestBasicOperations::vectorDivChar, a, b, c)
                .streamOut(c)
                .execute();
        //@formatter:on

        for (int i = 0; i < c.length; i++) {
            assertEquals((char) (a[i] / b[i]), c[i]);
        }
    }
}
