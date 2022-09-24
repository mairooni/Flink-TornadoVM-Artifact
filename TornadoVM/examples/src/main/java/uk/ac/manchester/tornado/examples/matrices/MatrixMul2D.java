package uk.ac.manchester.tornado.examples.matrices;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.TornadoDriver;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.collections.types.Matrix2DFloat;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.util.Arrays;
import java.util.Random;

public class MatrixMul2D {

    private static final int WARMING_UP_ITERATIONS = 20;
    private static final int TIMING_ITERATIONS = 50;
    private static final boolean CHECK_RESULT = false;
    private static final float DELTA = 0.01f;

    private static void matrixMultiplication(Matrix2DFloat A, Matrix2DFloat B, Matrix2DFloat C, final int size) {
        for (@Parallel int i = 0; i < size; i++) {
            for (@Parallel int j = 0; j < size; j++) {
                float sum = 0.0f;
                for (int k = 0; k < size; k++) {
                    sum += A.get(i, k) * B.get(k, j);
                }
                C.set(i, j, sum);
            }
        }
    }

    private static void printMatrices(int size, Matrix2DFloat matrixCCUDA, Matrix2DFloat matrixCOCL) {
        System.out.println("CUDA:");
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                System.out.print(" | " + matrixCCUDA.get(i, j));
            }
            System.out.println(" |");
        }

        System.out.println("OPENCL:");
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                System.out.print(" | " + matrixCOCL.get(i, j));
            }
            System.out.println(" |");
        }
    }

    public static void main(String[] args) {

        int size = 512;
        if (args.length >= 1) {
            try {
                size = Integer.parseInt(args[0]);
            } catch (NumberFormatException nfe) {
                size = 512;
            }
        }

        Matrix2DFloat matrixA = new Matrix2DFloat(size, size);
        Matrix2DFloat matrixB = new Matrix2DFloat(size, size);
        Matrix2DFloat matrixCCUDA = new Matrix2DFloat(size, size);
        Matrix2DFloat matrixCOCL = new Matrix2DFloat(size, size);
        Matrix2DFloat matrixCSeq = new Matrix2DFloat(size, size);

        Random r = new Random();
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrixA.set(i, j, r.nextFloat());
                matrixB.set(i, j, r.nextFloat());
            }
        }

        TaskSchedule cudaTask = new TaskSchedule("cuda_s0") //
                .task("t0", MatrixMul2D::matrixMultiplication, matrixA, matrixB, matrixCCUDA, size) //
                .streamOut(matrixCCUDA); //

        TornadoDriver cudaDriver = TornadoRuntime.getTornadoRuntime().getDriver(0);
        TornadoDevice cudaDevice = cudaDriver.getDevice(0);
        cudaTask.mapAllTo(cudaDevice);

        // Warm up CUDA
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            cudaTask.execute();
        }

        // Time CUDA
        long start,stop;
        long[] execTimesCUDA = new long[TIMING_ITERATIONS];

        for (int i = 0; i < TIMING_ITERATIONS; i++) {
            start = System.currentTimeMillis();
            cudaTask.execute();
            stop = System.currentTimeMillis();
            execTimesCUDA[i] = stop - start;
        }

        TaskSchedule oclTask = new TaskSchedule("ocl_s0") //
                .task("t0", MatrixMul2D::matrixMultiplication, matrixA, matrixB, matrixCOCL, size) //
                .streamOut(matrixCOCL); //

        TornadoDriver oclDriver = TornadoRuntime.getTornadoRuntime().getDriver(1);
        TornadoDevice oclDevice = null;
        for (int i = 0; i < oclDriver.getDeviceCount(); i++) {
            TornadoDevice device = oclDriver.getDevice(i);
            if (device.getPhysicalDevice().getDeviceName().equalsIgnoreCase(cudaDevice.getPhysicalDevice().getDeviceName())) {
                oclDevice = device;
            }
        }
        if (oclDevice == null) {
            System.err.println("There is no device with both OpenCL and CUDA-PTX support");
            System.exit(1);
        }
        oclTask.mapAllTo(oclDevice);

        // Warmup OPENCL
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            oclTask.execute();
        }

        // Time OPENCL
        long[] execTimesOCL = new long[TIMING_ITERATIONS];

        for (int i = 0; i < TIMING_ITERATIONS; i++) {
            start = System.currentTimeMillis();
            oclTask.execute();
            stop = System.currentTimeMillis();
            execTimesOCL[i] = stop - start;
        }

        // Warmup sequential
        for (int i = 0; i < WARMING_UP_ITERATIONS; i++) {
            matrixMultiplication(matrixA, matrixB, matrixCSeq, size);
        }

        // Time sequential
        long[] execTimesSequential = new long[TIMING_ITERATIONS];
        for (int i = 0; i < TIMING_ITERATIONS; i++) {
            start = System.currentTimeMillis();
            matrixMultiplication(matrixA, matrixB, matrixCSeq, size);
            stop = System.currentTimeMillis();
            execTimesSequential[i] = stop - start;
        }

        // Compute execution times
        double msecCUDAElapsedTime = Arrays.stream(execTimesCUDA).average().orElse(Double.NaN);
        double msecOCLElapsedTime = Arrays.stream(execTimesOCL).average().orElse(Double.NaN);
        double msecSeqElapsedTime = Arrays.stream(execTimesSequential).average().orElse(Double.NaN);

        boolean correctResult = true;
        if (CHECK_RESULT) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (Math.abs(matrixCCUDA.get(i, j) - matrixCOCL.get(i, j)) > DELTA) {
                        correctResult = false;
                        break;
                    }
                }
                if (!correctResult) {
                    break;
                }
            }

            if (correctResult) {
                System.out.println("[RESULT] correct");
            } else {
                System.out.println("[RESULT] wrong");
            }
        }

        if (size < 5) {
            printMatrices(size, matrixCCUDA, matrixCOCL);
        }

        // Compute Gigaflops and performance
        double flops = 2 * Math.pow(size, 3);
        double CUDAGigaFlops = (1.0E-9 * flops) / (msecCUDAElapsedTime / 1000.0f);
        double OpenCLGigaFlops = (1.0E-9 * flops) / (msecOCLElapsedTime / 1000.0f);
        double CUDAspeedup = msecSeqElapsedTime / msecCUDAElapsedTime;
        double OpenCLspeedup = msecSeqElapsedTime / msecOCLElapsedTime;

        String formatCUDAFGlops = String.format("%.2f", CUDAGigaFlops);
        String formatOpenCLFGlops = String.format("%.2f", OpenCLGigaFlops);

        System.out.println("\tOpenCL Execution: " + formatOpenCLFGlops + " GFlops, Total time = " + msecOCLElapsedTime + " ms");
        System.out.println("\tPTX Execution: " + formatCUDAFGlops + " GFlops, Total Time = " + msecCUDAElapsedTime + " ms");
        System.out.println("\tOpenCL Speedup: " + OpenCLspeedup + "x");
        System.out.println("\tPTX Speedup: " + CUDAspeedup + "x");
        System.out.println();
    }
}
