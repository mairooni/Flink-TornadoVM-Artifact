package org.apache.flink.examples.java.tornadovm;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import jcuda.jcublas.JCublas;
import jcuda.runtime.JCuda;
import jcuda.runtime.cudaEvent_t;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.runtime.JCuda.*;
import static jcuda.runtime.JCuda.cudaEventDestroy;

/**
 * Pi Estimation for Flink-GPU
 */
public class TestPiEstimationFlinkGPU {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		int numSamples = 16777216;
		int parallelism = 1;

		// parallelism
		if (params.has("parallelism")) {
			parallelism = Integer.parseInt(params.get("parallelism"));
		}

		if (params.has("samples")) {
			numSamples = Integer.parseInt(params.get("samples"));
		}

		List<Long[]> lst = buildDataset(numSamples);
		DataSet<Long[]> input = env.fromCollection(lst).setParallelism(parallelism);

		final int gridSize = params.getInt("gridSize", (numSamples/256)); // size / block size
		final int blockSize = params.getInt("blockSize", 256);
		// count how many of the samples would randomly fall into
		// the unit circle
		DataSet<Long[]> count = input
			//env.generateSequence(1, numSamples)
			.map(new Sampler(numSamples, gridSize, blockSize)).setParallelism(parallelism)
			.reduce(new SumReducer(numSamples, gridSize, blockSize)).setParallelism(parallelism);

		Long[] theCount = count.collect().get(0);
		for (int i = 0; i < theCount.length; i++) {
			System.out.print(theCount[i] + " ");
		}

		System.out.println("\n");
		double result = (theCount[0] * 4.0 / numSamples);
		System.out.println("We estimate Pi to be: " + result);

	}

	//*************************************************************************
	//     USER FUNCTIONS
	//*************************************************************************
	private static List<Long[]> buildDataset(final int size) {
		List<Long[]> indices = new ArrayList<>();
		Long[] ar = new Long[size];
		for (int i = 0; i < size; i++) {
			ar[i] = (long) i;
		}
		indices.add(ar);
		return indices;
	}

	/**
	 * Sampler randomly emits points that fall within a square of edg
	 * 		e x * y.
	 * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
	 * If the distance is less than 1, then and only then does it returns a 1.
	 */

	public static class Sampler extends RichMapFunction<Long[], Long[]> {

		private int size;
		String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
		private int gridSize;
		private int blockSize;

		Sampler (int size, int gridSize, int blockSize) {
			this.size = size;
			this.gridSize = gridSize;
			this.blockSize = blockSize;
		}

		@Override
		public void open(Configuration parameters) {
			// When multiple instances of this class and JCuda exist in different class loaders, then we will get UnsatisfiedLinkError.
			// To avoid that, we need to temporarily override the java.io.tmpdir, where the JCuda store its native library, with a random path.
			// For more details please refer to https://issues.apache.org/jira/browse/FLINK-5408 and the discussion in http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Classloader-and-removal-of-native-libraries-td14808.html
			final String originTempDir = System.getProperty("java.io.tmpdir");
			final String newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID();
			System.setProperty("java.io.tmpdir", newTempDir);

			final Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos("gpu");
			Preconditions.checkState(!externalResourceInfos.isEmpty(), "The TestAddTwoVectorsFlinkGPU needs at least one GPU device while finding 0 GPU.");
			final Optional<String> firstIndexOptional = externalResourceInfos.iterator().next().getProperty("index");
			Preconditions.checkState(firstIndexOptional.isPresent());

			// Set the CUDA device
			JCuda.cudaSetDevice(Integer.parseInt(firstIndexOptional.get()));

			// Initialize JCublas
			JCublas.cublasInit();

			// Change the java.io.tmpdir back to its original value.
			System.setProperty("java.io.tmpdir", originTempDir);
		}


		@Override
		public Long[] map(Long[] value) {
			final long[] input = new long[size];
			final long[] output = new long[size];

			final Pointer inputPointer = new Pointer();
			final Pointer outputPointer = new Pointer();

			// Fill the input and output vector
			for (int i = 0; i < size; i++) {
				input[i] = value[i];
				output[i] = 0;
			}

			CUmodule module = new CUmodule();
			cuModuleLoad(module, path + "/pi-map.ptx");

			// Obtain a function pointer to the kernel function.
			CUfunction function = new CUfunction();
			cuModuleGetFunction(function, module, "map");
			// Set up the kernel parameters: A pointer to an array of pointers which point to the actual values.

			// Allocate device memory for the input and output
			JCublas.cublasAlloc(size, Sizeof.LONG, inputPointer);
			JCublas.cublasAlloc(size, Sizeof.LONG, outputPointer);

			// Initialize the device matrices
			JCublas.cublasSetVector(size, Sizeof.LONG, Pointer.to(input), 1, inputPointer, 1);
			JCublas.cublasSetVector(size, Sizeof.LONG, Pointer.to(output), 1, outputPointer, 1);

			// pointer to the data pointers
			Pointer kernelParameters = Pointer.to(
				Pointer.to(new int[]{size}),
				Pointer.to(inputPointer),
				Pointer.to(outputPointer)
			);

			cudaEvent_t start = new cudaEvent_t();
			cudaEvent_t stop = new cudaEvent_t();

			cudaEventCreate(start);
			cudaEventRecord(start,null);

			cuLaunchKernel(function,
				gridSize,  1, 1,      // Grid dimension
				blockSize, 1, 1,      // Block dimension
				0, null,               // Shared memory size and stream
				kernelParameters, null // Kernel- and extra parameters
			);

			cudaEventCreate(stop);
			cudaEventRecord(stop,null);
			cudaEventSynchronize(stop);

			float elapsedTimeMsArray[] = { Float.NaN };
			cudaEventElapsedTime(elapsedTimeMsArray, start, stop);
			for (int i = 0; i < elapsedTimeMsArray.length; i++) {
				System.out.println("map kernel: " + elapsedTimeMsArray[i]);
			}
			cudaEventDestroy(stop);
			cudaEventDestroy(start);

			// Read the result back
			JCublas.cublasGetVector(size, Sizeof.LONG, outputPointer, 1, Pointer.to(output), 1);

			// Memory clean up
			JCublas.cublasFree(inputPointer);
			JCublas.cublasFree(outputPointer);
			JCublas.cublasFree(kernelParameters);

			Long[] resultWrapped = new Long[size];
			for (int i = 0; i < size; ++i) {
				resultWrapped[i] = output[i];
			}

			return resultWrapped;
		}

		@Override
		public void close() {
			JCublas.cublasShutdown();
		}
	}


	/**
	 * Simply sums up all long values.
	 */
	public static final class SumReducer extends RichReduceFunction<Long[]> {

		private int size;
		String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
		private int gridSize;
		private int blockSize;

		SumReducer (int size, int gridSize, int blockSize) {
			this.size = size;
			this.gridSize = gridSize;
			this.blockSize = blockSize;
		}

		@Override
		public void open(Configuration parameters) {
			// When multiple instances of this class and JCuda exist in different class loaders, then we will get UnsatisfiedLinkError.
			// To avoid that, we need to temporarily override the java.io.tmpdir, where the JCuda store its native library, with a random path.
			// For more details please refer to https://issues.apache.org/jira/browse/FLINK-5408 and the discussion in http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Classloader-and-removal-of-native-libraries-td14808.html
			final String originTempDir = System.getProperty("java.io.tmpdir");
			final String newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID();
			System.setProperty("java.io.tmpdir", newTempDir);

			final Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos("gpu");
			Preconditions.checkState(!externalResourceInfos.isEmpty(), "The TestAddTwoVectorsFlinkGPU needs at least one GPU device while finding 0 GPU.");
			final Optional<String> firstIndexOptional = externalResourceInfos.iterator().next().getProperty("index");
			Preconditions.checkState(firstIndexOptional.isPresent());

			// Set the CUDA device
			JCuda.cudaSetDevice(Integer.parseInt(firstIndexOptional.get()));

			// Initialize JCublas
			JCublas.cublasInit();

			// Change the java.io.tmpdir back to its original value.
			System.setProperty("java.io.tmpdir", originTempDir);
		}


		@Override
		public Long[] reduce(Long[] value1, Long[] value2) {
			// only one value;
			final long[] input = new long[size];
			final long[] output = new long[gridSize];

			final Pointer inputPointer = new Pointer();
			final Pointer outputPointer = new Pointer();

			// Fill the input and output vector
			for (int i = 0; i < size; i++) {
				input[i] = value1[i];
			}

			for (int i = 0; i < gridSize; i++) {
				output[i] = 0;
			}

			CUmodule module = new CUmodule();
			cuModuleLoad(module, path + "/pi-reduce.ptx");

			// Obtain a function pointer to the kernel function.
			CUfunction function = new CUfunction();
			cuModuleGetFunction(function, module, "reduce");
			// Set up the kernel parameters: A pointer to an array of pointers which point to the actual values.

			// Allocate device memory for the input and output
			JCublas.cublasAlloc(size, Sizeof.LONG, inputPointer);
			JCublas.cublasAlloc(size, Sizeof.LONG, outputPointer);

			// Initialize the device matrices
			JCublas.cublasSetVector(size, Sizeof.LONG, Pointer.to(input), 1, inputPointer, 1);
			JCublas.cublasSetVector(size, Sizeof.LONG, Pointer.to(output), 1, outputPointer, 1);

			// pointer to the data pointers
			Pointer kernelParameters = Pointer.to(
				Pointer.to(new int[]{size}),
				Pointer.to(inputPointer),
				Pointer.to(outputPointer)
			);

			cudaEvent_t start = new cudaEvent_t();
			cudaEvent_t stop = new cudaEvent_t();

			cudaEventCreate(start);
			cudaEventRecord(start,null);

			cuLaunchKernel(function,
				gridSize,  1, 1,      // Grid dimension
				blockSize, 1, 1,      // Block dimension
				0, null,               // Shared memory size and stream
				kernelParameters, null // Kernel- and extra parameters
			);
            JCuda.cudaDeviceSynchronize();

			cudaEventCreate(stop);
			cudaEventRecord(stop,null);
			cudaEventSynchronize(stop);

			float elapsedTimeMsArray[] = { Float.NaN };
			cudaEventElapsedTime(elapsedTimeMsArray, start, stop);
			for (int i = 0; i < elapsedTimeMsArray.length; i++) {
				System.out.println("reduce kernel: " + elapsedTimeMsArray[i]);
			}
			cudaEventDestroy(stop);
			cudaEventDestroy(start);
			// Read the result back
			JCublas.cublasGetVector(gridSize, Sizeof.LONG, outputPointer, 1, Pointer.to(output), 1);

			// Memory clean up
			JCublas.cublasFree(inputPointer);
			JCublas.cublasFree(outputPointer);
			JCublas.cublasFree(kernelParameters);

			Long[] resultWrapped = new Long[gridSize];
			for (int i = 0; i < gridSize; ++i) {
				resultWrapped[i] = output[i];
			}

			return resultWrapped;
		}

		@Override
		public void close() {
			JCublas.cublasShutdown();
		}
	}

}
