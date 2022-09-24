package org.apache.flink.examples.java.tornadovm;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import jcuda.jcublas.JCublas;
import jcuda.runtime.JCuda;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static jcuda.driver.JCudaDriver.*;

/**
 * Vector addition with Flink GPU implementation
 */
public class TestAddTwoVectorsFlinkGPUV2 {

	private static final int size = 32;

	private static List<Tuple2<Float[], Float[]>> buildTuplesOrder(ExecutionEnvironment env, final int size) {
		List<Tuple2<Float[], Float[]>> arrayList = new ArrayList<>();
		Float[] vector1 = new Float[size];
		Float[] vector2 = new Float[size];
		for (int i = 0; i < size; i++) {
			vector1[i] = (float) 1;
			vector2[i] = (float) 1;
		}
		Tuple2<Float[], Float[]> t2 = new Tuple2<>(vector1,vector2);
		arrayList.add(t2);
		return arrayList;
	}

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// Make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final int dataSize = params.getInt("size", size);
		final int parallelism = params.getInt("parallelism", 1);
		final int gridSize = params.getInt("gridSize", 32);
		final int blockSize = params.getInt("blockSize", 32);

		List<Tuple2<Float[], Float[]>> vectorsList = buildTuplesOrder(env, dataSize);
		DataSet<Tuple2<Float[], Float[]>> vectors = env.fromCollection(vectorsList).setParallelism(parallelism);
		DataSet<Float[]> result = vectors.map(new VectorAdd(dataSize, gridSize, blockSize));
		// Emit result
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		List<Float[]> resultList = result.collect();

		for (Float f : resultList.get(0)) {
			System.out.println(f.floatValue());
		}

	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Vector add
	 */
	public static final class VectorAdd extends RichMapFunction<Tuple2<Float[], Float[]>, Float[]> {

		private int size;
		String path = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR);
		private int gridSize;
		private int blockSize;

		VectorAdd(int size, int gridSize, int blockSize) {
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
		public Float[] map(Tuple2<Float[], Float[]> value) throws Exception {
			final float[] vector1 = new float[size];
			final float[] vector2 = new float[size];
			final float[] result = new float[size];
			final Pointer vector1Pointer = new Pointer();
			final Pointer vector2Pointer = new Pointer();
			final Pointer resultPointer = new Pointer();

			// Fill the input and output vector
			for (int i = 0; i < size; i++) {
				vector1[i] = value.f0[i];
				vector2[i] = value.f1[i];
				result[i] = 0;
			}
			CUmodule module = new CUmodule();
			cuModuleLoad(module, path + "/vadd.ptx");

			// Obtain a function pointer to the kernel function.
			CUfunction function = new CUfunction();
			cuModuleGetFunction(function, module, "add");
			// Set up the kernel parameters: A pointer to an array of pointers which point to the actual values.

			// Allocate device memory for the input and output
			JCublas.cublasAlloc(size, Sizeof.FLOAT, vector1Pointer);
			JCublas.cublasAlloc(size, Sizeof.FLOAT, vector2Pointer);
			JCublas.cublasAlloc(size, Sizeof.FLOAT, resultPointer);


			// Initialize the device matrices
			JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(vector1), 1, vector1Pointer, 1);
			JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(vector2), 1, vector2Pointer, 1);
			JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(result), 1, resultPointer, 1);

			Pointer kernelParameters = Pointer.to(
				Pointer.to(new int[]{size}),
				Pointer.to(vector1Pointer),
				Pointer.to(vector2Pointer),
				Pointer.to(resultPointer)
			);

			cuLaunchKernel(function,
				gridSize,  1, 1,      // Grid dimension
				blockSize, 1, 1,      // Block dimension
				0, null,               // Shared memory size and stream
				kernelParameters, null // Kernel- and extra parameters
			);

			// Read the result back
			JCublas.cublasGetVector(size, Sizeof.FLOAT, resultPointer, 1, Pointer.to(result), 1);

			// Memory clean up
			JCublas.cublasFree(vector1Pointer);
			JCublas.cublasFree(vector2Pointer);
			JCublas.cublasFree(resultPointer);
			JCublas.cublasFree(kernelParameters);

			Float[] resultWrapped = new Float[size];
			for (int i = 0; i < size; ++i) {
				resultWrapped[i] = result[i];
			}

			return resultWrapped;

		}

		@Override
		public void close() {
			JCublas.cublasShutdown();
		}
	}

}
