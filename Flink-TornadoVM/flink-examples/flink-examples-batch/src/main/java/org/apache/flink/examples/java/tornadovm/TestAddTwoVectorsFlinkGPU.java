package org.apache.flink.examples.java.tornadovm;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.jcublas.JCublas;
import jcuda.runtime.JCuda;
import jcuda.runtime.cudaEvent_t;
import jcuda.runtime.cudaStream_t;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static jcuda.runtime.JCuda.*;

/**
 * Vector addition with Flink GPU implementation
 */
public class TestAddTwoVectorsFlinkGPU {

	private static final int size = 12;

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

		List<Tuple2<Float[], Float[]>> vectorsList = buildTuplesOrder(env, dataSize);
		DataSet<Tuple2<Float[], Float[]>> vectors = env.fromCollection(vectorsList).setParallelism(parallelism);
		DataSet<Float[]> result = vectors.map(new VectorAdd(dataSize));
		// Emit result
		System.out.println("Printing result to stdout. Use --output to specify output path.");
		List<Float[]> resultList = result.collect();

		for (Float f : resultList.get(0)) {
			System.out.println(f);
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

		VectorAdd(int size) {
			this.size = size;
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
			//final float[] result = new float[size];
			final Pointer vector1Pointer = new Pointer();
			final Pointer vector2Pointer = new Pointer();
			//final Pointer resultPointer = new Pointer();

			// Fill the input and output vector
			for (int i = 0; i < size; i++) {
				vector1[i] = value.f0[i];
				vector2[i] = value.f1[i];
				//result[i] = 0;
			}

			// Allocate device memory for the input and output
			JCublas.cublasAlloc(size, Sizeof.FLOAT, vector1Pointer);
			JCublas.cublasAlloc(size, Sizeof.FLOAT, vector2Pointer);


			// Initialize the device matrices
			JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(vector1), 1, vector1Pointer, 1);
			JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(vector2), 1, vector2Pointer, 1);
		//	JCublas.cublasSetVector(size, Sizeof.FLOAT, Pointer.to(result), 1, resultPointer, 1);

			cudaEvent_t start = new cudaEvent_t();
			cudaEvent_t stop = new cudaEvent_t();

			cudaEventCreate(start);
			cudaEventRecord(start,null);

			// Performs operation using JCublas
			JCublas.cublasSaxpy(size, 1, vector1Pointer, 1, vector2Pointer, 1);

			cudaEventCreate(stop);
			cudaEventRecord(stop,null);
			cudaEventSynchronize(stop);

			float elapsedTimeMsArray[] = { Float.NaN };
			cudaEventElapsedTime(elapsedTimeMsArray, start, stop);
			for (int i = 0; i < elapsedTimeMsArray.length; i++) {
				System.out.println("Elapsed time : " + elapsedTimeMsArray[i]);
			}
			cudaEventDestroy(stop);
			cudaEventDestroy(start);
			// Read the result back
			JCublas.cublasGetVector(size, Sizeof.FLOAT, vector2Pointer, 1, Pointer.to(vector2), 1);

			// Memory clean up
			JCublas.cublasFree(vector1Pointer);
			JCublas.cublasFree(vector2Pointer);
			//JCublas.cublasFree(resultPointer);

			Float[] resultWrapped = new Float[size];
			for (int i = 0; i < size; ++i) {
				resultWrapped[i] = vector2[i];
			}

			return resultWrapped;

		}

		@Override
		public void close() {
			JCublas.cublasShutdown();
		}
	}

}
