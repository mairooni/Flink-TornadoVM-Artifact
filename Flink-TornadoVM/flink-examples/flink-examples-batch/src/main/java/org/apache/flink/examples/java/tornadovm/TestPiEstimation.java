package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;

/**
 * Estimates the value of Pi using the Monte Carlo method.
 * The area of a circle is Pi * R^2, R being the radius of the circle
 * The area of a square is 4 * R^2, where the length of the square's edge is 2*R.
 *
 * <p>Thus Pi = 4 * (area of circle / area of square).
 *
 * <p>The idea is to find a way to estimate the circle to square area ratio.
 * The Monte Carlo method suggests collecting random points (within the square)
 * and then counting the number of points that fall within the circle
 *
 * <pre>
 * {@code
 * x = Math.random()
 * y = Math.random()
 *
 * x * x + y * y < 1
 * }
 * </pre>
 */
@SuppressWarnings("serial")
public class TestPiEstimation implements java.io.Serializable {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		int numSamples = 16777216;
		int parallelism = 1;
		boolean unittest = true;
		// parallelism
		if (params.has("parallelism")) {
			parallelism = Integer.parseInt(params.get("parallelism"));
		}

		if (params.has("samples")) {
			unittest = false;
			numSamples = Integer.parseInt(params.get("samples"));
		}

		List<Long> lst = buildDataset(numSamples);
		DataSet<Long> input = env.fromCollection(lst).setParallelism(parallelism);
		// count how many of the samples would randomly fall into
		// the unit circle
		DataSet<Long> count = input
			//env.generateSequence(1, numSamples)
			.map(new Sampler()).setParallelism(parallelism)
			.reduce(new SumReducer()).setParallelism(parallelism);
		long theCount = count.collect().get(0);
		double result = (theCount * 4.0 / numSamples);
		//if results are incorrect comment-out tornadoVMCleanUp(); in chainedmapdriver
		//System.out.println("We estimate Pi to be: " + result);

		if (unittest) {
			System.setProperty("tornado", "false");
			DataSet<Long> countser = input
				//env.generateSequence(1, numSamples)
				.map(new Sampler()).setParallelism(parallelism)
				.reduce(new SumReducer()).setParallelism(parallelism);

			long theCountSerial = countser.collect().get(0);
			double resultSerial = (theCountSerial * 4.0 / numSamples);

			boolean correct = true;

			System.out.println("tornado: " + result + " serial: " + resultSerial);
			if (Math.abs(result - resultSerial) > 0.01f) {
				correct = false;
			}

			if (correct) {
				System.out.println("\033[1;32mSIZE " + numSamples + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " + numSamples + " ............................... [FAIL] \033[0;0m");
			}
		}
		//long theCount = count.collect().get(0);

		//System.out.println("We estimate Pi to be: " + (theCount * 4.0 / numSamples));
	}

	//*************************************************************************
	//     USER FUNCTIONS
	//*************************************************************************
	private static List<Long> buildDataset(final int size) {
		List<Long> indices = new ArrayList<>();
		for (int i = 0; i < size; i++) {	indices.add((long) i);
		}
		return indices;
	}

	/**
	 * Sampler randomly emits points that fall within a square of edg
	 * 		e x * y.
	 * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
	 * If the distance is less than 1, then and only then does it returns a 1.
	 */
//	public static class Sampler implements MapFunction<Long, Long> {
//
//		@Override
//		public Long map(Long value) {
//			double x = Math.random();
//			double y = Math.random();
//			return (x * x + y * y) < 1 ? 1L : 0L;
//		}
//	}

	public static class Sampler implements MapFunction<Long, Long> {

		@Override
		public Long map(Long value) {
			long seed = value;
			// generate a pseudo random number (you do need it twice)
			seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
			seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);

			// this generates a number between 0 and 1 (with an awful entropy)
			float x = (seed & 0x0FFFFFFF) / 268435455f;

			// repeat for y
			seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
			seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
			float y = (seed & 0x0FFFFFFF) / 268435455f;

			float dist = (float) Math.sqrt(x * x + y * y);
			long res;
			if (dist < 1.0f) {
				res = 1L;
			} else if (dist == 1.0f) {
				res = 1L;
			} else {
				res = 0L;
			}
			return res;
		}
	}


	/**
	 * Simply sums up all long values.
	 */
	public static final class SumReducer implements ReduceFunction<Long>{

		@Override
		public Long reduce(Long value1, Long value2) {
			return value1 + value2;
		}
	}
}

