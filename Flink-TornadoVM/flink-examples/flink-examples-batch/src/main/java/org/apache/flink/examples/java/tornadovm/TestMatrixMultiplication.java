package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;


import java.util.*;

/**
 * Unittest for Matrix Multiplication
 */
public class TestMatrixMultiplication {

	private static ArrayList<int[]> getInputSizes() {
		ArrayList<int[]> sizes = new ArrayList<>();
		int[] size = new int[2];
		int base = 64;
		int base2 = 32;
		size[0] = base;
		size[1] = base2;
		sizes.add(size);
		//for (int i = 1; i < sizes.length; i++) {
		//	base = base * 2;
		//	sizes[i] = base;
		//}
		return sizes;
	}

	private static void testTupleIntegers(String[] args) throws Exception {
		ArrayList<int[]> sizes = getInputSizes();
		for (int[] s : sizes) {
			testTuplesIntegersFlinkTornado(args, s[0], s[1]);
		}
	}

	private static void testTuplesIntegersFlinkTornado(String[] args, int size1, int size2) throws Exception {
		//final ExecutionEnvironment env = configureFlink(args);
		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		boolean unittest = false;
		int parallelism = 1;
		// parallelism
		if (params.has("parallelism")) {
			parallelism = Integer.parseInt(params.get("parallelism"));
		}
		System.out.println("parallelism: " + parallelism);
		DataSet<Float[]> matrix1Dataset;
		if (params.has("matrix1")) {
			DataSet<Tuple1<String>> dat = env.readCsvFile(params.get("matrix1")).fieldDelimiter("\n").types(String.class).setParallelism(parallelism);
			matrix1Dataset = dat.flatMap(new CreateDataSet()).setParallelism(parallelism);
		} else {
			List<Float[]> matrix1 = buildMatrix1List(size1, size2);
			matrix1Dataset = env.fromCollection(matrix1).setParallelism(parallelism);
			unittest = true;
		}

		DataSet<Float[]> matrix2Dataset;
		if (params.has("matrix2")) {
			DataSet<Tuple1<String>> dat = env.readCsvFile(params.get("matrix2")).fieldDelimiter("\n").types(String.class).setParallelism(parallelism);
			matrix2Dataset = dat.flatMap(new CreateDataSet()).setParallelism(parallelism);
		} else {
			List<Float[]> matrix2 = buildMatrix2List(size1, size2);
			matrix2Dataset = env.fromCollection(matrix2).setParallelism(parallelism);
			unittest = true;
		}

		DataSet<Float[]> outArray = matrix1Dataset.map(new TestMatrixMul()).withBroadcastSet(matrix2Dataset, "matrix2").setParallelism(parallelism);
		List<Float[]> tres = outArray.collect();

		if (unittest) {
			System.setProperty("tornado", "false");

			List<Float[]> out = outArray.collect();

			boolean correct = true;
			for (int i = 0; i < out.size(); i++) {
				for (int j = 0; j < out.get(i).length; j++) {
					if (Math.abs(out.get(i)[j] - tres.get(i)[j]) > 0.01f) {
						correct = false;
						break;
					}
				}
				if (!correct) break;
			}

			if (correct) {
				System.out.println("\033[1;32mSIZE " + size1 + "x" + size2 + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " + size1 + "x" + size2 + " ............................... [FAIL] \033[0;0m");
			}
		}

	}

	private static List<Float[]> buildMatrix1List(final int size1, final int size2) {
		List<Float[]> matrix1 = new ArrayList<>();
		Float[] arr = new Float[size2];
		Random r = new Random();
		for (int i = 0; i < size1; i++) {
			for (int j = 0; j < size2; j++) {
				arr[j] =  r.nextFloat();
			}
			matrix1.add(arr);
		}
		return matrix1;
	}

	private static List<Float[]> buildMatrix2List(final int size1, final int size2) {
		List<Float[]> matrix2 = new ArrayList<>();
		Float[] arr = new Float[size1];
		Random r = new Random();
		for (int i = 0; i < size2; i++) {
			for (int j = 0; j < size1; j++) {
				arr[j] = r.nextFloat();
			}
			matrix2.add(arr);
		}
		return matrix2;
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestMatrixMultiplication \033[0;0m");
		try {
			testTupleIntegers(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}
	}

	public static final class CreateDataSet implements FlatMapFunction<Tuple1<String>, Float[]> {

		@Override
		public void flatMap(Tuple1<String> value, Collector<Float[]> out) throws Exception {
			StringTokenizer tok = new StringTokenizer(value.f0, ",");
			Float[] row = new Float[tok.countTokens()];
			int i = 0;
			while (tok.hasMoreTokens()) {
				row[i] = Float.parseFloat(tok.nextToken());
				i++;
			}
			out.collect(row);
		}
	}
 	/**
	 * User function for matrix multiplication.
	 */
	public static final class TestMatrixMul extends RichMapFunction<Float[], Float[]> {

		private Collection<Float[]> matrix2;

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.matrix2 = getRuntimeContext().getBroadcastVariable("matrix2");
		}

		@Override
		public Float[] map(Float[] row) {
			int c2 = ((List<Float[]>) matrix2).get(0).length;
			int r2 =  matrix2.size();
			Float[] newRow = new Float[c2];
			for (int j = 0; j < c2; j++) {
				float sum = 0.0f;
				for (int k = 0; k < r2; k++) {
					sum += row[k] * ((List<Float[]>) matrix2).get(k)[j];
				}
				newRow[j] = sum;
			}
			return newRow;
		}
	}


}
