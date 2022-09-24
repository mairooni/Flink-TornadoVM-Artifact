package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Array field for Tuple3 on TornadoVM.
 */
public class TestTuple3WithArrayField {
	private static final boolean CHECK_RESULT = true;

	private static int[] getInputSizes() {
		// provide 20 input data sizes
		int[] sizes = new int[11];
		int base = 256;
		sizes[0] = base;
		for (int i = 1; i < sizes.length; i++) {
			base = base * 2;
			sizes[i] = base;
		}
		return sizes;
	}

	private static ExecutionEnvironment configureFlink(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		return env;
	}

	private static void testTupleIntegers(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testTuplesIntegersFlinkTornado(args, s);
		}
	}

	private static void testTuplesIntegersFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple3<Integer, Float, double[]>> array = buildTuplesIntegers(env, size);
		DataSet<Double> outArray = array.map(new TestTuple3Diff());
		List<Double> collect = outArray.collect();

		TestTuple3Diff testSerial = new TestTuple3Diff();
		Tuple3<Integer, Float, double[]>[] serial = buildVanillaFlinkIntegers(size);

		Double[] out = new Double[size];

		for (int i = 0; i < size; i++) {
			out[i] = testSerial.map(serial[i]);
		}

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (Math.abs(out[i] - collect.get(i)) > 0.01f) {
					correct = false;
					break;
				}
			}
			if (correct) {
				System.out.println("\033[1;32mSIZE " +  size  + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " +  size  + " ............................... [FAIL] \033[0;0m");
			}
		}

	}

	private static DataSet<Tuple3<Integer, Float, double[]>> buildTuplesIntegers(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple3<Integer, Float, double[]>> arrayList = new ArrayList<>();
		double[] field1 = new double[32];
		for (int i = 0; i < 32; i++) {
			field1[i] = 2.2;
		}
		for (int i = 0; i < size; i++) {
			Tuple3<Integer, Float, double[]> t2 = new Tuple3<>(i, 1.0f, field1);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static Tuple3<Integer, Float, double[]>[] buildVanillaFlinkIntegers(final int size) {
		Tuple3<Integer, Float, double[]>[] array = new Tuple3[size];
		double[] field1 = new double[32];
		for (int i = 0; i < 32; i++) {
			field1[i] = 2.2;
		}
		for (int i = 0; i < size; i++) {
			Tuple3<Integer, Float, double[]> t2 = new Tuple3<>(i, 1.0f, field1);
			array[i] = t2;
		}
		return array;
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestTuple3WithArrayField \033[0;0m");
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

	/**
	 * Test from Tuple2 to Integer.
	 */
	public static final class TestTuple3Diff implements MapFunction<Tuple3<Integer, Float, double[]>, Double> {
		@Override
		public Double map(Tuple3<Integer, Float, double[]> value) {
			return value.f0 + value.f1 + value.f2[4];
		}
	}

}
