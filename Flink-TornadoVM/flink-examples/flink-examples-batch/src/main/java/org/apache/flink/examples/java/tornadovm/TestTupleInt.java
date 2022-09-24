package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Flink functions with input type Tuple2 and return type Integer.
 */
public class TestTupleInt {

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
		DataSet<Tuple2<Integer, Integer>> array = buildTuplesIntegers(env, size);
		DataSet<Integer> outArray = array.map(new TestTupleInteger());
		List<Integer> collect = outArray.collect();

		TestTupleInteger testSerial = new TestTupleInteger();
		Tuple2<Integer, Integer>[] serial = buildVanillaFlinkIntegers(size);

		Integer[] out = new Integer[size];

		for (int i = 0; i < size; i++) {
			out[i] = testSerial.map(serial[i]);
		}

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (!out[i].equals(collect.get(i))) {
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

	private static DataSet<Tuple2<Integer, Integer>> buildTuplesIntegers(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Integer> t2 = new Tuple2<>(i, i);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static Tuple2<Integer, Integer>[] buildVanillaFlinkIntegers(final int size) {
		Tuple2<Integer, Integer>[] array = new Tuple2[size];
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Integer> t2 = new Tuple2<>(i, i);
			array[i] = t2;
		}
		return array;
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestTupleInt \033[0;0m");
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
	public static final class TestTupleInteger implements MapFunction<Tuple2<Integer, Integer>, Integer> {
		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f0 + value.f1;
		}
	}

}
