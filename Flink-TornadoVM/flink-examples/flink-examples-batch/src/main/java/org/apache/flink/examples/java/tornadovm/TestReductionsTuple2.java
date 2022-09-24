package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 * Reduction test for Tuple2 types.
 */
public class TestReductionsTuple2 {

	private static final boolean CHECK_RESULT = true;

	private static int[] getInputSizes() {
		int[] sizes = new int[1];
		int base = 16; //777216;
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

	private static Tuple2<Integer, Integer>[] buildDataSetTuple2(final int size) {
		Tuple2<Integer, Integer>[] array = new Tuple2[size];
		Tuple2<Integer, Integer> t = new Tuple2<>(1, 1);
		for (int i = 0; i < array.length; i++) {
			array[i] = t;
		}
		return array;
	}

	private static void testRedFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Tuple2<Integer, Integer>[] input = buildDataSetTuple2(size);
		DataSet<Tuple2<Integer, Integer>> array =  env.fromElements(input);
		DataSet<Tuple2<Integer, Integer>> outArray = array.reduce(new AddFloats());
		List<Tuple2<Integer, Integer>> collect = outArray.collect();

		for (Tuple2<Integer, Integer> t : collect) {
			System.out.println(t.f0 + " " + t.f1);
		}

		Tuple2<Integer, Integer>[] ser = new Tuple2[1];
		ser[0] = new Tuple2<>(0, 0);
		AddFloats ad = new AddFloats();
		for (int i = 0; i < input.length; i++) {
			ser[0] = ad.reduce(input[i], ser[0]);
		}
		System.out.println(ser[0].f0 + " " + ser[0].f1);
		if (CHECK_RESULT) {
			if (ser[0].f0.equals(collect.get(0).f0) && ser[0].f1.equals(collect.get(0).f1)) {
				System.out.println("\033[1;32mSIZE " +  size  + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " +  size  + " ............................... [FAIL] \033[0;0m");
			}
		}
	}

	private static void testRedTuple2(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testRedFlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestReductionsTuple2 \033[0;0m");
		try {
			testRedTuple2(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}
	}

	/**
	 * A simple reduction.
	 */
	public static final class AddFloats implements ReduceFunction<Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) {
			return new Tuple2<>(value1.f0 + value2.f0, value1.f1 + value2.f1);
		}
	}

}
