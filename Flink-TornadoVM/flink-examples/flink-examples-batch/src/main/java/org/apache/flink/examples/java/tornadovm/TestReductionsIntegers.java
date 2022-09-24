package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 *  Reduction test for Integers.
 */
public class TestReductionsIntegers {

	private static final boolean CHECK_RESULT = true;

	private static int[] getInputSizes() {
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

	private static Integer[] buildDataSetIntegers(final int size) {
		Integer[] array = new Integer[size];
		Integer in = 1;
		for (int i = 0; i < array.length; i++) {
			array[i] = in;
		}
		return array;
	}

	private static void testCopyFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Integer[] input = buildDataSetIntegers(size);
		DataSet<Integer> array =  env.fromElements(input);
		DataSet<Integer> outArray = array.reduce(new AddIntegers());
		List<Integer> collect = outArray.collect();

		Integer[] ser = new Integer[1];
		ser[0] = 0;
		AddIntegers ad = new AddIntegers();
		for (int i = 0; i < input.length; i++) {
			ser[0] = ad.reduce(input[i], ser[0]);
		}

		if (CHECK_RESULT) {
			if (ser[0].equals(collect.get(0))) {
				System.out.println("\033[1;32mSIZE " +  size  + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " +  size  + " ............................... [FAIL] \033[0;0m");
			}
		}
	}

	private static void testCopyIntegers(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testCopyFlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestReductionsIntegers \033[0;0m");
		try {
			testCopyIntegers(args);
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
	public static final class AddIntegers implements ReduceFunction<Integer> {
		@Override
		public Integer reduce(Integer value1, Integer value2) {
			return value1 + value2;
		}
	}

}
