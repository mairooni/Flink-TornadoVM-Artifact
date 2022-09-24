package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 *  Reduction test for Floats.
 */
public class TestReductionsFloats {

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

	private static Float[] buildDataSetDouble(final int size) {
		Float[] array = new Float[size];
		Float f = 1.0f;
		for (int i = 0; i < array.length; i++) {
			array[i] = f;
		}
		return array;
	}

	private static void testRedFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Float[] input = buildDataSetDouble(size);
		DataSet<Float> array =  env.fromElements(input);
		DataSet<Float> outArray = array.reduce(new AddFloats());
		List<Float> collect = outArray.collect();

		Float[] ser = new Float[1];
		ser[0] = 0.0f;
		AddFloats ad = new AddFloats();
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

	private static void testRedFloats(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testRedFlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestReductionsFloats \033[0;0m");
		try {
			testRedFloats(args);
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
	public static final class AddFloats implements ReduceFunction<Float> {
		@Override
		public Float reduce(Float value1, Float value2) {
			return value1 + value2;
		}
	}
}
