package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 * Test copy of Floats on TornadoVM.
 */
public class TestCopyFloats {

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

	private static void testCopyFloats(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testCopyFloatsFlinkTornado(args, s);
		}
	}

	private static Float[] buildDataSetFloat(ExecutionEnvironment env, final int size) {
		Float[] array = new Float[size];
		for (int i = 0; i < array.length; i++) {
			array[i] = (float) i;
		}
		return array;
	}

	private static void testCopyFloatsFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Float[] input = buildDataSetFloat(env, size);
		DataSet<Float> array =  env.fromElements(input);
		DataSet<Float> outArray = array.map(new CopyFloats());
		List<Float> collect = outArray.collect();

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (Math.abs(input[i] - collect.get(i)) > 0.01f) {
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

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestCopyFloats \033[0;0m");
		try {
			testCopyFloats(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}
	}

	/**
	 * Performs a copy (map operation) for floats.
	 */
	public static final class CopyFloats implements MapFunction<Float, Float> {
		@Override
		public Float map(Float value) {
			return value;
		}
	}
}
