package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 *  Reduction test for Longs.
 */
public class TestReductionsLongs {

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

	private static Long[] buildDataSetDouble(final int size) {
		Long[] array = new Long[size];
		Long l = 1L;
		for (int i = 0; i < array.length; i++) {
			array[i] = l;
		}
		return array;
	}

	private static void testRedFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Long[] input = buildDataSetDouble(size);
		DataSet<Long> array =  env.fromElements(input);
		DataSet<Long> outArray = array.reduce(new AddLongs());
		List<Long> collect = outArray.collect();

		Long[] ser = new Long[1];
		ser[0] = 0L;
		AddLongs ad = new AddLongs();
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

	private static void testRedLongs(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testRedFlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestReductionsLongs \033[0;0m");
		try {
			testRedLongs(args);
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
	public static final class AddLongs implements ReduceFunction<Long> {
		@Override
		public Long reduce(Long value1, Long value2) {
			return value1 + value2;
		}
	}

}
