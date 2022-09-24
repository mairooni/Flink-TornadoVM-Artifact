package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.List;

/**
 *  A map with input Tuple3(Double, Integer, Long) and return type Double.
 */
public class TestTuple3DiffInput {
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

	private static Tuple3<Double, Integer, Long>[] buildDataSetDouble(final int size) {
		Tuple3<Double, Integer, Long>[] array = new Tuple3[size];
		Double d = 2.2;
		Long l = 1L;
		for (int i = 0; i < array.length; i++) {
			array[i] = new Tuple3<>(d, i, l);
		}
		return array;
	}

	private static void testTuple3FlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Tuple3<Double, Integer, Long>[] input = buildDataSetDouble(size);
		DataSet<Tuple3<Double, Integer, Long>> array =  env.fromElements(input);
		DataSet<Double> outArray = array.map(new MapTuple3());
		List<Double> collect = outArray.collect();

		Double[] out = new Double[input.length];
		MapTuple3 mt3 = new MapTuple3();
		for (int i = 0; i < input.length; i++) {
			out[i] = mt3.map(input[i]);
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

	private static void testMapTuple3(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testTuple3FlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.TestTuple3DiffInput \033[0;0m");
		try {
			testMapTuple3(args);
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
	public static final class MapTuple3 implements MapFunction<Tuple3<Double, Integer, Long>, Double> {
		@Override
		public Double map(Tuple3<Double, Integer, Long> value) {
			return value.f0 + value.f1 + value.f2;
		}
	}
}
