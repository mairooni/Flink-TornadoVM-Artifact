package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Test map function with signature Tuple4 map(Tuple4).
 */
public class TestTuple4Tuple4 {
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

	private static void testVectorAdd(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testVectorAddFlinkTornado(args, s);
		}
	}

	private static DataSet<Tuple4<Double, Double, Double, Double>> buildTuplesOrder(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple4<Double, Double, Double, Double>> arrayList = new ArrayList<>();
		Double d = 2.2;
		for (int i = 0; i < size; i++) {
			Tuple4<Double, Double, Double, Double> t2 = new Tuple4<>(d, d, d, d);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static ArrayList<Tuple4<Double, Double, Double, Double>> buildTuplesOrderSerial(final int size) {
		ArrayList<Tuple4<Double, Double, Double, Double>> arrayList = new ArrayList<>();
		Double d = 2.2;
		for (int i = 0; i < size; i++) {
			Tuple4<Double, Double, Double, Double> t2 = new Tuple4<>(d, d, d, d);
			arrayList.add(t2);
		}
		return arrayList;
	}

	private static void testVectorAddFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple4<Double, Double, Double, Double>> array = buildTuplesOrder(env, size);
		DataSet<Tuple4<Double, Double, Double, Double>> outArray = array.map(new VectorAdd());
		List<Tuple4<Double, Double, Double, Double>> collect = outArray.collect();

		VectorAdd vadd = new VectorAdd();
		ArrayList<Tuple4<Double, Double, Double, Double>> serialArray = buildTuplesOrderSerial(size);
		List<Tuple4<Double, Double, Double, Double>> serial = new ArrayList<>();

		for (Tuple4<Double, Double, Double, Double> t : serialArray) {
			serial.add(vadd.map(t));
		}

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (!serial.get(i).f0.equals(collect.get(i).f0) || !serial.get(i).f1.equals(collect.get(i).f1)) {
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

		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestTuple4Tuple4 \033[0;0m");
		try {
			testVectorAdd(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

	}

	/**
	 * Simple vector add.
	 */
	public static final class VectorAdd implements MapFunction<Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>> {
		@Override
		public Tuple4<Double, Double, Double, Double> map(Tuple4<Double, Double, Double, Double> value) {
			return new Tuple4<>(value.f0, value.f0 + value.f1, value.f2 + value.f3, value.f3);
		}
	}
}
