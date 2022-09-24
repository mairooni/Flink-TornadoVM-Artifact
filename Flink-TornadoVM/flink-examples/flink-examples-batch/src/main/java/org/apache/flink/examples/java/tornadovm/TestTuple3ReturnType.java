package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Tuple3 return type.
 */
public class TestTuple3ReturnType {
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

	private static DataSet<Tuple2<Integer, Integer>> buildTuplesOrder(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Integer> t2 = new Tuple2<>(i, i);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static ArrayList<Tuple2<Integer, Integer>> buildTuplesOrderSerial(final int size) {
		ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Integer> t2 = new Tuple2<>(i, i);
			arrayList.add(t2);
		}
		return arrayList;
	}

	private static void testVectorAddFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple2<Integer, Integer>> array = buildTuplesOrder(env, size);
		DataSet<Tuple3<Integer, Integer, Integer>> outArray = array.map(new VectorAdd());
		List<Tuple3<Integer, Integer, Integer>> collect = outArray.collect();

		VectorAdd vadd = new VectorAdd();
		ArrayList<Tuple2<Integer, Integer>> serialArray = buildTuplesOrderSerial(size);
		List<Tuple3<Integer, Integer, Integer>> serial = new ArrayList<>();

		for (Tuple2<Integer, Integer> t : serialArray) {
			serial.add(vadd.map(t));
		}

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (!serial.get(i).f0.equals(collect.get(i).f0) || !serial.get(i).f1.equals(collect.get(i).f1) || !serial.get(i).f2.equals(collect.get(i).f2)) {
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

		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestTuple3ReturnType \033[0;0m");
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
	public static final class VectorAdd implements MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) {
			return new Tuple3<>(value.f0, value.f0 + value.f1, 9);
		}
	}
}
