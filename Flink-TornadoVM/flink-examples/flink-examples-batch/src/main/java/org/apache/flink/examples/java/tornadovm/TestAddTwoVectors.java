package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.List;

public class TestAddTwoVectors {

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

	private static void testVectorAdd(String[] args) throws Exception {
		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
		if (params.has("size")) {
			int s = Integer.parseInt(params.get("size"));
			testVectorAddFlinkTornado(args, s);
		} else {
			int[] sizes = getInputSizes();
			for (int s : sizes) {
				testVectorAddFlinkTornado(args, s);
			}
		}
	}

	private static List<Tuple2<Float, Float>> buildTuplesOrder(ExecutionEnvironment env, final int size) {
		List<Tuple2<Float, Float>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Float, Float> t2 = new Tuple2<>((float) 1,(float) 1);
			arrayList.add(t2);
		}
		return arrayList;
	}

	private static ArrayList<Tuple2<Float, Float>> buildTuplesOrderSerial(final int size) {
		ArrayList<Tuple2<Float, Float>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Float, Float> t2 = new Tuple2<>((float) 1,(float) 1);
			arrayList.add(t2);
		}
		return arrayList;
	}

	private static void testVectorAddFlinkTornado(String[] args, int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
		boolean unittest = false;
		int parallelism = 1;
		// parallelism
		if (params.has("parallelism")) {
			parallelism = Integer.parseInt(params.get("parallelism"));
		} else {
			unittest = true;
		}

		List<Tuple2<Float, Float>> vectorsList = buildTuplesOrder(env, size);
		DataSet<Tuple2<Float, Float>> vectors = env.fromCollection(vectorsList).setParallelism(parallelism);

		DataSet<Float> outArray = vectors.map(new VectorAdd());
		List<Float> collect = outArray.collect();

		VectorAdd vadd = new VectorAdd();
		ArrayList<Tuple2<Float, Float>> serialArray = buildTuplesOrderSerial(size);
		List<Float> serial = new ArrayList<>();

		for (Tuple2<Float, Float> t : serialArray) {
			serial.add(vadd.map(t));
		}

		if (unittest) {
			if (CHECK_RESULT) {
				boolean correct = true;
				for (int i = 0; i < collect.size(); i++) {
					if (Math.abs(serial.get(i) - collect.get(i)) > 0.01f) {
						correct = false;
						break;
					}
				}
				if (correct) {
					System.out.println("\033[1;32mSIZE " + size + " ............................... [PASS] \033[0;0m");
				} else {
					System.out.println("\033[1;31mSIZE " + size + " ............................... [FAIL] \033[0;0m");
				}
			}
		}

	}

	public static void main(String[] args) {

		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestAddTwoVectors \033[0;0m");
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
	public static final class VectorAdd implements MapFunction<Tuple2<Float, Float>, Float> {

		@Override
		public Float map (Tuple2<Float,Float> input) {
			return input.f0 + input.f1;
		}
	}

}
