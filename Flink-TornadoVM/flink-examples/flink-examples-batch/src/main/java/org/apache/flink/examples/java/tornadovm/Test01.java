package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import uk.ac.manchester.tornado.api.TornadoDriver;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

import java.util.ArrayList;
import java.util.List;

/**
 * TornadoVM tests.
 */
public class Test01 {

	private static final boolean CHECK_RESULT = true;

	private static Integer[] buildDataSetIntegers(ExecutionEnvironment env, final int size) {
		Integer[] array = new Integer[size];
		for (int i = 0; i < array.length; i++) {
			array[i] = i;
		}
		return array;
	}

	private static Float[] buildDataSetFloat(ExecutionEnvironment env, final int size) {
		Float[] array = new Float[size];
		for (int i = 0; i < array.length; i++) {
			array[i] = (float) i;
		}
		return array;
	}

	private static Double[] buildDataSetDouble(ExecutionEnvironment env, final int size) {
		Double[] array = new Double[size];
		for (int i = 0; i < array.length; i++) {
			array[i] = (double) i;
		}
		return array;
	}

	private static DataSet<Tuple2<Integer, Integer>> buildTuplesIntegers(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Integer, Integer> t2 = new Tuple2<>(i, i);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static DataSet<Tuple2<Float, Float>> buildTuplesFloat(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple2<Float, Float>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Float, Float> t2 = new Tuple2<>((float) i, (float) i);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static DataSet<Tuple2<Float, Float>> buildTuplesOrder(ExecutionEnvironment env, final int size) {
		ArrayList<Tuple2<Float, Float>> arrayList = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Float, Float> t2 = new Tuple2<>((float) i, (float) i);
			arrayList.add(t2);
		}
		return env.fromCollection(arrayList);
	}

	private static ExecutionEnvironment configureFlink(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		return env;
	}

	private static void testCopyFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Integer[] input = buildDataSetIntegers(env, size);
		DataSet<Integer> array =  env.fromElements(input);
		DataSet<Integer> outArray = array.map(new CopyIntegers());
		List<Integer> collect = outArray.collect();

		if (CHECK_RESULT) {
			boolean correct = true;
			for (int i = 0; i < collect.size(); i++) {
				if (!input[i].equals(collect.get(i))) {
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

	private static void testCopyDoublesFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		Double[] input = buildDataSetDouble(env, size);
		DataSet<Double> array =  env.fromElements(input);
		DataSet<Double> outArray = array.map(new CopyDoubles());
		List<Double> collect = outArray.collect();

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

	private static void testVectorAddFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple2<Float, Float>> array = buildTuplesOrder(env, size);
		DataSet<Tuple2<Float, Float>> outArray = array.map(new VectorAdd());
		outArray.collect();
	}

	private static void testTuplesIntegersFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple2<Integer, Integer>> array = buildTuplesIntegers(env, size);
		DataSet<Integer> outArray = array.map(new TestTupleInteger());
		outArray.collect();
	}

	private static void testTuplesFloatsFlinkTornado(String[] args, final int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		DataSet<Tuple2<Float, Float>> array = buildTuplesFloat(env, size);
		DataSet<Float> outArray = array.map(new TestTupleFloat());
		outArray.collect();
	}

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

	public static void clean() {
		for (int i = 0; i < TornadoRuntime.getTornadoRuntime().getNumDrivers(); i++) {
			final TornadoDriver driver = TornadoRuntime.getTornadoRuntime().getDriver(i);
			driver.getDefaultDevice().reset();
		}
	}

	private static void testCopyIntegers(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testCopyFlinkTornado(args, s);
		}
	}

	private static void testCopyFloats(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testCopyFloatsFlinkTornado(args, s);
		}
	}

	private static void testCopyDoubles(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testCopyDoublesFlinkTornado(args, s);
		}
	}

	private static void testVectorAdd(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testVectorAddFlinkTornado(args, s);
		}
	}

	private static void testTupleIntegers(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testTuplesIntegersFlinkTornado(args, s);
		}
	}

	private static void testTupleFloats(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			clean();
			testTuplesFloatsFlinkTornado(args, s);
		}
	}

	public static void main(String[] args) {

		// 1. Test copies Integers
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.Test01#CopyIntegers \033[0;0m");
		try {
			testCopyIntegers(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

		// 2. Test copies Floats
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.Test01#CopyFloats \033[0;0m");
		try {
			testCopyFloats(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

		// 3. Test copies Doubles
		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.tornadovm.Test01#CopyDoubles \033[0;0m");
		try {
			testCopyDoubles(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

		// 4. Test Vector Addition
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.Test01#vectorAdd \033[0;0m");
		try {
			testVectorAdd(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

		// 5. Test Vector Addition
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.Test01#testTupleIntegers \033[0;0m");
		try {
			testTupleIntegers(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

		// 6. Test Vector Addition
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.Test01#testTupleFloats \033[0;0m");
		try {
			testTupleFloats(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}

	}

	/**
	 * Performs a copy (map operation).
	 */
	public static final class CopyIntegers implements MapFunction<Integer, Integer> {
		@Override
		public Integer map(Integer value) {
			return value;
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

	/**
	 * Performs a copy (map operation) for Double.
	 */
	public static final class CopyDoubles implements MapFunction<Double, Double> {
		@Override
		public Double map(Double value) {
			return value;
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

	/**
	 * Test from Tuple2 to Float.
	 */
	public static final class TestTupleFloat implements MapFunction<Tuple2<Float, Float>, Float> {
		@Override
		public Float map(Tuple2<Float, Float> value) {
			return value.f0 + value.f1;
		}
	}

	/**
	 * Simple vector add.
	 */
	public static final class VectorAdd implements MapFunction<Tuple2<Float, Float>, Tuple2<Float, Float>> {
		@Override
		public Tuple2<Float, Float> map(Tuple2<Float, Float> value) {
			return new Tuple2<>(value.f0, value.f1 + value.f1);
		}
	}
}
