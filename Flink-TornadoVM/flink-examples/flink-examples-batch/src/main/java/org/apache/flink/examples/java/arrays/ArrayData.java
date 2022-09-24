package org.apache.flink.examples.java.arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * This class produces two sizes of datasets, one "big" for Tornado, and one small for regular Flink execution.
 * At the moment TornadoVM does not support Plain Java Objects, therefore, input and output data can only be
 * of type Integer, Double, Float or Long.
 */
public class ArrayData {

	private static final Object[][] POINTS = new Object[][] {
		new Object[] {1, 10},
		new Object[] {2, 9},
		new Object[] {3, 8},
		new Object[] {4, 7},
		new Object[] {5, 6},
	};

	private static final int SMALL_SIZE = 170;

	private static final int BIG_SIZE = 65536;

	static DataSet<Integer> getSmallIntArray(ExecutionEnvironment env) {
		Integer[] array = new Integer[SMALL_SIZE];
		for (int i = 0; i < array.length; i++) {
			array[i] = i;
			System.out.println("array[" + i + "] = " + array[i]);
		}
		return env.fromElements(array);
	}

	public static DataSet<Double> getSmallDoubleArray(ExecutionEnvironment env) {
		Double[] array = new Double[SMALL_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextDouble();
			System.out.println("array[" + i + "] = " + array[i]);
		}
		return env.fromElements(array);
	}

	public static DataSet<Float> getSmallFloatArray(ExecutionEnvironment env) {
		Float[] array = new Float[SMALL_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextFloat();
			System.out.println("array[" + i + "] = " + array[i]);
		}
		return env.fromElements(array);
	}

	public static DataSet<Long> getSmallLongArray(ExecutionEnvironment env) {
		Long[] array = new Long[SMALL_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextLong();
			System.out.println("array[" + i + "] = " + array[i]);
		}
		return env.fromElements(array);
	}

	public static DataSet<Integer> getBigIntArray(ExecutionEnvironment env) {
		Integer[] array = new Integer[BIG_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextInt();
		}
		return env.fromElements(array);
	}

	static Double[] getBigDoubleArray(ExecutionEnvironment env) {
		Double[] array = new Double[BIG_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextDouble();
		}
		return array;
	}

	public static DataSet<Float> getBigFloatArray(ExecutionEnvironment env) {
		Float[] array = new Float[BIG_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextFloat();
		}
		return env.fromElements(array);
	}

	public static DataSet<Long> getBigLongArray(ExecutionEnvironment env) {
		Long[] array = new Long[BIG_SIZE];
		Random rand = new Random();
		for (int i = 0; i < array.length; i++) {
			array[i] = rand.nextLong();
		}
		return env.fromElements(array);
	}

	public static DataSet<Arrays.Point> getDefaultPointDataSet(ExecutionEnvironment env) {
		List<Arrays.Point> pointList = new LinkedList<Arrays.Point>();
		for (Object[] point : POINTS) {
			pointList.add(new Arrays.Point((Integer) point[0], (Integer) point[1]));
		}
		return env.fromCollection(pointList);
	}

	public static DataSet<Tuple2<Integer, Integer>> getDefaultTupleDataSet(ExecutionEnvironment env) {
		List<Tuple2<Integer, Integer>> pointList = new LinkedList<Tuple2<Integer, Integer>>();
		for (int i = 0; i < SMALL_SIZE; i++) {
			pointList.add(new Tuple2(i, i + 2));
			System.out.println("(" + i + ", " + (i + 2) + ")");
		}
		return env.fromCollection(pointList);
	}

	public static DataSet<Tuple2<Double, Double>> getMixTuple2DataSet(ExecutionEnvironment env) {
		List<Tuple2<Double, Double>> pointList = new LinkedList<Tuple2<Double, Double>>();
		Double d = 2.2;
		for (int i = 0; i < SMALL_SIZE; i++) {
			pointList.add(new Tuple2(d, d));
		}
		return env.fromCollection(pointList);
	}

	public static Tuple2<Double, Double>[] getMixTuple2DataSetVanillaFlink() {
		Tuple2<Double, Double>[] out = new Tuple2[SMALL_SIZE];
		Double d = 2.2;
		for (int i = 0; i < SMALL_SIZE; i++) {
			out[i] = new Tuple2(d, d);
		}
		return out;
	}

	public static DataSet<Tuple3<Integer, Integer, Integer>> getDefaultTuple3DataSet(ExecutionEnvironment env) {
		List<Tuple3<Integer, Integer, Integer>> pointList = new LinkedList<Tuple3<Integer, Integer, Integer>>();
		int i = 11;
		for (Object[] point : POINTS) {
			pointList.add(new Tuple3(i, (Integer) point[0], (Integer) point[1]));
			i++;
		}
		return env.fromCollection(pointList);
	}

	public static DataSet<Tuple3<Integer, Integer, Tuple2<Arrays.Point, Integer>>> getMixTuple3DataSet(ExecutionEnvironment env) {
		List<Tuple3<Integer, Integer, Tuple2<Arrays.Point, Integer>>> pointList = new LinkedList<Tuple3<Integer, Integer, Tuple2<Arrays.Point, Integer>>>();
		int i = 11;
		for (Object[] point : POINTS) {
			pointList.add(new Tuple3(i, (Integer) 98, new Tuple2(new Arrays.Point((Integer) point[0], (Integer) point[1]), 99)));
			i++;
		}
		return env.fromCollection(pointList);
	}

}
