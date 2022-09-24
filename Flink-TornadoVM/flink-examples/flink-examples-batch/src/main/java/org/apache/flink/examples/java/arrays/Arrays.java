package org.apache.flink.examples.java.arrays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.List;

/**
 * This is the first example where a user function written using Flink's API can automatically be accelerated
 * on Tornado. For the purposes if this example, the execution is directed to Tornado if the data size is
 * bigger than 544288. Moreover, we provide the class ArrayData that produces either small or big datasets
 * of type Integer, Float, Double or Long. If the function getBig(type)Array is called, the execution is
 * performed on the GPU (via Tornado), otherwise, if one of the functions getSmall(type)Array is called,
 * the execution proceeds like in any other Flink program.
 */
public class Arrays {
	public static void main(String[] args) throws Exception {

		System.out.println("\033[1;36mTesting: org.apache.flink.examples.java.arrays.Arrays#Multiplication \033[0;0m");

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		DataSet<Tuple2<Double, Double>> array = ArrayData.getMixTuple2DataSet(env);

		DataSet<Double> outArray = array.map(new Multiplication());

		List<Double> tornado = outArray.collect();

		Multiplication m = new Multiplication();
		Tuple2<Double, Double>[] serial = ArrayData.getMixTuple2DataSetVanillaFlink();
		Double[] out = new Double[serial.length];
		for (int i = 0; i < serial.length; i++) {
			out[i] = m.map(serial[i]);
		}

		boolean correct = true;
		for (int i = 0; i < tornado.size(); i++) {
			if (Math.abs(out[i] - tornado.get(i)) > 0.01f) {
				correct = false;
				break;
			}
		}
		if (correct) {
			System.out.println("\033[1;32mSIZE " +  tornado.size()  + " ............................... [PASS] \033[0;0m");
		} else {
			System.out.println("\033[1;31mSIZE " +  tornado.size()  + " ............................... [FAIL] \033[0;0m");
		}

	}

	/**
	 * A simple Point.
	 */
	public static class Point implements Serializable {

		public int x, y;

		public Point() {
		}

		Point(int x, int y) {
			this.x = x;
			this.y = y;
		}

		public Point add(Point other) {
			x += other.x;
			y += other.y;
			return this;
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	/**
	 * The user function performs a set of simple arithmetic operations.
	 */
	public static final class Multiplication implements MapFunction<Tuple2<Double, Double>, Double> {
		@Override
		public Double map(Tuple2<Double, Double> value) {
			return value.f0 * value.f1;
		}
	}
}
