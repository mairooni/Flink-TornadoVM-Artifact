package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

/**
 *
 */
public class TestReductionExus {
	private static Tuple2<Tuple2<double[], Integer>, Integer>[] buildDataSetTuple2(final int size) {
		Tuple2<Tuple2<double[], Integer>, Integer>[] array = new Tuple2[size];
		double[] d = new double[83];
		for (int i = 0; i < 83; i++) {
			d[i] = i;
		}
		Tuple2<double[], Integer> t = new Tuple2<>(d, 1);
		for (int i = 0; i < array.length; i++) {
			array[i] = new Tuple2<>(t, 1);
		}
		return array;
	}
	private static ExecutionEnvironment configureFlink(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		return env;
	}
	public static void main (String[] args) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		env.setParallelism(1);
		Tuple2<Tuple2<double[], Integer>, Integer>[] input = buildDataSetTuple2(64);
		DataSet<Tuple2<Tuple2<double[], Integer>, Integer>> array =  env.fromElements(input);
		DataSet<Tuple2<Tuple2<double[], Integer>, Integer>> outArray = array.reduce(new UpdateAccumulator());
		List<Tuple2<Tuple2<double[], Integer>, Integer>> collect = outArray.collect();
		for (Tuple2<Tuple2<double[], Integer>, Integer> t : collect) {
			System.out.print("[ ");
			for (int i = 0; i < t.f0.f0.length; i++) {
				System.out.print(t.f0.f0[i] + "  ");
			}
			System.out.println("] , " + t.f0.f1 + " , " + t.f1);
		}
	}

	public static class ReduceMap implements MapFunction<Tuple2<Tuple2<double[], Integer>, Integer>, Tuple2<Tuple2<double[], Integer>, Integer>> {

		@Override
		public Tuple2<Tuple2<double[], Integer>, Integer> map(Tuple2<Tuple2<double[], Integer>, Integer> value) throws Exception {
			for (int j = 0; j < 83; j++) {
				value.f0.f0[j] += value.f0.f0[j];
			}
			return new Tuple2<>(new Tuple2<>(value.f0.f0, value.f0.f1), value.f1);
		}
	}
	/**
	 *
	 */
	public static class UpdateAccumulator implements ReduceFunction<Tuple2<Tuple2<double[], Integer>, Integer>> {

		@Override
		public Tuple2<Tuple2<double[], Integer>, Integer> reduce(Tuple2<Tuple2<double[], Integer>, Integer> val1, Tuple2<Tuple2<double[], Integer>, Integer> val2) {
			for (int j = 0; j < 83; j++) {
				val1.f0.f0[j] = val1.f0.f0[j] + val2.f0.f0[j];
				//val1.f0.f0[j] += val2.f0.f0[j];
			}
			return new Tuple2<>(new Tuple2<>(val1.f0.f0, val1.f0.f1), val1.f1 + val2.f1);
		}
	}

}
