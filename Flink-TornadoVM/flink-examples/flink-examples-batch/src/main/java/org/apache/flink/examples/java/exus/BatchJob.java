package org.apache.flink.examples.java.exus;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TODO: add documentation.
 */
@SuppressWarnings("serial")
public class BatchJob {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		//default params
		int numberOfFeatures = 82;
		double alpha = 0.1;
		double lambda = 0.1;

		// convert to binary
//		FileOutputFormat<Tuple2<double[], Double>>
//			of = new TypeSerializerOutputFormat<>();
//
//		env.readCsvFile(params.get("train")).fieldDelimiter("\t").ignoreFirstLine().types(String.class)
//			.flatMap(new ConvertToIndexMatrix(numberOfFeatures))
//			.write(of, params.get("binary"), FileSystem.WriteMode.OVERWRITE);
//		env.execute();
//
//		System.out.println(String.format("Conversion runtime: %d ms", env.getLastJobExecutionResult()
//			.getNetRuntime(TimeUnit.MILLISECONDS)));

		// input formats
		final TupleTypeInfo<Tuple2<double[], Double>> typeInfo = new TupleTypeInfo(Types.PRIMITIVE_ARRAY(Types.DOUBLE()), Types.DOUBLE());

		FileInputFormat<Tuple2<double[], Double>> inputFormat = new TypeSerializerInputFormat<>(typeInfo);
		//read binary training data
		DataSet<Tuple2<double[], Double>> data =  getDummyData(env); //env.readFile(inputFormat, params.get("binary"));
		//Initialize W and b
		DataSet<Tuple2<double[], Integer>> parameters = env.fromElements(new Tuple2<>(new double[numberOfFeatures + 1], numberOfFeatures));

		// set number of bulk iterations for KMeans algorithm
		//IterativeDataSet<Tuple2<double[], Integer>> loop = parameters.iterate(params.getInt("iterations", 10));

		System.setProperty("tornado", "true");
		//train
		DataSet<Tuple2<Tuple2<double[], Integer>, Integer>> newParameters = data
			// compute a single step using every sample
			.map(new SubUpdate()).withBroadcastSet(parameters, "parameters");
			// sum up all the steps
//			.reduce(new UpdateAccumulator(numberOfFeatures))
//			// average the steps and update all parameters
//			.map(new Update());
//
//		// feed new parameters back into next iteration
//		DataSet<Tuple2<double[], Integer>> finalParams = loop.closeWith(newParameters);
//
//		//read train data-set from csv
//		DataSet<Tuple1<String>> testCsv = env.readCsvFile(params.get("test")).fieldDelimiter("\t").ignoreFirstLine().types(String.class);
//		//convert to Data (X, y)
//		DataSet<Tuple2<double[], Double>> testData = testCsv.flatMap(new ConvertToIndexMatrix(numberOfFeatures));
//		//evaluate results
//		DataSet<Tuple4<Double, Double, Double, Double>> results = testData
//			.map(new Predict(numberOfFeatures)).withBroadcastSet(finalParams, "params")
//			.reduce(new Evaluate())
//			.map(new ComputeMetrics());

		// emit result
//		if (params.has("output")) {
//
//			results
//				.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
//
//			// since file sinks are lazy, we trigger the execution explicitly
//			env.execute("Exus Use Case");
//		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			newParameters.collect();
		//}

		System.out.println(String.format("Job runtime: %d ms", env.getLastJobExecutionResult()
			.getNetRuntime(TimeUnit.MILLISECONDS)));

	}

	public static DataSet<Tuple2<double[], Double>> getDummyData(ExecutionEnvironment env) {
		DataSet<Tuple2<double[], Double>> data;
		ArrayList<Tuple2<double[], Double>> arrayList = new ArrayList<>();
		double[] field1 = new double[82];
		for (int i = 0; i < 82; i++) {
			field1[i] = 2.2;
		}
		for (int i = 0; i < 2; i++) {
			Tuple2<double[], Double> t2 = new Tuple2<>(field1, 3.0);
			arrayList.add(t2);
		}
		data = env.fromCollection(arrayList);
		return data;
	}

	/**
	 * Flatmap.
	 */
	public static class ConvertToIndexMatrix implements FlatMapFunction<Tuple1<String>, Tuple2<double[], Double>> {

		private int n;

		ConvertToIndexMatrix(int n) {
			this.n = n;
		}

		@Override
		public void flatMap(Tuple1<String> stringTuple1, Collector<Tuple2<double[], Double>> collector) {
			String line = stringTuple1.f0;
			int col = -1;
			double[] x = new double[n + 1];
			double y = -1.0;
			for (String cell : line.split(",")) {

				//when col is -1 , then take the row number , else collect values
				if (col > -1 && col < n) {
					x[col] = cell != null ? Double.valueOf(cell) : 0.0;
				} else if (col == n) {
					x[col] = 1.0;
					y = Double.valueOf(cell);
				}
				col++;
			}

			collector.collect(new Tuple2(x, y));
		}
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Compute a single BGD type update for every parameters.
	 */
	public static class SubUpdate extends RichMapFunction<Tuple2<double[], Double>, Tuple2<Tuple2<double[], Integer>, Integer>> {

		private Collection<Tuple2<double[], Integer>> parameters;

		private Tuple2<double[], Integer> parameter;

//		SubUpdate(int n, double lr) {
//			this.n = n;
//			this.lr = lr;
//		}

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
		}

		@Override
		public Tuple2<Tuple2<double[], Integer>, Integer> map(Tuple2<double[], Double> in) {
			int count = 1;
			int n = 82;
			double lr = 0.1;

			parameter = (Tuple2<double[], Integer>) ((List) parameters).get(0);

			double z = 0.0;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			double error = (double) 1 / (1 + Math.exp(-z)) - in.f1;

			for (int j = 0; j < n + 1; j++) {
				in.f0[j] = parameter.f0[j] - lr * (error * in.f0[j]);
			}

			return new Tuple2<>(new Tuple2<>(in.f0, n), count);

		}
	}

	/**
	 * Compute TP,TN,FP,FN.
	 */
	public static class Predict extends RichMapFunction<Tuple2<double[], Double>, Tuple4<Integer, Integer, Integer, Integer>> {

		private Collection<Tuple2<double[], Integer>> parameters;

		private Tuple2<double[], Integer> parameter;

		private int n;

		Predict(int n) {
			this.n = n;
		}

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.parameters = getRuntimeContext().getBroadcastVariable("params");
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(Tuple2<double[], Double> in) {

			// TODO: Could this be moved out of the map function?
			for (int i = 0; i < parameters.size(); i++) {
				parameter = (Tuple2<double[], Integer>) ((List) parameters).get(i);
			}

			double z = 0.0;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			double predict = ((double) 1 / (1 + Math.exp(-z))) > 0.5 ? 1.0 : 0.0;

			if (predict == 0.0 && in.f1 == 0.0) {
				return new Tuple4<>(0, 1, 0, 0); // tn
			} else if (predict == 0.0 && in.f1 == 1.0) {
				return new Tuple4<>(0, 0, 0, 1); // fn
			} else if (predict == 1.0 && in.f1 == 0.0) {
				return new Tuple4<>(0, 0, 1, 0); // fp
			} else if (predict == 1.0 && in.f1 == 1.0) {
				return new Tuple4<>(1, 0, 0, 0); // tp
			} else {
				return new Tuple4<>(0, 0, 0, 0); // tp
			}

		}
	}

	/**
	 * Accumulator all the update.
	 */
	public static class UpdateAccumulator implements ReduceFunction<Tuple2<Tuple2<double[], Integer>, Integer>> {

		private int n;

		UpdateAccumulator(int n) {
			this.n = n;
		}

		@Override
		public Tuple2<Tuple2<double[], Integer>, Integer> reduce(Tuple2<Tuple2<double[], Integer>, Integer> val1, Tuple2<Tuple2<double[], Integer>, Integer> val2) {

			for (int j = 0; j < n + 1; j++) {
				val1.f0.f0[j] = val1.f0.f0[j] + val2.f0.f0[j];
			}
			return new Tuple2<>(val1.f0, val1.f1 + val2.f1);

		}
	}

	/**
	 * Reduction.
	 */
	public static class Evaluate implements ReduceFunction<Tuple4<Integer, Integer, Integer, Integer>> {

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> reduce(Tuple4<Integer, Integer, Integer, Integer> val1, Tuple4<Integer, Integer, Integer, Integer> val2) {
			return new Tuple4<>(val1.f0 + val2.f0, val1.f1 + val2.f1, val1.f2 + val2.f2, val1.f3 + val2.f3);
		}
	}

	/**
	 * Compute the final update by average them.
	 */
	public static class Update implements MapFunction<Tuple2<Tuple2<double[], Integer>, Integer>, Tuple2<double[], Integer>> {

		@Override
		public Tuple2<double[], Integer> map(Tuple2<Tuple2<double[], Integer>, Integer> arg0) {
			Tuple2<double[], Integer> arg1 = arg0.f0;
			Integer arg2 = arg0.f1;
			for (int i = 0; i < arg1.f0.length; i++) {
				arg1.f0[i] = arg1.f0[i] / arg2;
			}
			return new Tuple2<>(arg1.f0, arg1.f1);
		}

	}

	/**
	 * Map function.
	 */
	public static class ComputeMetrics implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Double, Double, Double, Double>> {
		@Override
		public Tuple4<Double, Double, Double, Double> map(Tuple4<Integer, Integer, Integer, Integer> v) {
			double acc = 1.0 - (double) (v.f2 + v.f3) / (v.f0 + v.f1 + v.f2 + v.f3);
			double pr = (double) v.f0 / (v.f0 + v.f2);
			double rec = (double) v.f0 / (v.f0 + v.f3);
			double f1 = (2 * pr * rec) / (pr + rec);
			return new Tuple4<>(acc, pr, rec, f1);
		}

	}
}
