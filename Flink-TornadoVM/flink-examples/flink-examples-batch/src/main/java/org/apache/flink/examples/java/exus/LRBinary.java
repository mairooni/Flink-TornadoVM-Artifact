package org.apache.flink.examples.java.exus;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;

@SuppressWarnings("serial")
public class LRBinary {

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
		FileOutputFormat<Tuple2<double[], Double>>
			of = new TypeSerializerOutputFormat<>();
		env.readCsvFile(params.get("train")).fieldDelimiter("\t").ignoreFirstLine().types(String.class)
			.flatMap(new ConvertToIndexMatrix(numberOfFeatures))
			.write(of, params.get("binary"), FileSystem.WriteMode.OVERWRITE);
		env.execute();

	//	System.out.println(String.format("Conversion runtime: %d ms", env.getLastJobExecutionResult()
	//		.getNetRuntime(TimeUnit.MILLISECONDS)));

		final TupleTypeInfo<Tuple2<double[], Double>> typeInfo = new TupleTypeInfo(Types.PRIMITIVE_ARRAY(Types.DOUBLE()), Types.DOUBLE());

		FileInputFormat<Tuple2<double[], Double>> inputFormat = new TypeSerializerInputFormat<>(typeInfo);

		//read binary training data
		DataSet<Tuple2<double[], Double>> data = env.readFile(inputFormat, params.get("binary"));
		//Initialize W and b
		DataSet<Tuple2<double[], Integer>> parameters = env.fromElements(new Tuple2(new double[numberOfFeatures + 1], numberOfFeatures));

		DataSet<Tuple1<String>> test_csv = env.readCsvFile(params.get("test")).fieldDelimiter("\t").ignoreFirstLine().types(String.class);
		//convert to Data (X, y)
		List<Tuple2<double[], Double>> list = test_csv.flatMap(new ConvertToIndexMatrix(numberOfFeatures)).collect();
		DataSet<Tuple2<double[], Double>> test_data = env.fromCollection(list);
		env.setParallelism(1);
		//System.setProperty("tornado", "true");
		Utils.tornadoVM = true;
		IterativeDataSet<Tuple2<double[], Integer>> loop = parameters.iterate(params.getInt("iterations", 10));
		DataSet<Tuple2<double[], Integer>> newParameters = data
			// compute a single step using every sample
			.map(new SubUpdate()).withBroadcastSet(loop, "parameters")
			// sum up all the steps
			.reduce(new UpdateAccumulator())
			// average the steps and update all parameters
			.map(new Update());

		 //feed new parameters back into next iteration
		DataSet<Tuple2<double[], Integer>> final_paramsT = loop.closeWith(newParameters);

		DataSet<Tuple4<Double, Double, Double, Double>> results = test_data
			.map(new Predict()).withBroadcastSet(final_paramsT, "params")
			.reduce(new Evaluate())
			.map(new ComputeMetrics());

		List<Tuple4<Double, Double, Double, Double>> tornadoData = results.collect();

		// -------------------------------

		System.out.println("*************************** FLINK EXECUTION: ");
		System.setProperty("tornado", "false");
		Utils.tornadoVM = false;
		DataSet<Tuple1<String>> test_csv1 = env.readCsvFile(params.get("test")).fieldDelimiter("\t").ignoreFirstLine().types(String.class);
		//convert to Data (X, y)
		List<Tuple2<double[], Double>> list1 = test_csv1.flatMap(new ConvertToIndexMatrix(numberOfFeatures)).collect();
		DataSet<Tuple2<double[], Double>> test_data1 = env.fromCollection(list1);

		IterativeDataSet<Tuple2<double[], Integer>> loop1 = parameters.iterate(params.getInt("iterations", 10));
		//train
		DataSet<Tuple2<double[], Integer>> newParameters1 = data
			// compute a single step using every sample
			.map(new SubUpdate()).withBroadcastSet(loop1, "parameters")
			// sum up all the steps
			.reduce(new UpdateAccumulator())
			// average the steps and update all parameters
			.map(new Update());

		// feed new parameters back into next iteration
		DataSet<Tuple2<double[], Integer>> final_paramsF = loop1.closeWith(newParameters1);
	//	final_paramsF.collect();

		DataSet<Tuple4<Double, Double, Double, Double>> results2 = test_data1
			.map(new Predict()).withBroadcastSet(final_paramsF, "params")
			.reduce(new Evaluate())
			.map(new ComputeMetrics());

		List<Tuple4<Double, Double, Double, Double>> flinkData = results2.collect();

		System.out.println("=== tornado data ===");
		for (int i = 0; i < tornadoData.size(); i++) {
			System.out.println(tornadoData.get(i).f0 + ", " + tornadoData.get(i).f1 + ", " + tornadoData.get(i).f2 + ", " + tornadoData.get(i).f3);
		}

		System.out.println("=== flink data ===");
		for (int i = 0; i < flinkData.size(); i++) {
			System.out.println(flinkData.get(i).f0 + ", " + flinkData.get(i).f1 + ", " + flinkData.get(i).f2 + ", " + flinkData.get(i).f3);
		}

		boolean correct = true;
		for (int i = 0; i < flinkData.size(); i++) {
			if (!tornadoData.get(i).f0.equals(flinkData.get(i).f0) || !tornadoData.get(i).f1.equals(flinkData.get(i).f1) || !tornadoData.get(i).f2.equals(flinkData.get(i).f2) || !tornadoData.get(i).f3.equals(flinkData.get(i).f3)) {
				correct = false;
				break;
			}
		}

		if (correct) {
			System.out.println("\033[1;32mTEST ............................... [PASS] \033[0;0m");
		} else {
			System.out.println("\033[1;31mTEST ............................... [FAIL] \033[0;0m");
		}

	}

	public static class ConvertToIndexMatrix implements FlatMapFunction<Tuple1<String>, Tuple2<double[], Double>> {

		private int n;

		ConvertToIndexMatrix(int n) {
			this.n = n;
		}


		@Override
		public void flatMap(Tuple1<String> stringTuple1, Collector<Tuple2<double[], Double>> collector) {
			String line = stringTuple1.f0;
			int col = -1;
			double[] X = new double[n + 1];
			double y = -1.0;
			for (String cell : line.split(",")) {

				//when col is -1 , then take the row number , else collect values
				if (col > -1 && col < n) {
					X[col] = cell != null ? Double.valueOf(cell) : 0.0;
				} else if (col == n) {
					X[col] = 1.0;
					y = Double.valueOf(cell);
				}
				col++;
			}

			collector.collect(new Tuple2(X, y));
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
			//System.out.println("++++ parameter.f0[82]: " + parameter.f0[82]);

			double z = 0.0;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			double error = (double) 1 / (1 + Math.exp(-z)) - in.f1;

			for (int j = 0; j < n + 1; j++) {
				in.f0[j] = parameter.f0[j] - lr * (error * in.f0[j]);
			}

			return new Tuple2<>(new Tuple2(in.f0, n), count);

		}
	}

	/**
	 * Compute TP,TN,FP,FN
	 */
	public static class Predict extends RichMapFunction<Tuple2<double[], Double>, Tuple4<Integer, Integer, Integer, Integer>> {

		private Collection<Tuple2<double[], Integer>> parameters;

		private Tuple2<double[], Integer> parameter;

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.parameters = getRuntimeContext().getBroadcastVariable("params");
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(Tuple2<double[], Double> in) {

			int n = 82;

			parameter = (Tuple2<double[], Integer>) ((List) parameters).get(0);

			double z = 0.0;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			double predict = ((double) 1 / (1 + Math.exp(-z))) > 0.5 ? 1.0 : 0.0;

			//int f0 = 0, f1 = 0, f2 = 0, f3 = 0;

//			if (predict == 1.0 && df1 == 0.0) {
//				f2 = 1;
//			}
//			if (predict == 1.0 && df1 == 1.0) {
//				f0 = 1;
//			}

			int f0 = -1, f1 = -1, f2 = -1, f3 = -1;
			if (predict == 0.0 && in.f1 == 0.0) {
				f0 = 0; f1 = 1; f2 = 0; f3 = 0;
				//return new Tuple4<>(0, 1, 0, 0); // tn
			}
			if (predict == 0.0 && in.f1 == 1.0) {
				f0 = 0; f1 = 0; f2 = 0; f3 = 1;
				///return new Tuple4<>(0, 0, 0, 1); // fn
			}
			if (predict == 1.0 && in.f1 == 0.0) {
				f0 = 0; f1 = 0; f2 = 1; f3 = 0;
				//return new Tuple4<>(0, 0, 1, 0); // fp
			}
			if (predict == 1.0 && in.f1 == 1.0) {
				f0 = 1; f1 = 0; f2 = 0; f3 = 0;
				//return new Tuple4<>(1, 0, 0, 0); // tp
			}

			if (in.f1 != 1.0 && in.f1 != 0.0) {
				f0 = 0; f1 = 0; f2 = 0; f3 = 0;
			} // tp


			return new Tuple4<>(f0, f1, f2, f3);

		}
	}

	/**
	 * Accumulator all the update.
	 */
	public static class UpdateAccumulator implements ReduceFunction<Tuple2<Tuple2<double[], Integer>, Integer>> {

		private int n = 82;

		@Override
		public Tuple2<Tuple2<double[], Integer>, Integer> reduce(Tuple2<Tuple2<double[], Integer>, Integer> val1, Tuple2<Tuple2<double[], Integer>, Integer> val2) {

			for (int j = 0; j < n + 1; j++) {
				val1.f0.f0[j] = val1.f0.f0[j] + val2.f0.f0[j];
			}
			return new Tuple2<>(val1.f0, val1.f1 + val2.f1);

		}
	}

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


	public static class ComputeMetrics implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Double, Double, Double, Double>> {
		@Override
		public Tuple4<Double, Double, Double, Double> map(Tuple4<Integer, Integer, Integer, Integer> v) {
			double acc = 1.0 - (double) (v.f2 + v.f3) / (v.f0 + v.f1 + v.f2 + v.f3);
			double pr = (double) v.f0 / (v.f0 + v.f2);
			double rec = (double) v.f0 / (v.f0 + v.f3);
			double f1 = (2 * pr * rec) / (pr + rec);
			return new Tuple4(acc, pr, rec, f1);
		}

	}


}
