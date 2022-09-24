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
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

import java.util.Collection;
import java.util.List;

public class ExusFlinkTornadoFloat {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		//default params
		int numberOfFeatures = 82;
		// convert to binary

		FileOutputFormat<Tuple2<float[], Float>>
			of = new TypeSerializerOutputFormat<>();
		env.readCsvFile(params.get("train")).fieldDelimiter("\t").ignoreFirstLine().types(String.class)
			.flatMap(new ConvertToIndexMatrix(numberOfFeatures))
			.write(of, params.get("binary"), FileSystem.WriteMode.OVERWRITE);
		env.execute();

		//System.out.println(String.format("Conversion runtime: %d ns", env.getLastJobExecutionResult()
		//		.getNetRuntime(TimeUnit.NANOSECONDS)));

		final TupleTypeInfo<Tuple2<float[], Float>> typeInfo = new TupleTypeInfo(Types.PRIMITIVE_ARRAY(Types.FLOAT()), Types.FLOAT());

		FileInputFormat<Tuple2<float[], Float>> inputFormat = new TypeSerializerInputFormat<>(typeInfo);

		//read binary training data
		DataSet<Tuple2<float[], Float>> data = env.readFile(inputFormat, params.get("binary"));
		//Initialize W and b
		DataSet<Tuple2<float[], Integer>> parameters = env.fromElements(new Tuple2(new float[numberOfFeatures + 1], numberOfFeatures));

		DataSet<Tuple1<String>> test_csv = env.readCsvFile(params.get("test")).fieldDelimiter("\t").ignoreFirstLine().types(String.class);
		//convert to Data (X, y)
		List<Tuple2<float[], Float>> list = test_csv.flatMap(new ConvertToIndexMatrix(numberOfFeatures)).collect();
		DataSet<Tuple2<float[], Float>> test_data = env.fromCollection(list);

		//System.setProperty("tornado", "true");
		Utils.tornadoVM = true;
		IterativeDataSet<Tuple2<float[], Integer>> loop = parameters.iterate(params.getInt("iterations", 10));
		DataSet<Tuple2<float[], Integer>> newParameters = data
			// compute a single step using every sample
			.map(new SubUpdate()).withBroadcastSet(loop, "parameters")
			// sum up all the steps
			.reduce(new UpdateAccumulator())
			// average the steps and update all parameters
			.map(new Update());

		//feed new parameters back into next iteration
		DataSet<Tuple2<float[], Integer>> final_paramsT = loop.closeWith(newParameters);

		DataSet<Tuple4<Float, Float, Float, Float>> results = test_data
			.map(new Predict()).withBroadcastSet(final_paramsT, "params")
 		    .reduce(new Evaluate())
			.map(new ComputeMetrics());

		results.print();

	}

	public static class ConvertToIndexMatrix implements FlatMapFunction<Tuple1<String>, Tuple2<float[], Float>> {

		private int n;

		ConvertToIndexMatrix(int n) {
			this.n = n;
		}


		@Override
		public void flatMap(Tuple1<String> stringTuple1, Collector<Tuple2<float[], Float>> collector) {
			String line = stringTuple1.f0;
			int col = -1;
			float[] X = new float[n + 1];
			float y = -1.0f;
			for (String cell : line.split(",")) {

				//when col is -1 , then take the row number , else collect values
				if (col > -1 && col < n) {
					X[col] = cell != null ? Float.valueOf(cell) : 0.0f;
				} else if (col == n) {
					X[col] = 1.0f;
					y = Float.valueOf(cell);
				}
				col++;
			}

			collector.collect(new Tuple2(X, y));
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Compute a single BGD type update for every parameters.
	 */
	public static class SubUpdate extends RichMapFunction<Tuple2<float[], Float>, Tuple2<Tuple2<float[], Integer>, Integer>> {

		private Collection<Tuple2<float[], Integer>> parameters;

		private Tuple2<float[], Integer> parameter;

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
		}

		@Override
		public Tuple2<Tuple2<float[], Integer>, Integer> map(Tuple2<float[], Float> in) {

			int count = 1;
			int n = 82;
			float lr = 0.1f;

			parameter = (Tuple2<float[], Integer>) ((List) parameters).get(0);

			float z = 0.0f;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			float error = (float)(1 / (1 + TornadoMath.exp(-z)) - in.f1);

			for (int j = 0; j < n + 1; j++) {
				in.f0[j] = parameter.f0[j] - lr * (error * in.f0[j]);
			}

			return new Tuple2<>(new Tuple2(in.f0, n), count);

		}
	}

	/**
	 * Compute TP,TN,FP,FN
	 */
	public static class Predict extends RichMapFunction<Tuple2<float[], Float>, Tuple4<Integer, Integer, Integer, Integer>> {

		private Collection<Tuple2<float[], Integer>> parameters;

		private Tuple2<float[], Integer> parameter;

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.parameters = getRuntimeContext().getBroadcastVariable("params");
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(Tuple2<float[], Float> in) {

			int n = 82;

			parameter = (Tuple2<float[], Integer>) ((List) parameters).get(0);

			float z = 0.0f;
			for (int j = 0; j < n + 1; j++) {
				z += in.f0[j] * parameter.f0[j];
			}

			float predict = ((float) 1 / (1 + TornadoMath.exp(-z))) > 0.5f ? 1.0f : 0.0f;

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
	public static class UpdateAccumulator implements ReduceFunction<Tuple2<Tuple2<float[], Integer>, Integer>> {

		private int n = 82;

		@Override
		public Tuple2<Tuple2<float[], Integer>, Integer> reduce(Tuple2<Tuple2<float[], Integer>, Integer> val1, Tuple2<Tuple2<float[], Integer>, Integer> val2) {

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
	public static class Update implements MapFunction<Tuple2<Tuple2<float[], Integer>, Integer>, Tuple2<float[], Integer>> {

		@Override
		public Tuple2<float[], Integer> map(Tuple2<Tuple2<float[], Integer>, Integer> arg0) {
			Tuple2<float[], Integer> arg1 = arg0.f0;
			Integer arg2 = arg0.f1;
			for (int i = 0; i < arg1.f0.length; i++) {
				arg1.f0[i] = arg1.f0[i] / arg2;
			}
			return new Tuple2<>(arg1.f0, arg1.f1);
		}

	}


	public static class ComputeMetrics implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Float, Float, Float, Float>> {
		@Override
		public Tuple4<Float, Float, Float, Float> map(Tuple4<Integer, Integer, Integer, Integer> v) {
			float acc = 1.0f - (float) (v.f2 + v.f3) / (v.f0 + v.f1 + v.f2 + v.f3);
			float pr = (float) v.f0 / (v.f0 + v.f2);
			float rec = (float) v.f0 / (v.f0 + v.f3);
			float f1 = (2 * pr * rec) / (pr + rec);
			return new Tuple4(acc, pr, rec, f1);
		}

	}

}
