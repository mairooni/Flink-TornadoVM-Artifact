package org.apache.flink.examples.java.tornadovm;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;

/**
 * It runs with AllReduceDriver Flink.
 */
public class SparkWorksAllReduce {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		final String filename;
		final String output;
		Integer parallelism = null;
		try {
			// access the arguments of the command line tool
			final ParameterTool params = ParameterTool.fromArgs(args);
			if (!params.has("filename")) {
				filename = "/tmp/sensordata.csv";
				System.err.println("No filename specified. Please run 'WindowProcessor " +
					"--filename <filename>, where filename is the name of the dataset in CSV format");
			} else {
				filename = params.get("filename");
			}

			if (!params.has("output")) {
				output = "/tmp";
				System.err.println("No output filename specified. Please run 'WindowProcessor " +
					"--output <filename>, where filename is the name of the dataset in CSV format");
			} else {
				output = params.get("output");
			}

			if (params.has("parallelism")) {
				parallelism = params.getInt("parallelism");
			}

		} catch (Exception ex) {
			System.err.println("No filename specified. Please run 'WindowProcessor " +
				"--filename <filename>, where filename is the name of the dataset in CSV format");
			return;
		}

		if (Objects.nonNull(parallelism)) {
			env.setParallelism(parallelism);
		}


		env.setParallelism(8);

		final DataSource<String> stringDataSource = env.readTextFile(filename);

		System.setProperty("tornado.flink", "False");
		final MapOperator<Tuple3<Long, Double, Long>, Tuple4<Long, Double, Long, Long>> groupedDataSource =
			stringDataSource
				.map(new SparksSensorDataLineSplitterMapFunction())
				.map(new TimestampMapFunction())
				.map(new MapFunction<Tuple3<Long, Double, Long>, Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> map(Tuple3<Long, Double, Long> value) {
						return new Tuple4<>(value.f0, value.f1, value.f2, 1L);
					}
				});

		List<Tuple4<Long, Double, Long, Long>> collect1 = groupedDataSource.collect();
		DataSource<Tuple4<Long, Double, Long, Long>> datasource = env.fromCollection(collect1);

		/**
		 * Currently we can evaluate in TornadoVM two functions together. This is due to memory limitations.
		 */

		System.setProperty("tornado.flink", "True");
		datasource
			.reduce(new ReduceMin())
			.writeAsCsv(output + "/min.csv", FileSystem.WriteMode.OVERWRITE);

		datasource
			.reduce(new ReduceMax())
			.writeAsCsv(output + "/max.csv", FileSystem.WriteMode.OVERWRITE);

		datasource
			.reduce(new ReduceSum())
			.writeAsCsv(output + "/sum.csv", FileSystem.WriteMode.OVERWRITE);

//		datasource
//			.reduce(new ReduceAvg())
//			.writeAsCsv(output + "/avg.csv", FileSystem.WriteMode.OVERWRITE);

		datasource
			.reduceGroup(new OutliersDetectionGroupReduceFunction())
			.writeAsCsv(output + "/outliers.csv", FileSystem.WriteMode.OVERWRITE);


		final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");

		System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
			jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
	}

	public static class SparksSensorDataLineSplitterMapFunction implements MapFunction<String, Tuple3<Long, Double, Long>> {

		@Override
		public Tuple3<Long, Double, Long> map(String line) {
			String[] tokens = line.split("(,|;)\\s*");
			if (tokens.length != 3) {
				throw new IllegalStateException("Invalid record: " + line);
			}
			return new Tuple3<>(getNumericReferenceNumber(tokens[0]
				.substring(0, tokens[0].indexOf("/"))
				.replaceAll("[^\\d.]", "")),
				Double.parseDouble(tokens[2]),
				Long.parseLong(tokens[1]));
		}

		public static Long getNumericReferenceNumber(String str) {
			String result = "";
			for (int i = 0; i < str.length(); i++) {
				char ch = str.charAt(i);
				if (Character.isLetter(ch)) {
					char initialCharacter = Character.isUpperCase(ch) ? 'A' : 'a';
					result = result.concat(String.valueOf((ch - initialCharacter + 1)));
				} else {
					result = result + ch;
				}
			}
			return Long.parseLong(result);
		}
	}

	public static class SparksSensorDataLineSplitter implements FlatMapFunction<String, Tuple3<String, Double, Long>> {

		@Override
		public void flatMap(String line, Collector<Tuple3<String, Double, Long>> out) {

			String[] tokens = line.split("(,|;)\\s*");
			if (tokens.length != 3) {
				throw new IllegalStateException("Invalid record: " + line);
			}
			out.collect(new Tuple3<>(tokens[0], Double.parseDouble(tokens[2]), Long.parseLong(tokens[1])));
		}

	}

	public static class TimestampMapFunction implements MapFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

		public final int DEFAULT_WINDOW_MINUTES = 5;

		public int windowMinutes;

		public TimestampMapFunction() {
			this.windowMinutes = DEFAULT_WINDOW_MINUTES;
		}

		public int getWindowMinutes() {
			return windowMinutes;
		}

		public void setWindowMinutes(final int windowMinutes) {
			this.windowMinutes = windowMinutes;
		}

		public Tuple3<Long, Double, Long> map(Tuple3<Long, Double, Long> value) {
			LocalDateTime timestamp =
				LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getField(2)),
					ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
			int minute = Math.floorDiv(timestamp.get(ChronoField.MINUTE_OF_HOUR), getWindowMinutes());
			timestamp.with(ChronoField.MINUTE_OF_HOUR, minute);
			return new Tuple3<>(value.getField(0),
				value.getField(1),
				Long.valueOf(minute));
		}
	}

	public static class ReduceMin implements ReduceFunction<Tuple4<Long, Double, Long, Long>> {
		@Override
		public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
													   Tuple4<Long, Double, Long, Long> t2) {
			return new Tuple4<>(t1.f0, Math.min(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
		}
	}

	public static class ReduceMax implements ReduceFunction<Tuple4<Long, Double, Long, Long>> {
		@Override
		public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
													   Tuple4<Long, Double, Long, Long> t2) {
			return new Tuple4<>(t1.f0, Math.max(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
		}
	}

	public static class ReduceSum implements ReduceFunction<Tuple4<Long, Double, Long, Long>> {
		@Override
		public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
													   Tuple4<Long, Double, Long, Long> t2) {
			return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2, t1.f3 + 1);
		}
	}

	public static class ReduceAvg implements ReduceFunction<Tuple4<Long, Double, Long, Long>> {
		@Override
		public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
													   Tuple4<Long, Double, Long, Long> t2) {
			return new Tuple4<>(t1.f0, (t1.f1 * t1.f3 + t2.f1 * t2.f3) / (t1.f3 + t2.f3), t1.f2, t1.f3 + t2.f3);
		}
	}

	public static class OutliersDetectionGroupReduceFunction implements GroupReduceFunction<Tuple4<Long, Double, Long, Long>, Tuple3<Long, Double, Long>> {

		@Override
		public void reduce(Iterable<Tuple4<Long, Double, Long, Long>> values, Collector<Tuple3<Long, Double, Long>> out) {
			DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
			Long device = null;
			Long timestampWindow = null;
			for (Tuple4<Long, Double, Long, Long> sensorData : values) {
				descriptiveStatistics.addValue(sensorData.getField(1));
				device = sensorData.getField(0);
				timestampWindow = sensorData.getField(2);
			}
			double std = descriptiveStatistics.getStandardDeviation();
			double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
			double upperThreshold = descriptiveStatistics.getMean() + 2 * std;

			Objects.requireNonNull(device);
			Objects.requireNonNull(timestampWindow);

			Long finalDevice = device;
			Long finalTimestampWindow = timestampWindow;

			DoubleStream.of(descriptiveStatistics.getValues()).boxed().forEach(sd -> {
				if (sd < lowerThreshold || sd > upperThreshold) {
					//System.out.println(String.format("Detected Outlier value: %s.", sd));
					out.collect(new Tuple3<>(finalDevice, sd, finalTimestampWindow));
				}
			});
		}
	}
}
