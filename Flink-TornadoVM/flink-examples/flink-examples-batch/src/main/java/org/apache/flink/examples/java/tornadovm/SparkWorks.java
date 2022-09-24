package org.apache.flink.examples.java.tornadovm;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;

public class SparkWorks {

	private enum EXECUTION_MODE {
		MANUAL_GROUP_BY,
		ALTER_EXECUTION,
		VANILLA_FLINK
	}
	public static final EXECUTION_MODE MODE = EXECUTION_MODE.ALTER_EXECUTION;

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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

		// Parallelism to 1 for debugging
		env.setParallelism(1);

		final DataSource<String> stringDataSource = env.readTextFile(filename);

		System.setProperty("tornado", "false");
		final UnsortedGrouping<Tuple4<Long, Double, Long, Long>> groupedDataSource =
			stringDataSource
				.map(new SparksSensorDataLineSplitterMapFunction())
				.map(new TimestampMapFunction())
				.map(new MapFunction<Tuple3<Long, Double, Long>, Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> map(Tuple3<Long, Double, Long> value) {
						return new Tuple4<>(value.f0, value.f1, value.f2, 1L);
					}
				}).groupBy(0, 2);

		if (MODE == EXECUTION_MODE.MANUAL_GROUP_BY) {
			List<Tuple4<Long, Double, Long, Long>> collect1 = groupedDataSource.first(Integer.MAX_VALUE).collect();
			ArrayList<ArrayList<Tuple4<Long, Double, Long, Long>>> allLists = splitCollectionGroup(0, collect1);
			ArrayList<ArrayList<Tuple4<Long, Double, Long, Long>>> newList = new ArrayList<>();
			for (ArrayList<Tuple4<Long, Double, Long, Long>> allList : allLists) {
				ArrayList<ArrayList<Tuple4<Long, Double, Long, Long>>> lists1 = splitCollectionGroup(2, allList);
				newList.addAll(lists1);
			}
			allLists.clear();
			allLists = null;
			System.gc();

			for (ArrayList<Tuple4<Long, Double, Long, Long>> groupData : newList) {
				DataSource<Tuple4<Long, Double, Long, Long>> datasource = env.fromCollection(groupData);
				System.setProperty("tornado", "true");
				datasource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
																   Tuple4<Long, Double, Long, Long> t2) {
						return new Tuple4<>(t1.f0, Math.min(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
					}
				}).writeAsCsv(output + "/min.csv", FileSystem.WriteMode.OVERWRITE);

				datasource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
																   Tuple4<Long, Double, Long, Long> t2) {
						return new Tuple4<>(t1.f0, Math.max(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
					}
				}).writeAsCsv(output + "/max.csv", FileSystem.WriteMode.OVERWRITE);

				datasource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
																   Tuple4<Long, Double, Long, Long> t2) {
						return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2, t1.f3 + 1);
					}
				}).writeAsCsv(output + "/sum.csv", FileSystem.WriteMode.OVERWRITE);

				datasource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
					@Override
					public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
																   Tuple4<Long, Double, Long, Long> t2) {
						return new Tuple4<>(t1.f0, (t1.f1 * t1.f3 + t2.f1 * t2.f3) / (t1.f3 + t2.f3), t1.f2, t1.f3 + t2.f3);
					}
				}).writeAsCsv(output + "/avg.csv", FileSystem.WriteMode.OVERWRITE);

				System.setProperty("tornado", "False");
				datasource
					.reduceGroup(new OutliersDetectionGroupReduceFunction())
					.writeAsCsv(output + "/outliers.csv", FileSystem.WriteMode.OVERWRITE);

			}
			final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");

			System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
				jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));

		} else if (MODE == EXECUTION_MODE.ALTER_EXECUTION) {
			List<Tuple4<Long, Double, Long, Long>> collect1 = groupedDataSource.first(Integer.MAX_VALUE).collect();
			DataSource<Tuple4<Long, Double, Long, Long>> tuple4DataSource = env.fromCollection(collect1);

			System.setProperty("tornado", "True");
			tuple4DataSource.groupBy(0, 2).reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, Math.min(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/min.csv", FileSystem.WriteMode.OVERWRITE);

			tuple4DataSource.groupBy(0, 2).reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, Math.max(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/max.csv", FileSystem.WriteMode.OVERWRITE);

			tuple4DataSource.groupBy(0, 2).reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/sum.csv", FileSystem.WriteMode.OVERWRITE);

			tuple4DataSource.groupBy(0, 2).reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, (t1.f1 * t1.f3 + t2.f1 * t2.f3) / (t1.f3 + t2.f3), t1.f2, t1.f3 + t2.f3);
				}
			}).writeAsCsv(output + "/avg.csv", FileSystem.WriteMode.OVERWRITE);

			System.setProperty("tornado", "False");
			tuple4DataSource
				.groupBy(0, 2)
				.reduceGroup(new OutliersDetectionGroupReduceFunction())
				.writeAsCsv(output + "/outliers.csv", FileSystem.WriteMode.OVERWRITE);

			final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");

			System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
				jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
		} else {
			System.setProperty("tornado", "True");
			groupedDataSource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, Math.min(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/min.csv", FileSystem.WriteMode.OVERWRITE);

			groupedDataSource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, Math.max(t1.f1, t2.f1), t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/max.csv", FileSystem.WriteMode.OVERWRITE);

			groupedDataSource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2, t1.f3 + 1);
				}
			}).writeAsCsv(output + "/sum.csv", FileSystem.WriteMode.OVERWRITE);

			groupedDataSource.reduce(new ReduceFunction<Tuple4<Long, Double, Long, Long>>() {
				@Override
				public Tuple4<Long, Double, Long, Long> reduce(Tuple4<Long, Double, Long, Long> t1,
															   Tuple4<Long, Double, Long, Long> t2) {
					return new Tuple4<>(t1.f0, (t1.f1 * t1.f3 + t2.f1 * t2.f3) / (t1.f3 + t2.f3), t1.f2, t1.f3 + t2.f3);
				}
			}).writeAsCsv(output + "/avg.csv", FileSystem.WriteMode.OVERWRITE);

			System.setProperty("tornado", "False");
			groupedDataSource
				.reduceGroup(new OutliersDetectionGroupReduceFunction())
				.writeAsCsv(output + "/outliers.csv", FileSystem.WriteMode.OVERWRITE);

			final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");

			System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
				jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
		}
	}

	private static ArrayList<ArrayList<Tuple4<Long, Double, Long, Long>>> splitCollectionGroup(int fieldNumber, List<Tuple4<Long, Double, Long, Long>> input) {
		ArrayList<ArrayList<Tuple4<Long, Double, Long, Long>>> lists = new ArrayList<>();
		long previous = input.get(0).getField(fieldNumber);
		long current;
		ArrayList<Tuple4<Long, Double, Long, Long>> sublist = new ArrayList<>();
		sublist.add(input.get(0));
		for (int i = 1; i < input.size(); i++) {
			Tuple4<Long, Double, Long, Long> t = input.get(i);
			current = t.getField(fieldNumber);

			if (current == previous) {
				sublist.add(t);
			} else {
				lists.add(sublist);
				sublist = new ArrayList<>();
			}
			previous = current;
		}
		lists.add(sublist);
		return lists;
	}

	public static class SparksSensorDataLineSplitterMapFunction implements MapFunction<String, Tuple3<Long, Double, Long>>{

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

/*
            return new Tuple3<>(value.getField(0),
                    value.getField(1),
                    timestamp.toInstant(ZoneOffset.UTC).toEpochMilli());
*/
		}
	}

	public static class SumGroupReduceFunction implements GroupReduceFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Double, Long>> values, Collector<Tuple3<Long, Double, Long>> out) {
			Double sum = 0d;
			Long device = null;
			Long timestampWindow = null;
			for (Tuple3<Long, Double, Long> t : values) {
				sum += (Double) t.getField(1);
				device = t.getField(0);
				timestampWindow = t.getField(2);
			}
			Objects.requireNonNull(device);
			Objects.requireNonNull(timestampWindow);
			out.collect(new Tuple3<>(device, sum, timestampWindow));
		}

	}

	public static class AverageGroupReduceFunction implements GroupReduceFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Double, Long>> values, Collector<Tuple3<Long, Double, Long>> out) {
			Double sum = 0d;
			long length = 0;
			Long device = null;
			Long timestampWindow = null;
			for (Tuple3<Long, Double, Long> t : values) {
				sum += (Double) t.getField(1);
				length++;
				device = t.getField(0);
				timestampWindow = t.getField(2);
			}
			Objects.requireNonNull(device);
			Objects.requireNonNull(timestampWindow);
			out.collect(new Tuple3<>(device, sum / length, timestampWindow));
		}

	}

	public static class MinGroupReduceFunction implements GroupReduceFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Double, Long>> values,
						   Collector<Tuple3<Long, Double, Long>> out) {
			Double min = 0d;
			Long device = null;
			Long timestampWindow = null;
			for (Tuple3<Long, Double, Long> t : values) {
				min = Math.min(min, t.getField(1));
				device = t.getField(0);
				timestampWindow = t.getField(2);
			}
			Objects.requireNonNull(device);
			Objects.requireNonNull(timestampWindow);
			out.collect(new Tuple3<>(device, min, timestampWindow));
		}

	}

	public static class MaxGroupReduceFunction implements GroupReduceFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Double, Long>> values, Collector<Tuple3<Long, Double, Long>> out) {
			Double max = 0d;
			Long device = null;
			Long timestampWindow = null;
			for (Tuple3<Long, Double, Long> t : values) {
				max = Math.max(max, t.getField(1));
				device = t.getField(0);
				timestampWindow = t.getField(2);
			}
			Objects.requireNonNull(device);
			Objects.requireNonNull(timestampWindow);
			out.collect(new Tuple3<>(device, max, timestampWindow));
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
