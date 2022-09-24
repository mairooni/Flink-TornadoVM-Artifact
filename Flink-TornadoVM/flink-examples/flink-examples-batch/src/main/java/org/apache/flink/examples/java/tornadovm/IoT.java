package org.apache.flink.examples.java.tornadovm;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.DoubleStream;

public class IoT {
	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final String filename;
		final String output;
		Integer parallelism = 1;
		Integer reduceNumber = 1;
		try {
			// access the arguments of the command line tool
			final ParameterTool params = ParameterTool.fromArgs(args);
			if (!params.has("filename") || !params.has("output")) {
				filename = "/tmp/sensordata.csv";
				output = "/tmp";
				System.err.println("No filename specified. Please run with " +
					"--filename <filename> --output <directory>, where filename is the name of the dataset in CSV format and directory the folder that the output will be stored.");
			} else {
				filename = params.get("filename");
				output = params.get("output");
			}

			if (params.has("parallelism")) {
				parallelism = params.getInt("parallelism");
			}

			if (params.has("reduceNumber")) {
				reduceNumber = params.getInt("reduceNumber");
			}

		} catch (Exception ex) {
			System.err.println("No filename specified. Please run 'WindowProcessor " +
				"--filename <filename>, where filename is the name of the dataset in CSV format");
			return;
		}

		if (Objects.nonNull(parallelism)) {
			env.setParallelism(parallelism);
		}

		final DataSource<Tuple4<Long, Double, Long, Long>> datasource = env
			.readCsvFile(filename).types(Long.class, Double.class, Long.class, Long.class);

		if (reduceNumber == 1) {
			System.out.println("Running MIN");
			datasource
				.reduce(new ReduceMin()).collect();
		} else if (reduceNumber == 2) {
			System.out.println("Running MAX");
			datasource
				.reduce(new ReduceMax()).collect();
		} else if (reduceNumber == 3) {
			System.out.println("Running SUM");
			datasource
				.reduce(new ReduceSum())
				.collect();
		} else if (reduceNumber == 4) {
			System.out.println("Running AVG");
			datasource
				.reduce(new ReduceAvg())
				.collect();
		}

		datasource
			.reduceGroup(new OutliersDetectionGroupReduceFunction())
			.writeAsCsv(output + "/outliers.csv", FileSystem.WriteMode.OVERWRITE);

		final JobExecutionResult jobExecutionResult = env.execute("SparkWorks DataSet Window Processor");

		System.out.println(String.format("SparkWorks DataSet Window Processor Job took: %d ms with parallelism: %d",
			jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS), env.getParallelism()));
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

