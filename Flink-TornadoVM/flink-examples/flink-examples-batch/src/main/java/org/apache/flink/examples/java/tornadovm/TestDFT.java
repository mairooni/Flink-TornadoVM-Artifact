package org.apache.flink.examples.java.tornadovm;

import org.apache.flink.api.common.eventtime.IngestionTimeAssigner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TornadoFlinkTypeRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestDFT {

	private static int[] getInputSizes() {
		int[] sizes = new int[1];
		int base = 128;
		sizes[0] = base;
		for (int i = 1; i < sizes.length; i++) {
			base = base * 2;
			sizes[i] = base;
		}
		return sizes;
	}

	private static ExecutionEnvironment configureFlink(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		return env;
	}

	private static void testTupleIntegers(String[] args) throws Exception {
		int[] sizes = getInputSizes();
		for (int s : sizes) {
			testTuplesIntegersFlinkTornado(args, s);
		}
	}

	private static void testTuplesIntegersFlinkTornado(String[] args, int size) throws Exception {
		final ExecutionEnvironment env = configureFlink(args);
		final ParameterTool params = ParameterTool.fromArgs(args);
		boolean unittest = false;
		int parallelism = 1;
		// size
		if (params.has("size")) {
			size = Integer.parseInt(params.get("size"));
		} else {
			unittest = true;
		}
		// parallelism
		if (params.has("parallelism")) {
			parallelism = Integer.parseInt(params.get("parallelism"));
		}

		System.out.println("Parallelism: " + parallelism);
		DataSet<Tuple2<Float, Float>> data;
		// input data
		if (params.has("data")) {
			data = env.readCsvFile(params.get("data")).fieldDelimiter(",").types(Float.class, Float.class).setParallelism(parallelism);
		} else {
			List<Tuple2<Float, Float>> input = buildData(size);
			data = env.fromCollection(input).setParallelism(parallelism);
		}
		// input indices
		DataSet<Integer> indices;
		if (params.has("indices")) {
			indices = env.readFileOfPrimitives(params.get("indices"), Integer.class);
		} else {
			indices = env.fromCollection(buildIndices(size)).setParallelism(parallelism);
		}

		DataSet<Tuple2<Float, Float>> outArray = indices.map(new DFT()).withBroadcastSet(data, "data").setParallelism(parallelism);
		List<Tuple2<Float, Float>> tres = outArray.collect();

		if (unittest) {
			System.setProperty("tornado", "false");

			List<Tuple2<Float, Float>> fres = outArray.collect();

			boolean correct = false;
			for (int i = 0; i < fres.size(); i++) {
				correct = false;
				for (int j = 0; j < tres.size(); j++) {
					if (Math.abs(fres.get(i).f0 - tres.get(j).f0) < 0.01f && Math.abs(fres.get(i).f1 - tres.get(j).f1) < 0.01f) {
						//System.out.println("FOUND " + i);
						correct = true;
						break;
					}
				}
				if (!correct) break;
//				if (Math.abs(fres.get(i).f0 - tres.get(i).f0) > 0.01f || Math.abs(fres.get(i).f1 - tres.get(i).f1) > 0.01f) {
//					correct = false;
//					break;
//				}
			}

			if (correct) {
				System.out.println("\033[1;32mSIZE " + size + " ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE " + size + " ............................... [FAIL] \033[0;0m");
			}
		}

	}

	private static List<Tuple2<Float, Float>> buildData(final int size) {
		List<Tuple2<Float, Float>> dataset = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			Tuple2<Float, Float> t = new Tuple2<>(1 / (float) (i + 2), 1 / (float) (i + 2));
			dataset.add(t);
		}
		return dataset;
	}

	private static List<Integer> buildIndices(final int size) {
		List<Integer> indices = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			indices.add(i);
		}
		return indices;
	}


	public static void main(String[] args) {
		System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.tornadovm.TestDFT \033[0;0m");
		try {
			testTupleIntegers(args);
		} catch (TornadoFlinkTypeRuntimeException e) {
			System.out.println("\033[1;36mType not supported ............... [NOT SUPPORTED] \033[0;0m");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("\033[1;31mType not supported ............... [FAIL] \033[0;0m");
			e.printStackTrace();
		}
	}

	/**
	 * User function for matrix multiplication.
	 */
	public static final class DFT extends RichMapFunction<Integer, Tuple2<Float, Float>> {

		private Collection<Tuple2<Float, Float>> data;

		/**
		 * Reads the parameters from a broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) {
			this.data = getRuntimeContext().getBroadcastVariable("data");
		}

		@Override
		public Tuple2<Float, Float> map(Integer index) {
			Float sumreal = 0.0f;
			Float sumimag = 0.0f;

			int n = data.size();
			for (int t = 0; t < n; t++) {
				Tuple2<Float, Float> in = (Tuple2<Float, Float>) ((List) data).get(t);
				float angle = ((2 * (float) Math.PI * t * index) / (float) n);
				sumreal += (in.f0 * (float) (Math.cos(angle)) + in.f1 * (float) (Math.sin(angle)));
				sumimag += -(in.f0 * (float) (Math.sin(angle)) + in.f1 * (float) (Math.cos(angle)));
			}
			return new Tuple2<>(sumreal, sumimag);
		}
	}
}
