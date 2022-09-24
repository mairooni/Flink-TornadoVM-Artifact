/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.clustering.util.KMeansData;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 *
 * <p>This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>Usage: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link org.apache.flink.examples.java.clustering.util.KMeansData} and 10 iterations.
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeans {

	static DataSet<Tuple2<Double, Double>> points;
	static DataSet<Tuple3<Integer, Double, Double>> centroids;

	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
		boolean unittest = false;
		if (params.has("data")) {
			String inputFile = params.get("data");
			readDataSetsFromFile(inputFile, env);
		} else {
			points = getPointDataSet(env);
			centroids = getCentroidDataSet(env);
			unittest = true;
		}

		int iterations;
		if (params.has("iterations")) {
			iterations = Integer.parseInt(params.get("iterations"));
		} else {
			iterations = 10;
		}

		if (unittest) {
			System.out.println("\033[1;36m Testing: org.apache.flink.examples.java.clustering.KMeans \033[0;0m");
		}

		if (params.has("parallelism")) {
			env.setParallelism(Integer.parseInt(params.get("parallelism")));
		} else {
			env.setParallelism(1);
		}
		System.out.println("Parallelism: " + env.getParallelism());

		IterativeDataSet<Tuple3<Integer, Double, Double>> loop = centroids.iterate(iterations);
		DataSet<Tuple3<Integer, Double, Double>> newCentroids = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0)
			.reduce(new CentroidAccumulator())
			// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());

		List<Tuple3<Integer, Double, Double>> tornadoRes = loop.closeWith(newCentroids).collect();

		if (!unittest) {
//				for (Tuple3<Integer, Double, Double> res : tornadoRes) {
//					System.out.println(res.f0 + ", " + res.f1 + ", " + res.f2);
//				}
		} else {
			System.setProperty("tornado", "false");
			IterativeDataSet<Tuple3<Integer, Double, Double>> loop2 = centroids.iterate(iterations);
			DataSet<Tuple3<Integer, Double, Double>> newCentroidsSer = points
				// compute closest centroid for each point
				.map(new SelectNearestCenter()).withBroadcastSet(loop2, "centroids")
				// count and sum point coordinates for each centroid
				.map(new CountAppender())
				.groupBy(0).reduce(new CentroidAccumulator())
				// compute new centroids from point counts and coordinate sums
				.map(new CentroidAverager());


			List<Tuple3<Integer, Double, Double>> flinkRes = loop2.closeWith(newCentroidsSer).collect();

			boolean correct = true;
			for (int i = 0; i < tornadoRes.size(); i++) {
				if (!(tornadoRes.get(i).f0.equals(flinkRes.get(i).f0)) || (Math.abs(tornadoRes.get(i).f1 - flinkRes.get(i).f1) > 0.01f || (Math.abs(tornadoRes.get(i).f2 - flinkRes.get(i).f2) > 0.01f))) {
					correct = false;
					break;
				}
			}
			if (correct) {
				System.out.println("\033[1;32mSIZE 256 ............................... [PASS] \033[0;0m");
			} else {
				System.out.println("\033[1;31mSIZE 256 ............................... [FAIL] \033[0;0m");
			}
		}
	}

	// *************************************************************************
	//     DATA SOURCE READING (POINTS AND CENTROIDS)
	// *************************************************************************

	public static void readDataSetsFromFile(String fileName, ExecutionEnvironment env) {

		// This will reference one line at a time
		String line;
		List<Tuple2<Double, Double>> pointList = new LinkedList<>();
		List<Tuple3<Integer, Double, Double>> centroidList = new LinkedList<>();
		boolean flag = false;
		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			// flag is false while we are reading points
			while ((line = bufferedReader.readLine()) != null) {
				if (!line.contains("---- Centroids:") && !flag) {
					StringTokenizer multiTokenizer = new StringTokenizer(line, "{,}");
					String num1 = null;
					String num2 = null;
					while (multiTokenizer.hasMoreTokens()) {
						if (num1 == null) {
							num1 = multiTokenizer.nextToken();
						} else {
							num2 = multiTokenizer.nextToken();
						}
					}
					Tuple2<Double, Double> p = new Tuple2<>(Double.parseDouble(num1), Double.parseDouble(num2));
					pointList.add(p);
				}
				if (line.contains("---- Centroids:")) {
					// we have read all the points, now set flag to true to store the centroids
					flag = true;
					line = bufferedReader.readLine();
				}

				if (flag) {
					// read centroids
					StringTokenizer multiTokenizer = new StringTokenizer(line, "{,}");
					String id = null;
					String num1 = null;
					String num2 = null;
					while (multiTokenizer.hasMoreTokens()) {
						if (id == null) {
							id = multiTokenizer.nextToken();
						} else if (num1 == null) {
							num1 = multiTokenizer.nextToken();
						} else {
							num2 = multiTokenizer.nextToken();
						}
					}
					Tuple3<Integer, Double, Double> c = new Tuple3<>(Integer.parseInt(id), Double.parseDouble(num1), Double.parseDouble(num2));
					centroidList.add(c);
				}
			}
			bufferedReader.close();

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		points = env.fromCollection(pointList);
		centroids = env.fromCollection(centroidList);
	}

	public static DataSet<Tuple3<Integer, Double, Double>> getCentroidDataSet(ExecutionEnvironment env) {
		DataSet<Tuple3<Integer, Double, Double>> centroids;
		centroids = KMeansData.getDefaultCentroidDataSet(env);
		return centroids;
	}

	public static DataSet<Tuple2<Double, Double>> getPointDataSet(ExecutionEnvironment env) {
		DataSet<Tuple2<Double, Double>> points;
		points = KMeansData.getDefaultPointDataSet(env);
		return points;
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	/** Determines the closest cluster center for a data point. */
	//@ForwardedFields("*->1")
	public static final class SelectNearestCenter extends RichMapFunction<Tuple2<Double, Double>, Tuple2<Integer, Tuple2<Double, Double>>> {
		private Collection<Tuple3<Integer, Double, Double>> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Tuple2<Double, Double>> map(Tuple2<Double, Double> p) {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			for (int j = 0; j < centroids.size(); j++) {
				// compute distance
				Tuple3<Integer, Double, Double> centroid = (Tuple3<Integer, Double, Double>) ((List) centroids).get(j);
				double distance = Math.sqrt((p.f0 - centroid.f1) * (p.f0 - centroid.f1) + (p.f1 - centroid.f2) * (p.f1 - centroid.f2));
				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.f0;
				}
			}
			// emit a new record with the center id and the data point.
			return new Tuple2<>(closestCentroidId, p);
		}
	}

	/** Appends a count variable to the tuple. */
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Tuple2<Double, Double>>, Tuple3<Integer, Tuple2<Double, Double>, Long>> {

		@Override
		public Tuple3<Integer, Tuple2<Double, Double>, Long> map(Tuple2<Integer, Tuple2<Double, Double>> t) {
			return new Tuple3<>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Tuple2<Double, Double>, Long>> {

		@Override
		public Tuple3<Integer, Tuple2<Double, Double>, Long> reduce(Tuple3<Integer, Tuple2<Double, Double>, Long> val1, Tuple3<Integer, Tuple2<Double, Double>, Long> val2) {
			Tuple2<Double, Double> addPoints = new Tuple2<>(val1.f1.f0 + val2.f1.f0, val1.f1.f1 + val2.f1.f1);

			return new Tuple3<>(val1.f0, addPoints, val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Tuple2<Double, Double>, Long>, Tuple3<Integer, Double, Double>> {

		@Override
		public Tuple3<Integer, Double, Double> map(Tuple3<Integer, Tuple2<Double, Double>, Long> value) {
			return new Tuple3<>(value.f0, value.f1.f0 / value.f2, value.f1.f1 / value.f2);
		}
	}
}
