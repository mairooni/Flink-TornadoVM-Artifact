package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.java.tuple.Tuple4;
import uk.ac.manchester.tornado.api.annotations.Parallel;

/**
 * The functions that will be executed on Tornado.
 */
public class TornadoMap3 {
	public MiddleMap3 mdm;

	public TornadoMap3(MiddleMap3 mdm) {
		this.mdm = mdm;
	}


	public void map(int[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapintint(in[i]);
		}
	}

	public void map(double[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapintdouble(in[i]);
		}

	}

	public void map(float[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapintfloat(in[i]);
		}

	}

	public void map(long[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapintlong(in[i]);
		}

	}

	public void map(double[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoubledouble(in[i]);
		}

	}

	public void map(int[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoubleint(in[i]);
		}

	}

	public void map(float[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoublefloat(in[i]);
		}

	}

	public void map(long[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoublelong(in[i]);
		}

	}

	public void map(float[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloatfloat(in[i]);
		}

	}

	public void map(int[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloatint(in[i]);
		}

	}

	public void map(long[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloatlong(in[i]);
		}

	}

	public void map(double[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloatdouble(in[i]);
		}

	}

	public void map(long[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplonglong(in[i]);
		}

	}

	public void map(int[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplongint(in[i]);
		}

	}

	public void map(float[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplongfloat(in[i]);
		}

	}

	public void map(double[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplongdouble(in[i]);
		}

	}

	public void map(Float[][] in, Float[][] in2, Float[][] out) {
		Float f = in2[0][0];
		for (@Parallel int i = 0; i < in[0].length; i++) {
			out[i] = mdm.mymapfloatarfloatar(in[i]);
		}

	}

	public void map(int[] in, Tuple2[] in2, Tuple2[] out) {
		Tuple2 dummy = in2[0];
		Float ind = (Float) dummy.f1;
		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple2int(in[i]);
		}
	}

	public void map(Tuple2[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapinttuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoubletuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloattuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplongtuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, Tuple2[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple2tuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, Tuple3[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple3tuple2(in[i]);
		}

	}

	public void map(Tuple3[] in, int[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapinttuple3(in[i]);
		}

	}

	public void map(Tuple3[] in, double[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapdoubletuple3(in[i]);
		}

	}

	public void map(Tuple3[] in, float[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymapfloattuple3(in[i]);
		}

	}

	public void map(Tuple3[] in, long[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaplongtuple3(in[i]);
		}

	}

	public void map(Tuple3[] in, Tuple3[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple3tuple3(in[i]);
		}

	}

	public void map(Tuple2[] points, Tuple3[] centroids, Tuple2[] out) {
		Tuple3 dummy = centroids[0];
		Integer in = (Integer) dummy.f0;
		for (@Parallel int i = 0; i < points.length; i++) {
			out[i] = mdm.mymaptuple2tuple2(points[i]);
		}

	}

	public void map(Tuple2[] in, Tuple2[] in2, Tuple2[] out) {
		Tuple2 dummy = in2[0];
		Integer ind = (Integer) dummy.f1;
		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple2tuple2(in[i]);
		}

	}

	public void map(Tuple2[] in, Tuple2[] in2, Tuple4[] out) {
		Tuple2 dummy = in2[0];
		Integer ind = (Integer) dummy.f1;
		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple4tuple2(in[i]);
		}

	}

	public void map(Tuple4[] in, Tuple3[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple3tuple4(in[i]);
		}

	}

	public void map(Tuple4[] in, Tuple4[] out) {

		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple4tuple4(in[i]);
		}

	}

	public void map(Tuple3[] in, Integer[] in2, Tuple2[] out) {
		Integer dummy = in2[0];
		for (@Parallel int i = 0; i < in.length; i++) {
			out[i] = mdm.mymaptuple2tuple3(in[i]);
		}

	}

}
