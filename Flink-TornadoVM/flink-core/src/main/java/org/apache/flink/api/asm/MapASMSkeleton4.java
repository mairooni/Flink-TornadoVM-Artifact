package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * In these map skeletons we will patch a call of the Flink UDF, using bytecode manipulation.
 */
public class MapASMSkeleton4 extends MiddleMap4 {

	@Override
	public int mymapintint(int i) {
		return 0;
	}

	@Override
	public int mymapintdouble(double i) {
		return 0;
	}

	@Override
	public int mymapintfloat(float i) {
		return 0;
	}

	@Override
	public int mymapintlong(long i) {
		return 0;
	}

	@Override
	public double mymapdoubledouble(double i) {
		return 0;
	}

	@Override
	public double mymapdoubleint(int i) {
		return 0;
	}

	@Override
	public double mymapdoublefloat(float i) {
		return 0;
	}

	@Override
	public double mymapdoublelong(long i) {
		return 0;
	}

	@Override
	public float mymapfloatfloat(float i) {
		return 0;
	}

	@Override
	public float mymapfloatint(int i) {
		return 0;
	}

	@Override
	public float mymapfloatdouble(double i) {
		return 0;
	}

	@Override
	public float mymapfloatlong(long i) {
		return 0;
	}

	@Override
	public long mymaplonglong(long i) {
		return 0;
	}

	@Override
	public long mymaplongint(int i) {
		return 0;
	}

	@Override
	public long mymaplongdouble(double i) {
		return 0;
	}

	@Override
	public long mymaplongfloat(float i) {
		return 0;
	}

	@Override
	public Float[] mymapfloatarfloatar(Float[] i) {
		return null;
	}

	@Override
	public Tuple2 mymaptuple2int(int i) {
		return null;
	}

	@Override
	public int mymapinttuple2(Tuple2 t) {
		return 0;
	}

	@Override
	public double mymapdoubletuple2(Tuple2 i) {
		return 0;
	}

	@Override
	public float mymapfloattuple2(Tuple2 i) {
		return 0;
	}

	@Override
	public long mymaplongtuple2(Tuple2 i) {
		return 0;
	}

	@Override
	public Tuple2 mymaptuple2tuple2(Tuple2 i) {
		return null;
	}

	@Override
	public Tuple3 mymaptuple3tuple2(Tuple2 i) {
		return null;
	}

	@Override
	public int mymapinttuple3(Tuple3 t) {
		return 0;
	}

	@Override
	public double mymapdoubletuple3(Tuple3 i) {
		return 0;
	}

	@Override
	public float mymapfloattuple3(Tuple3 i) {
		return 0;
	}

	@Override
	public long mymaplongtuple3(Tuple3 i) {
		return 0;
	}

	@Override
	public Tuple3 mymaptuple3tuple3(Tuple3 i) {
		return null;
	}

	@Override
	public Tuple3 mymaptuple3tuple4(Tuple4 i) {
		return null;
	}

	@Override
	public Tuple4 mymaptuple4tuple4(Tuple4 i) {
		return null;
	}

	@Override
	public Tuple4 mymaptuple4tuple2(Tuple2 i) {
		return null;
	}

	@Override
	public Tuple2 mymaptuple2tuple3(Tuple3 i) {
		return null;
	}

}
