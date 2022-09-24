package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * The purpose of this abstract class is to enable us to use the newly loaded
 * class like any other regular java class, using casting.
 * This is necessary because if we were to use other techniques, like reflection, we
 * wouldn't be able to pass the ASM generated class to the Task Schedules of Tornado.
 */
public abstract class MiddleMap5 {
	public abstract int mymapintint(int i);

	public abstract int mymapintdouble(double i);

	public abstract int mymapintfloat(float i);

	public abstract int mymapintlong(long i);

	public abstract double mymapdoubledouble(double i);

	public abstract double mymapdoubleint(int i);

	public abstract double mymapdoublefloat(float i);

	public abstract double mymapdoublelong(long i);

	public abstract float mymapfloatfloat(float i);

	public abstract float mymapfloatint(int i);

	public abstract float mymapfloatdouble(double i);

	public abstract float mymapfloatlong(long i);

	public abstract long mymaplonglong(long i);

	public abstract long mymaplongint(int i);

	public abstract long mymaplongdouble(double i);

	public abstract long mymaplongfloat(float i);

	public abstract Float[] mymapfloatarfloatar(Float[] i);

	public abstract Tuple2 mymaptuple2int(int i);

	public abstract int mymapinttuple2(Tuple2 i);

	public abstract double mymapdoubletuple2(Tuple2 i);

	public abstract float mymapfloattuple2(Tuple2 i);

	public abstract long mymaplongtuple2(Tuple2 i);

	public abstract Tuple2 mymaptuple2tuple2(Tuple2 i);

	public abstract Tuple3 mymaptuple3tuple2(Tuple2 i);

	public abstract int mymapinttuple3(Tuple3 i);

	public abstract double mymapdoubletuple3(Tuple3 i);

	public abstract float mymapfloattuple3(Tuple3 i);

	public abstract long mymaplongtuple3(Tuple3 i);

	public abstract Tuple3 mymaptuple3tuple3(Tuple3 i);

	public abstract Tuple3 mymaptuple3tuple4(Tuple4 i);

	public abstract Tuple4 mymaptuple4tuple4(Tuple4 i);

	public abstract Tuple4 mymaptuple4tuple2(Tuple2 i);

	public abstract Tuple2 mymaptuple2tuple3(Tuple3 i);
}
