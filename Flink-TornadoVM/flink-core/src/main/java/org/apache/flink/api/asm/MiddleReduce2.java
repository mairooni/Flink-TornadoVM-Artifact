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
public abstract class MiddleReduce2 {
	public abstract int myredintint(int x, int y);

	public abstract double myreddoubledouble(double x, double y);

	public abstract float myredfloatfloat(float x, float y);

	public abstract long myredlonglong(long x, long y);

	public abstract Tuple2 myredtuple2tuple2(Tuple2 x, Tuple2 y);

	public abstract Tuple3 myredtuple3tuple3(Tuple3 x, Tuple3 y);

	public abstract Tuple4 myredtuple4tuple4(Tuple4 x, Tuple4 y);
}
