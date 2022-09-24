package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * In these reduce skeletons we will patch a call of the Flink UDF, using bytecode manipulation.
 */
public class ReduceASMSkeleton extends MiddleReduce {

	@Override
	public int myredintint(int x, int y) {
		return 0;
	}

	@Override
	public double myreddoubledouble(double x, double y) {
		return 0;
	}

	@Override
	public float myredfloatfloat(float x, float y) {
		return 0;
	}

	@Override
	public long myredlonglong(long x, long y) {
		return 0;
	}

	@Override
	public Tuple2 myredtuple2tuple2(Tuple2 x, Tuple2 y) {
		return null;
	}

	@Override
	public Tuple3 myredtuple3tuple3(Tuple3 x, Tuple3 y) {
		return null;
	}

	@Override
	public Tuple4 myredtuple4tuple4(Tuple4 x, Tuple4 y) {
		return null;
	}
}

