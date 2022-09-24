package org.apache.flink.api.asm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.annotations.Reduce;

/**
 * The functions that will be executed on Tornado.
 */
public class TornadoReduce {

	// This variable will store the ASM generated instance of ReduceASMSkeleton
	public MiddleReduce mdr;

	public TornadoReduce(MiddleReduce mdr) {
		this.mdr = mdr;
	}

	public void reduce(int[] input, @Reduce int[] result) {
		result[0] = 0;
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredintint(input[i], result[0]);
		}
	}

	public void reduce(double[] input, @Reduce double[] result) {
		result[0] = 0.0f;
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myreddoubledouble(input[i], result[0]);
		}
	}

	public void reduce(long[] input, @Reduce long[] result) {
		//result[0] = 0;
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredlonglong(input[i], result[0]);
		}
	}

	public void reduce(float[] input, @Reduce float[] result) {
		result[0] = 0;
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredfloatfloat(input[i], result[0]);
		}
	}

	public void reduce(Tuple2[] input, @Reduce Tuple2[] result) {
		//result[0] =
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredtuple2tuple2(input[i], result[0]);
		}
	}

	public void reduce(Tuple3[] input, @Reduce Tuple3[] result) {
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredtuple3tuple3(input[i], result[0]);
		}
	}

	public void reduce(Tuple4[] input, @Reduce Tuple4[] result) {
		//result[0] = null;
		for (@Parallel int i = 0; i < input.length; i++) {
			result[0] = mdr.myredtuple4tuple4(input[i], result[0]);
		}
	}

}
