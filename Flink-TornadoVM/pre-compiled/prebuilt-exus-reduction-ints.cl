void reduceOCL(__global int* input,
               __global int* output,
               const int fieldToStore,
               __local int localSums[])
{
	size_t idx = get_global_id(0);
	size_t localIdx = get_local_id(0);
	size_t group_size = get_local_size(0);
	size_t groupID = get_group_id(0);
	int myID = get_global_id(0);

	localSums[localIdx] = input[fieldToStore + (4 * idx)];
	for (size_t stride = group_size / 2; stride > 0; stride /=2) {
		barrier(CLK_LOCAL_MEM_FENCE);
		if (localIdx < stride) {
            localSums[localIdx] += localSums[localIdx + stride];
		}
	}

	if (localIdx == 0) {
		output[fieldToStore + (4 * groupID)] = localSums[localIdx];
	}
}
#pragma OPENCL EXTENSION cl_khr_fp64 : enable
__kernel void reduce(__global uchar *_heap_base,
                     ulong _frame_base,
                    __constant uchar *_constant_region,
                    __local uchar *_local_region,
                    __global int *_atomics) {
     //Tuple4<Integer, Integer, Integer, Integer> -> Tuple4<Integer, Integer, Integer, Integer>
     // Frame:
     //           0        1        2         3          4
     //      |--------------------------------------| --------------------------------------|
     // 4:  [ integer1, integer2, integer3, integer4, integer1, integer2, integer3, integer4, ...   INPUT
     // 5:  [ integer1, integer2, integer3, integer4, integer1, integer2, integer3, integer4, ...   OUTPUT
    __global ulong *frame = (__global ulong *) &_heap_base[_frame_base];
    ulong4 args = vload4(0, &frame[3]);
    __global int* input = (__global  int *) (args.x + 24);
    __global int* output = (__global int *) (args.y + 24);
    size_t idx = get_global_id(0);
    __local int localSums[1024];
    reduceOCL(input, output, 0, localSums);
    reduceOCL(input, output, 1, localSums);
    reduceOCL(input, output, 2, localSums);
    reduceOCL(input, output, 3, localSums);
    // Output
    // Output: [ PR1, PR2, PR3, PR4 , PR1, PR2, PR3, PR4 ... ]    (PR = Partial Reduction)
}
