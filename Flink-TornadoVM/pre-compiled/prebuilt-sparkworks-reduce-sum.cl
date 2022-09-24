#define TUPLE_SIZE 4

void reduceOCL(__global double* input,
               __global double* output,
               const int fieldToStore,
               __local double localSums[])
{
	size_t idx = get_global_id(0);
	size_t localIdx = get_local_id(0);
	size_t group_size = get_local_size(0);
	size_t groupID = get_group_id(0);
	int myID = get_global_id(0);

	localSums[localIdx] = input[fieldToStore + (TUPLE_SIZE * idx)];
	for (size_t stride = group_size / 2; stride > 0; stride /=2) {
		barrier(CLK_LOCAL_MEM_FENCE);
		if (localIdx < stride) {
                  localSums[localIdx] += localSums[localIdx + stride];
		}
	}

	if (localIdx == 0) {
		output[fieldToStore + (TUPLE_SIZE * groupID)] = localSums[localIdx];
	}
}

#pragma OPENCL EXTENSION cl_khr_fp64 : enable
__kernel void reduce(__global uchar *_heap_base,
                     ulong _frame_base,
                    __constant uchar *_constant_region,
                    __local uchar *_local_region,
                    __global int *_atomics) {
     //Tuple4<Long, Double, Long, Long> -> Tuple4<Long, Double, Long, Long>
     // Frame:
     //           0      1      2      3       4      5     6       7    | ... 
     //     |-----------------------------| -----------------------------|-------------|
     // 3:  [ long1, double1, long1, long1, long2, double2, long2, long2, ...   INPUT
     // 4:  [ long1, double1, long1, long1, long2, double2, long2, long2, ...   OUTPUT
    __global ulong *frame = (__global ulong *) &_heap_base[_frame_base];
    ulong4 args = vload4(0, &frame[3]);
    __global double* input =  (__global double *) (args.x + 24);
    __global double* output = (__global double *) (args.y + 24);

    size_t idx = get_global_id(0);
    __local double localSums[1024];

    // The reductions happens with the field 1 (double)
    reduceOCL(input, output, 1, localSums);
    output[0] = input[0];    
    output[2] = input[2];    
    output[3] = input[3] +  1;    

    // Output
    // Output: [ LONG, PR1, LONG, LONG| LONG2, PR2, LONG, LONG, | ...  ]    (PR = Partial Reduction)
}
