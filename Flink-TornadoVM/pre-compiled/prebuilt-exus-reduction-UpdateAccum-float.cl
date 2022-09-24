/*
 * UpdateAccumulator#reduce
 */
#pragma OPENCL EXTENSION cl_khr_fp64 : enable
__kernel void reduce(__global uchar *_heap_base,
                     ulong _frame_base,
                    __constant uchar *_constant_region,
                    __local uchar *_local_region,
                    __global int *_atomics) {
                        
     //Tuple2<Float, Integer> -> Tuple2<Float, Integer>
     // Frame:
     //        83        , 1,         2  ,      83
     // 3: [ float1[], integer1, integer1, float2[], integer2, integer2, float3[], integer3, integer3  ... ]  (Input)
     // 4: [ float[]]  (Input2, product of map function)
     // 5: [ integer ]  (array field size)
     // 6: [ float1[], integer1, float2[], integer2, float3[], integer3,  ... ]  (Output)

    __global ulong *frame = (__global ulong *) &_heap_base[_frame_base];
    ulong4 args = vload4(0, &frame[3]);
    ulong ul_0 = (ulong) frame[3]; // input
    ulong ul_1 = (ulong) frame[4]; // input2
    __global int* arraySizeBytes = (__global int *) (args.z + 24); // frame[5]
    ulong ul_2 = (ulong) frame[6]; //output

    size_t idx = get_global_id(0);

    int arraySize = arraySizeBytes[0];

    if (idx == 0) {
        float in;
        for (int i = 0; i < arraySize; i++) {
            in = *((__global float *) ((4 * i) + 24L + ul_1));
            ulong ul_3 = (4 * i) + 24L + ul_2;
            *((__global float *) ul_3) = in;
            // output[((arraySize + 2) * idx + i)] = input2[((arraySize + 2) * idx + i)];
        }
        int f1 = *((__global int *) (arraySize * 4 + 24L + ul_0));
        ulong ul_4 = arraySize * 4 + 24L + ul_2;
        *((__global int *) ul_4) = f1;
        //output[((arraySize + 2) * idx) + arraySize] = input[((arraySize + 2) * idx) + arraySize];
    }

    __local int localSums[1024];

    size_t localIdx = get_local_id(0);
   	size_t group_size = get_local_size(0);
   	size_t groupID = get_group_id(0);
   	int myID = get_global_id(0);

    // size of array + 2
    int f2 = *((__global int *) ((arraySize*4 + 4) + (arraySize*4 + 8)*idx + 24L + ul_0));
   	localSums[localIdx] = f2; //input[fieldToStore + ((arraySize + 2) * idx)];
   	for (size_t stride = group_size / 2; stride > 0; stride /=2) {
   		barrier(CLK_LOCAL_MEM_FENCE);
   		if (localIdx < stride) {
   		    localSums[localIdx] += localSums[localIdx + stride];
    	}
    }
    if (localIdx == 0) {
        // size of array + 2
        ulong ul_5 = (arraySize*4 + 4) + (arraySize*4 + 8)*groupID + 24L + ul_2;
        *((__global int *) ul_5) = localSums[localIdx];
    	//output[fieldToStore + ((arraySize + 2) * groupID)] = localSums[localIdx];
    }

    // Output
    // Output: [ float1[], Integer, PR1, ----- , PR2,  ------ , PR3 ... ]
    // PR1 + PR2
    // Host: PR1 + PR2
}
