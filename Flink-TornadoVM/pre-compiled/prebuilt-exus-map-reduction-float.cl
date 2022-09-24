// Launch with Global 1D: INPUT SIZE
// Local : null
#pragma OPENCL EXTENSION cl_khr_fp64 : enable
__kernel void mapOperations(__global uchar *_heap_base,
                     ulong _frame_base,
                    __constant uchar *_constant_region,
                    __local uchar *_local_region,
                    __global int *_atomics) {
     //Tuple3<float[], Integer, Integer> -> Tuple2<float[], Integer, Integer>
     // Frame:
     //        83        , 1,     ,   83
     // 3: [ float1[], integer1, float2[], integer2, float3[], integer3,  ... ]  (Input)
     // 4: [integer] (tupleSize)
     // 5: [integer] (arraySize)
     // 6: [ float1[], integer1, float2[], integer2, float3[], integer3,  ... ]  (Output)
    __global ulong *frame = (__global ulong *) &_heap_base[_frame_base];
    ulong4 args = vload4(0, &frame[3]);
    ulong ul_0 = (ulong) frame[3];
    __global int* tupleSize = (__global int *) (args.y + 24);
    __global int* arraySize = (__global int *) (args.z + 24);
    ulong ul_1 = (ulong) frame[6];
    int tupsize = tupleSize[0];
    int arrsize = arraySize[0];

    int idx = get_global_id(0);
    float out;
    float in;
    ulong ul_2;
    float add;

    for (int i = 0; i < tupsize; i++) {
       //output[idx] += input[(i * (arSize + 2)) + idx];
       // read output[idx]
       out = *((__global float *) ((idx * 4) + 24L + ul_1));
       //printf("==== out: %f \n", out);
       // read input[(i * (arSize + 2)) + idx]
       in = *((__global float *) ((4 * idx) + (arrsize*4 + 8)*i + 24L + ul_0));
       //printf("in value: %f \n", in);
       // output[idx] + input[(i * (arSize + 2)) + idx]
       add = in + out;
       ul_2 = (idx * 4) + 24L + ul_1;
       *((__global float *) ul_2) = add;
    }

}
