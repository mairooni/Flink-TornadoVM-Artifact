#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  float f_20, f_15, f_9;
  long l_6, l_7, l_11, l_12, l_13, l_16, l_17, l_18, l_5;
  ulong ul_8, ul_19, ul_14, ul_0, ul_1, ul_2;
  int i_2, i_10, i_3, i_4, i_21, i_22;

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[3];
  ul_1  =  (ulong) _frame[5];

  // get input size
  ulong3 args = vload3(0, &_frame[3]);
  __global int* tupleSize = (__global int *) (args.y + 24);
  int size = tupleSize[0];

  i_2  =  get_global_id(0);
  // BLOCK 1 MERGES [0 2 ]
  i_3  =  i_2;
  for(;i_3 < size;)
  {
    // BLOCK 2
    i_4  =  i_3 << 1;
    l_5  =  (long) i_4;
    l_6  =  l_5 << 2;
    l_7  =  l_6 + 24L;
    ul_8  =  ul_0 + l_7;
    f_9  =  *((__global float *) ul_8);
    i_10  =  i_4 + 1;
    l_11  =  (long) i_10;
    l_12  =  l_11 << 2;
    l_13  =  l_12 + 24L;
    ul_14  =  ul_0 + l_13;
    f_15  =  *((__global float *) ul_14);
    l_16  =  (long) i_3;
    l_17  =  l_16 << 2;
    l_18  =  l_17 + 24L;
    ul_19  =  ul_1 + l_18;
    f_20  =  f_9 + f_15;
    *((__global float *) ul_19)  =  f_20;
    i_21  =  get_global_size(0);
    i_22  =  i_21 + i_3;
    i_3  =  i_22;
  }  // B2
  // BLOCK 3
  return;
}  //  kernel
