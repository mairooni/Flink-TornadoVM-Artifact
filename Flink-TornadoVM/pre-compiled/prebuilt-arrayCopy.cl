#pragma OPENCL EXTENSION cl_khr_fp64 : enable
__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region)
{
  double d_15;
  long l_5, l_21, l_4;
  ulong ul_20, ul_8, ul_24, ul_1, ul_0, ul_14;
  int i_2, i_3, i_22, i_23, i_26, i_25, i_13, i_16, i_18, i_17, i_19, i_6, i_7, i_10, i_9, i_12, i_11, index;

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[3];
  ul_1  =  (ulong) _frame[4];
  i_2  =  get_global_id(0);
  // BLOCK 1 MERGES [0 2 ]
  i_3  =  i_2;
  for(;i_3 < 10;)  {
    // BLOCK 2
    l_4  =  (long) i_3;
    l_5  =  l_4 * 48;
    i_6  =  40 + l_5;
    i_7  =  i_6 + 24L;
    ul_8  =  ul_0 + i_7;
    i_9  =  *((__global int *) ul_8);
    for (index = 0; index < 5; index++) {
        i_10  =  8 * index;
        i_11  =  48 * l_4;
        i_12  =  i_10 + i_11;
        i_13  =  i_12 + 24L;
        ul_14  =  ul_0 + i_13;
        d_15  =  *((__global double *) ul_14);
        i_16  =  8 * index;
        i_17  =  48 * l_4;
        i_18  =  i_16 + i_17;
        i_19  =  i_18 + 24L;
        ul_20  =  ul_1 + i_19;
        *((__global double *) ul_20)  =  d_15;
    }
    l_21  =  l_4 * 48;
    i_22  =  40 + l_21;
    i_23  =  i_22 + 24L;
    ul_24  =  ul_1 + i_23;
    *((__global int *) ul_24)  =  i_9;
    i_25  =  get_global_size(0);
    i_26  =  i_25 + i_3;
    i_3  =  i_26;
  }
  // BLOCK 3
  return;
}
