#pragma OPENCL EXTENSION cl_khr_fp64 : enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
__kernel void reduce(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  bool z_17, z_25;
  long l_21, l_20, l_7, l_18, l_29, l_31, l_30, l_9, l_8, l_11, l_26;
  ulong ul_1, ul_0, ul_32, ul_10;
  int i_27, i_28, i_23, i_24, i_22, i_3, i_4, i_33, i_12, i_5, i_6, i_19, i_15, i_16, i_13, i_14;

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[3];
  ul_1  =  (ulong) _frame[4];

  __local long l_2[1024];
  i_3  =  get_global_id(0);
  // BLOCK 1 MERGES [0 7 ]
  i_4  =  i_3;
  for(;i_4 < size;)
  {
    // BLOCK 2
    i_5  =  get_local_id(0);
    i_6  =  get_local_size(0);
    l_7  =  (long) i_4;
    l_8  =  l_7 << 3;
    l_9  =  l_8 + 24L;
    ul_10  =  ul_0 + l_9;
    l_11  =  *((__global long *) ul_10);
    l_2[i_5]  =  l_11;
    i_12  =  i_6 >> 31;
    i_13  =  i_12 + i_6;
    i_14  =  i_13 >> 1;
    // BLOCK 3 MERGES [2 11 ]
    i_15  =  i_14;
    for(;i_15 >= 1;)
    {
      // BLOCK 8
      barrier(CLK_LOCAL_MEM_FENCE);
      i_16  =  i_15 >> 1;
      z_17  =  i_5 < i_15;
      if(z_17)
      {
        // BLOCK 9
        l_18  =  l_2[i_5];
        i_19  =  i_15 + i_5;
        l_20  =  l_2[i_19];
        l_21  =  l_18 + l_20;
        l_2[i_5]  =  l_21;
        l_18  =  l_21;
      }  // B9
      else
      {
        // BLOCK 10
      }  // B10
      // BLOCK 11 MERGES [10 9 ]
      i_22  =  i_16;
      i_15  =  i_22;
    }  // B11
    // BLOCK 4
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_23  =  get_global_size(0);
    i_24  =  i_23 + i_4;
    z_25  =  i_5 == 0;
    if(z_25)
    {
      // BLOCK 5
      l_26  =  l_2[0];
      i_27  =  get_group_id(0);
      i_28  =  i_27 + 1;
      l_29  =  (long) i_28;
      l_30  =  l_29 << 3;
      l_31  =  l_30 + 24L;
      ul_32  =  ul_1 + l_31;
      *((__global long *) ul_32)  =  l_26;
    }  // B5
    else
    {
      // BLOCK 6
    }  // B6
    // BLOCK 7 MERGES [6 5 ]
    i_33  =  i_24;
    i_4  =  i_33;
  }  // B7
  // BLOCK 12
  return;
}  //  kernel
