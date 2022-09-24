#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  long l_7, l_8, l_40, l_9, l_41, l_42, l_16, l_17, l_18, l_29, l_30, l_23, l_24, l_25, l_35, l_36, l_37, l_31; 
  int i_33, i_3, i_2, i_34, i_5, i_4, i_39, i_6, i_13, i_45, i_44, i_15, i_14, i_22, i_27, i_28; 
  double d_20, d_12, d_11; 
  ulong ul_0, ul_32, ul_1, ul_43, ul_10, ul_26, ul_19, ul_21, ul_38; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[4];
  ul_1  =  (ulong) _frame[5];
  i_2  =  get_global_id(0);
  // BLOCK 1 MERGES [0 5 ]
  i_3  =  i_2;
  for(;i_3 < 64;)
  {
    // BLOCK 2
    // BLOCK 3 MERGES [2 4 ]
    i_4  =  0;
    for(;i_4 < 83;)
    {
      // BLOCK 4
      i_5  =  i_4 << 1;
      i_6  =  i_5 + i_4;
      l_7  =  (long) i_6;
      l_8  =  l_7 << 3;
      l_9  =  l_8 + 24L;
      ul_10  =  ul_0 + l_9;
      d_11  =  *((__global double *) ul_10);
      d_12  =  d_11 + d_11;
      *((__global double *) ul_10)  =  d_12;
      d_11  =  d_12;
      i_13  =  i_4 + 1;
      i_4  =  i_13;
    }  // B4
    // BLOCK 5
    i_14  =  i_3 << 1;
    i_15  =  i_14 + i_3;
    l_16  =  (long) i_15;
    l_17  =  l_16 << 3;
    l_18  =  l_17 + 24L;
    ul_19  =  ul_0 + l_18;
    d_20  =  *((__global double *) ul_19);
    ul_21  =  ul_1 + l_18;
    *((__global double *) ul_21)  =  d_20;
    i_22  =  i_15 + 1;
    l_23  =  (long) i_22;
    l_24  =  l_23 << 2;
    l_25  =  l_24 + 24L;
    ul_26  =  ul_0 + l_25;
    i_27  =  *((__global int *) ul_26);
    i_28  =  i_15 + 2;
    l_29  =  (long) i_28;
    l_30  =  l_29 << 2;
    l_31  =  l_30 + 24L;
    ul_32  =  ul_0 + l_31;
    i_33  =  *((__global int *) ul_32);
    i_34  =  i_15 + 1;
    l_35  =  (long) i_34;
    l_36  =  l_35 << 2;
    l_37  =  l_36 + 24L;
    ul_38  =  ul_1 + l_37;
    *((__global int *) ul_38)  =  i_27;
    i_39  =  i_15 + 2;
    l_40  =  (long) i_39;
    l_41  =  l_40 << 2;
    l_42  =  l_41 + 24L;
    ul_43  =  ul_1 + l_42;
    *((__global int *) ul_43)  =  i_33;
    i_44  =  get_global_size(0);
    i_45  =  i_44 + i_3;
    i_3  =  i_45;
  }  // B5
  // BLOCK 6
  return;
}  //  kernel
