#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void reduce(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  ulong ul_54, ul_0, ul_18, ul_1, ul_12, ul_30, ul_24, ul_10; 
  double d_11; 
  bool z_38, z_46; 
  long l_9, l_8, l_7, l_29, l_28, l_27, l_23, l_22, l_21, l_53, l_52, l_51, l_17, l_16, l_15; 
  int i_6, i_5, i_4, i_3, i_55, i_45, i_44, i_43, i_50, i_49, i_48, i_47, i_37, i_36, i_35, i_42, i_41, i_40, i_39, i_34, i_33, i_32, i_31, i_20, i_19, i_26, i_25, i_14, i_13; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[4];
  ul_1  =  (ulong) _frame[5];
  __local int i_2[64];
  i_3  =  get_global_id(0);
  // BLOCK 1 MERGES [0 7 ]
  i_4  =  i_3;
  for(;i_4 < 64;)
  {
    // BLOCK 2
    i_5  =  i_4 << 1;
    i_6  =  i_5 + i_4;
    l_7  =  (long) i_6;
    l_8  =  l_7 << 3;
    l_9  =  l_8 + 24L;
    ul_10  =  ul_0 + l_9;
    d_11  =  *((__global double *) ul_10);
    ul_12  =  ul_1 + l_9;
    *((__global double *) ul_12)  =  d_11;
    i_13  =  i_4 * 85;
    i_14  =  i_13 + 84;
    l_15  =  (long) i_14;
    l_16  =  l_15 << 2;
    l_17  =  l_16 + 24L;
    ul_18  =  ul_0 + l_17;
    i_19  =  *((__global int *) ul_18);
    i_20  =  i_6 + 1;
    l_21  =  (long) i_20;
    l_22  =  l_21 << 2;
    l_23  =  l_22 + 24L;
    ul_24  =  ul_0 + l_23;
    i_25  =  *((__global int *) ul_24);
    i_26  =  i_6 + 1;
    l_27  =  (long) i_26;
    l_28  =  l_27 << 2;
    l_29  =  l_28 + 24L;
    ul_30  =  ul_1 + l_29;
    *((__global int *) ul_30)  =  i_25;
    i_31  =  get_local_id(0);
    i_32  =  get_local_size(0);
    i_2[i_31]  =  i_19;
    i_33  =  i_32 >> 31;
    i_34  =  i_33 + i_32;
    i_35  =  i_34 >> 1;
    // BLOCK 3 MERGES [2 11 ]
    i_36  =  i_35;
    for(;i_36 >= 1;)
    {
      // BLOCK 8
      barrier(CLK_LOCAL_MEM_FENCE);
      i_37  =  i_36 >> 1;
      z_38  =  i_31 < i_36;
      if(z_38)
      {
        // BLOCK 9
        i_39  =  i_2[i_31];
        i_40  =  i_36 + i_31;
        i_41  =  i_2[i_40];
        i_42  =  i_39 + i_41;
        i_2[i_31]  =  i_42;
        i_39  =  i_42;
      }  // B9
      else
      {
        // BLOCK 10
      }  // B10
      // BLOCK 11 MERGES [10 9 ]
      i_43  =  i_37;
      i_36  =  i_43;
    }  // B11
    // BLOCK 4
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_44  =  get_global_size(0);
    i_45  =  i_44 + i_4;
    z_46  =  i_31 == 0;
    if(z_46)
    {
      // BLOCK 5
      i_47  =  i_2[i_31];
      i_48  =  get_group_id(0);
      i_49  =  i_48 * 85;
      i_50  =  i_49 + 84;
      l_51  =  (long) i_50;
      l_52  =  l_51 << 2;
      l_53  =  l_52 + 24L;
      ul_54  =  ul_1 + l_53;
      *((__global int *) ul_54)  =  i_47;
    }  // B5
    else
    {
      // BLOCK 6
    }  // B6
    // BLOCK 7 MERGES [6 5 ]
    i_55  =  i_45;
    i_4  =  i_55;
  }  // B7
  // BLOCK 12
  return;
}  //  kernel
