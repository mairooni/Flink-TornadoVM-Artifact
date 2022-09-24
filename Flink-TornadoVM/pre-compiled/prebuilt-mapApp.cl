#pragma OPENCL EXTENSION cl_khr_fp64 : enable  
__kernel void mapApp(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global uchar *_private_region)
{
  ulong ul_28, ul_58, ul_0, ul_1, ul_36, ul_5, ul_2, ul_8, ul_6, ul_7, ul_44, ul_14, ul_21, ul_51;
  double d_29, d_37;
  long l_26, l_25, l_24, l_56, l_55, l_54, l_19, l_34, l_33, l_32, l_10, l_42, l_41, l_40, l_18, l_17, l_49, l_48, l_47, l_12, l_11;
  int i_30, i_27, i_59, i_60, i_31, i_53, i_22, i_20, i_52, i_57, i_23, i_13, i_45, i_46, i_43, i_50, i_15, i_16, i_38, i_3, i_35, i_4, i_9, i_39;
  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];
  // BLOCK 0
  ul_0  =  (ulong) _frame[6];
  ul_1  =  (ulong) _frame[7];
  ul_2  =  (ulong) _frame[8];
  i_3  =  get_global_id(0);
  // BLOCK 1 MERGES [0 2 ]
  i_4  =  i_3;
  for(;i_4 < 256;)  {
    // BLOCK 2
    ul_5  =  ul_0 + 16L;
    ul_6  =  *((__global ulong *) ul_5);
    ul_7  =  ul_6 + 16L;
    ul_8  =  *((__global ulong *) ul_7);
    i_9  =  i_4 * 16;
    l_10  =  (long) i_4;
    l_11  =  l_10 << 2;
    l_12  =  l_11 + 24L;
    i_13  =  i_9 + l_12;
    ul_14  =  ul_1 + i_13;
    i_15  =  *((__global int *) ul_14);
    i_16  =  i_4 * 24;
    l_17  =  (long) i_4;
    l_18  =  l_17 << 2;
    l_19  =  l_18 + 24L;
    i_20  =  i_16 + l_19;
    ul_21  =  ul_2 + i_20;
    *((__global int *) ul_21)  =  i_15;
    i_22  =  i_4 * 12;
    i_23  =  i_22 + 12;
    l_24  =  (long) i_4;
    l_25  =  l_24 << 3;
    l_26  =  l_25 + 24L;
    i_27  =  i_23 + l_26;
    ul_28  =  ul_1 + i_27;
    d_29  =  *((__global double *) ul_28);
    i_30  =  i_4 * 12;
    i_31  =  4 + i_30;
    l_32  =  (long) i_4;
    l_33  =  l_32 << 3;
    l_34  =  l_33 + 24L;
    i_35  =  i_31 + l_34;
    ul_36  =  ul_1 + i_35;
    d_37  =  *((__global double *) ul_36);
    i_38  =  i_4 * 20;
    i_39  =  4 + i_38;
    l_40  =  (long) i_4;
    l_41  =  l_40 << 3;
    l_42  =  l_41 + 24L;
    i_43  =  i_39 + l_42;
    ul_44  =  ul_2 + i_43;
    *((__global double *) ul_44)  =  d_37;
    i_45  =  i_4 * 20;
    i_46  =  12 + i_45;
    l_47  =  (long) i_4;
    l_48  =  l_47 << 3;
    l_49  =  l_48 + 24L;
    i_50  =  i_46 + l_49;
    ul_51  =  ul_2 + i_50;
    *((__global double *) ul_51)  =  d_29;
    i_52  =  i_4 * 20;
    i_53  =  i_52 + 20;
    l_54  =  (long) i_4;
    l_55  =  l_54 << 3;
    l_56  =  l_55 + 24L;
    i_57  =  i_53 + l_56;
    ul_58  =  ul_2 + i_57;
    *((__global long *) ul_58)  =  1L;
    i_59  =  get_global_size(0);
    i_60  =  i_59 + i_4;
    i_4  =  i_60;
  }
  // BLOCK 3
  return;
}
