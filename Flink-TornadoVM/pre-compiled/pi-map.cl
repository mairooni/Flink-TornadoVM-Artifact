#pragma OPENCL EXTENSION cl_khr_fp64 : enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  bool z_32;
  double d_29, d_30;
  long l_21, l_20, l_23, l_22, l_19, l_18, l_13, l_12, l_15, l_14, l_8, l_11, l_10, l_5, l_4, l_6, l_24;
  float f_28, f_26, f_27, f_16, f_17, f_31, f_25;
  ulong ul_7, ul_1, ul_0, ul_9;
  int i_3, i_35, i_2, i_34;

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[3];
  ul_1  =  (ulong) _frame[4];

  i_2  =  get_global_id(0);
  // BLOCK 1 MERGES [0 2 ]
  i_3  =  i_2;
  for(;i_3 < 1048576;)
  {
    // BLOCK 2
    l_4  =  (long) i_3;
    l_5  =  l_4 << 3;
    l_6  =  l_5 + 24L;
    ul_7  =  ul_0 + l_6;
    l_8  =  *((__global long *) ul_7);
    ul_9  =  ul_1 + l_6;
    l_10  =  l_8 * 25214903917L;
    l_11  =  l_10 + 11L;
    l_12  =  l_11 & 281474976710655L;
    l_13  =  l_12 * 25214903917L;
    l_14  =  l_13 + 11L;
    l_15  =  l_14 & 268435455L;
    f_16  =  (float) l_15;
    f_17  =  f_16 / 2.68435456E8F;
    l_18  =  l_14 & 281474976710655L;
    l_19  =  l_18 * 25214903917L;
    l_20  =  l_19 + 11L;
    l_21  =  l_20 & 281474976710655L;
    l_22  =  l_21 * 25214903917L;
    l_23  =  l_22 + 11L;
    l_24  =  l_23 & 268435455L;
    f_25  =  (float) l_24;
    f_26  =  f_25 / 2.68435456E8F;
    f_27  =  f_26 * f_26;
    f_28  =  fma(f_17, f_17, f_27);
    d_29  =  (double) f_28;
    d_30  =  native_sqrt(d_29);
    f_31  =  (float) d_30;
    z_32  =  isless(f_31, 1.0F) || isequal(f_31, 1.0F);
    long l_33 = z_32 ? 1L : 0L;
    *((__global long *) ul_9)  =  l_33;
    i_34  =  get_global_size(0);
    i_35  =  i_34 + i_3;
    i_3  =  i_35;
  }  // B2
  // BLOCK 3
  return;
}  //  kernel
