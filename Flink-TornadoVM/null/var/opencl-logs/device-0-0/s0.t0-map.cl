#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  long l_30, l_71, l_24, l_72, l_6, l_38, l_11, l_75, l_9, l_73; 
  ulong ul_20, ul_57, ul_58, ul_27, ul_0, ul_1, ul_2, ul_35, ul_41, ul_74, ul_14, ul_78, ul_47; 
  int i_70, i_8, i_3, i_5, i_4, i_46, i_43, i_45, i_44, i_56, i_31, i_33, i_32, i_26, i_29, i_39, i_40, i_34, i_37, i_79, i_17, i_16, i_80, i_10, i_13, i_77, i_12, i_76, i_23, i_25, i_19, i_18; 
  double d_51, d_52, d_49, d_50, d_55, d_21, d_53, d_22, d_54, d_28, d_36, d_7, d_42, d_15, d_48; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[4];
  ul_1  =  (ulong) _frame[5];
  ul_2  =  (ulong) _frame[6];
  i_3  =  get_global_id(0);
  i_4  =  _frame[0];
  // BLOCK 1 MERGES [0 8 ]
  i_5  =  i_3;
  for(;i_5 < i_4;)
  {
    // BLOCK 2
    l_6  =  (long) i_5;
    // BLOCK 3 MERGES [2 4 ]
    d_7  =  0.0;
    i_8  =  0;
    for(;i_8 < 83;)
    {
      // BLOCK 4
      l_9  =  (long) i_8;
      i_10  =  8 * l_9;
      l_11  =  l_6 * 672;
      i_12  =  i_10 + l_11;
      i_13  =  i_12 + 24L;
      ul_14  =  ul_0 + i_13;
      d_15  =  *((__global double *) ul_14);
      i_16  =  8 * l_9;
      i_17  =  0 * 672;
      i_18  =  i_16 + i_17;
      i_19  =  i_18 + 24L;
      ul_20  =  ul_1 + i_19;
      d_21  =  *((__global double *) ul_20);
      d_22  =  fma(d_15, d_21, d_7);
      i_23  =  i_8 + 1;
      d_7  =  d_22;
      i_8  =  i_23;
    }  // B4
    // BLOCK 5
    l_24  =  l_6 * 672;
    i_25  =  664 + l_24;
    i_26  =  i_25 + 24L;
    ul_27  =  ul_0 + i_26;
    d_28  =  *((__global double *) ul_27);
    // BLOCK 6 MERGES [5 7 ]
    i_29  =  0;
    for(;i_29 < 83;)
    {
      // BLOCK 7
      l_30  =  (long) i_29;
      i_31  =  8 * l_30;
      i_32  =  0 * 672;
      i_33  =  i_31 + i_32;
      i_34  =  i_33 + 24L;
      ul_35  =  ul_1 + i_34;
      d_36  =  *((__global double *) ul_35);
      i_37  =  8 * l_30;
      l_38  =  l_6 * 672;
      i_39  =  i_37 + l_38;
      i_40  =  i_39 + 24L;
      ul_41  =  ul_0 + i_40;
      d_42  =  *((__global double *) ul_41);
      i_43  =  8 * l_30;
      i_44  =  672 * l_6;
      i_45  =  i_43 + i_44;
      i_46  =  i_45 + 24L;
      ul_47  =  ul_0 + i_46;
      d_48  =  -d_7;
      d_49  =  exp(d_48);
      d_50  =  d_49 + 1.0;
      d_51  =  1.0 / d_50;
      d_52  =  d_51 - d_28;
      d_53  =  d_52 * d_42;
      d_54  =  d_53 * 0.1;
      d_55  =  d_36 - d_54;
      *((__global double *) ul_47)  =  d_55;
      i_56  =  i_29 + 1;
      i_29  =  i_56;
    }  // B7
    // BLOCK 8
    ul_57  =  ul_0 + 24L;
    ul_58  =  ul_2 + 24L;
    i_70  =  i_5;
    int i_69, i_59, i_60, i_61, i_65, i_66, i_67;
    double d_64;
    ulong ul_63, ul_68;
    i_69 = 0;
    for(;i_69 < 83;)  {
      i_59 = 8 * i_69;
      i_60 = 672 * i_70;
      i_61 = i_59 + i_60;
      ul_63 = ul_57 + i_61;
      d_64 = *((__global double *) ul_63); 
      i_65 = 8 * i_69;
      i_66 = 680 * i_70;
      i_67 = i_65 + i_66;
      ul_68 = ul_58 + i_67;
      *((__global double *) ul_68)  = d_64;
      i_69 = i_69 + 1;
    }
    l_71  =  l_6 * 680;
    l_72  =  l_71 + 664;
    l_73  =  l_72 + 24L;
    ul_74  =  ul_2 + l_73;
    *((__global int *) ul_74)  =  82;
    l_75  =  l_6 * 680;
    i_76  =  672 + l_75;
    i_77  =  i_76 + 24L;
    ul_78  =  ul_2 + i_77;
    *((__global int *) ul_78)  =  1;
    i_79  =  get_global_size(0);
    i_80  =  i_79 + i_5;
    i_5  =  i_80;
  }  // B8
  // BLOCK 9
  return;
}  //  kernel
