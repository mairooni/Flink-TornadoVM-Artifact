#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void reduce(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  double d_44, d_76, d_43, d_47, d_79, d_46, d_78, d_34, d_61, d_27, d_33, d_62, d_84, d_54; 
  long l_31, l_35, l_45, l_48, l_49, l_55, l_57, l_58, l_59, l_64, l_65, l_66, l_9, l_10, l_11, l_13, l_14, l_15, l_16, l_18, l_86, l_23, l_87, l_24, l_88, l_25, l_29, l_30; 
  int i_63, i_56, i_52, i_51, i_50, i_40, i_42, i_37, i_36, i_39, i_38, i_28, i_90, i_21, i_85, i_20, i_22, i_81, i_80, i_82, i_77, i_73, i_8, i_72, i_74, i_5, i_69, i_68, i_7, i_71, i_6, i_70; 
  bool z_83, z_53, z_41, z_75; 
  ulong ul_26, ul_89, ul_12, ul_60, ul_0, ul_32, ul_1, ul_17, ul_19, ul_67; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[4];
  ul_1  =  (ulong) _frame[5];
  __local double d_2[16];
  __local long l_3[16];
  __local double d_4[16];
  i_5  =  get_global_id(0);
  // BLOCK 1 MERGES [0 12 ]
  i_6  =  i_5;
  for(;i_6 < 16;)
  {
    // BLOCK 2
    i_7  =  i_6 << 2;
    i_8  =  i_7 + 2;
    l_9  =  (long) i_8;
    l_10  =  l_9 << 3;
    l_11  =  l_10 + 24L;
    ul_12  =  ul_0 + l_11;
    l_13  =  *((__global long *) ul_12);
    l_14  =  (long) i_7;
    l_15  =  l_14 << 3;
    l_16  =  l_15 + 24L;
    ul_17  =  ul_0 + l_16;
    l_18  =  *((__global long *) ul_17);
    ul_19  =  ul_1 + l_16;
    *((__global long *) ul_19)  =  l_18;
    i_20  =  get_local_id(0);
    i_21  =  get_local_size(0);
    i_22  =  i_7 + 3;
    l_23  =  (long) i_22;
    l_24  =  l_23 << 3;
    l_25  =  l_24 + 24L;
    ul_26  =  ul_0 + l_25;
    d_27  =  *((__global double *) ul_26);
    i_28  =  i_7 + 1;
    l_29  =  (long) i_28;
    l_30  =  l_29 << 3;
    l_31  =  l_30 + 24L;
    ul_32  =  ul_0 + l_31;
    d_33  =  *((__global double *) ul_32);
    d_2[i_20]  =  d_33;
    d_34  =  *((__global double *) ul_26);
    l_35  =  isnan(d_34)? 0 : (int) d_34;
    l_3[i_20]  =  l_35;
    barrier(CLK_LOCAL_MEM_FENCE);
    i_36  =  i_21 >> 31;
    i_37  =  i_36 + i_21;
    i_38  =  i_37 >> 1;
    // BLOCK 3 MERGES [2 20 ]
    i_39  =  i_38;
    for(;i_39 >= 1;)
    {
      // BLOCK 17
      barrier(CLK_LOCAL_MEM_FENCE);
      i_40  =  i_39 >> 1;
      z_41  =  i_20 < i_39;
      if(z_41)
      {
        // BLOCK 18
        i_42  =  i_39 + i_20;
        d_43  =  d_2[i_42];
        d_44  =  d_2[i_20];
        l_45  =  l_3[i_42];
        d_46  =  (double) l_45;
        d_47  =  fma(d_46, d_43, d_44);
        d_2[i_20]  =  d_47;
        d_44  =  d_47;
        l_48  =  l_3[i_20];
        l_49  =  l_48 + l_48;
        l_3[i_20]  =  l_49;
        l_48  =  l_49;
      }  // B18
      else
      {
        // BLOCK 19
      }  // B19
      // BLOCK 20 MERGES [19 18 ]
      i_50  =  i_40;
      i_39  =  i_50;
    }  // B20
    // BLOCK 4
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_51  =  get_group_id(0);
    i_52  =  i_51 << 2;
    z_53  =  i_20 == 0;
    if(z_53)
    {
      // BLOCK 5
      d_54  =  d_2[i_20];
      l_55  =  l_3[i_20];
      i_56  =  i_52 + 1;
      l_57  =  (long) i_56;
      l_58  =  l_57 << 3;
      l_59  =  l_58 + 24L;
      ul_60  =  ul_1 + l_59;
      d_61  =  (double) l_55;
      d_62  =  d_54 / d_61;
      *((__global double *) ul_60)  =  d_62;
    }  // B5
    else
    {
      // BLOCK 6
    }  // B6
    // BLOCK 7 MERGES [6 5 ]
    i_63  =  i_7 + 2;
    l_64  =  (long) i_63;
    l_65  =  l_64 << 3;
    l_66  =  l_65 + 24L;
    ul_67  =  ul_1 + l_66;
    *((__global long *) ul_67)  =  l_13;
    i_68  =  get_local_id(0);
    i_69  =  get_local_size(0);
    d_4[i_68]  =  d_27;
    i_70  =  i_69 >> 31;
    i_71  =  i_70 + i_69;
    i_72  =  i_71 >> 1;
    // BLOCK 8 MERGES [7 16 ]
    i_73  =  i_72;
    for(;i_73 >= 1;)
    {
      // BLOCK 13
      barrier(CLK_LOCAL_MEM_FENCE);
      i_74  =  i_73 >> 1;
      z_75  =  i_68 < i_73;
      if(z_75)
      {
        // BLOCK 14
        d_76  =  d_4[i_68];
        i_77  =  i_73 + i_68;
        d_78  =  d_4[i_77];
        d_79  =  d_76 + d_78;
        d_4[i_68]  =  d_79;
        d_76  =  d_79;
      }  // B14
      else
      {
        // BLOCK 15
      }  // B15
      // BLOCK 16 MERGES [15 14 ]
      i_80  =  i_74;
      i_73  =  i_80;
    }  // B16
    // BLOCK 9
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_81  =  get_global_size(0);
    i_82  =  i_81 + i_6;
    z_83  =  i_68 == 0;
    if(z_83)
    {
      // BLOCK 10
      d_84  =  d_4[i_68];
      i_85  =  i_52 + 3;
      l_86  =  (long) i_85;
      l_87  =  l_86 << 3;
      l_88  =  l_87 + 24L;
      ul_89  =  ul_1 + l_88;
      *((__global double *) ul_89)  =  d_84;
    }  // B10
    else
    {
      // BLOCK 11
    }  // B11
    // BLOCK 12 MERGES [11 10 ]
    i_90  =  i_82;
    i_6  =  i_90;
  }  // B12
  // BLOCK 21
  return;
}  //  kernel
