#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void reduce(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  long l_65, l_64, l_66, l_45, l_44, l_46, l_93, l_92, l_94, l_85, l_84, l_23, l_25, l_24, l_90, l_17, l_19, l_18, l_82, l_71, l_9, l_73, l_8, l_72, l_75, l_10; 
  double d_36, d_21, d_37, d_34, d_62, d_42, d_58, d_27, d_59, d_56; 
  int i_49, i_50, i_48, i_43, i_39, i_40, i_38, i_35, i_31, i_32, i_96, i_29, i_30, i_91, i_28, i_87, i_88, i_22, i_86, i_83, i_15, i_79, i_16, i_80, i_77, i_14, i_78, i_12, i_76, i_7, i_5, i_69, i_6, i_70, i_68, i_63, i_60, i_57, i_53, i_54, i_51, i_52; 
  ulong ul_26, ul_74, ul_11, ul_13, ul_67, ul_20, ul_47, ul_95, ul_0, ul_1; 
  bool z_61, z_41, z_89, z_55, z_33, z_81; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[4];
  ul_1  =  (ulong) _frame[5];
  __local double d_2[128];
  __local double d_3[128];
  __local long l_4[128];
  i_5  =  get_global_id(0);
  // BLOCK 1 MERGES [0 17 ]
  i_6  =  i_5;
  for(;i_6 < 128;)
  {
    // BLOCK 2
    i_7  =  i_6 << 2;
    l_8  =  (long) i_7;
    l_9  =  l_8 << 2;
    l_10  =  l_9 + 24L;
    ul_11  =  ul_0 + l_10;
    i_12  =  *((__global int *) ul_11);
    ul_13  =  ul_1 + l_10;
    *((__global int *) ul_13)  =  i_12;
    i_14  =  get_local_id(0);
    i_15  =  get_local_size(0);
    i_16  =  i_7 + 2;
    l_17  =  (long) i_16;
    l_18  =  l_17 << 3;
    l_19  =  l_18 + 24L;
    ul_20  =  ul_0 + l_19;
    d_21  =  *((__global double *) ul_20);
    i_22  =  i_7 + 1;
    l_23  =  (long) i_22;
    l_24  =  l_23 << 3;
    l_25  =  l_24 + 24L;
    ul_26  =  ul_0 + l_25;
    d_27  =  *((__global double *) ul_26);
    d_2[i_14]  =  d_27;
    i_28  =  i_15 >> 31;
    i_29  =  i_28 + i_15;
    i_30  =  i_29 >> 1;
    // BLOCK 3 MERGES [2 29 ]
    i_31  =  i_30;
    for(;i_31 >= 1;)
    {
      // BLOCK 26
      barrier(CLK_LOCAL_MEM_FENCE);
      i_32  =  i_31 >> 1;
      z_33  =  i_14 < i_31;
      if(z_33)
      {
        // BLOCK 27
        d_34  =  d_2[i_14];
        i_35  =  i_31 + i_14;
        d_36  =  d_2[i_35];
        d_37  =  d_34 + d_36;
        d_2[i_14]  =  d_37;
        d_34  =  d_37;
      }  // B27
      else
      {
        // BLOCK 28
      }  // B28
      // BLOCK 29 MERGES [28 27 ]
      i_38  =  i_32;
      i_31  =  i_38;
    }  // B29
    // BLOCK 4
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_39  =  get_group_id(0);
    i_40  =  i_39 << 2;
    z_41  =  i_14 == 0;
    if(z_41)
    {
      // BLOCK 5
      d_42  =  d_2[i_14];
      i_43  =  i_40 + 1;
      l_44  =  (long) i_43;
      l_45  =  l_44 << 3;
      l_46  =  l_45 + 24L;
      ul_47  =  ul_1 + l_46;
      *((__global double *) ul_47)  =  d_42;
    }  // B5
    else
    {
      // BLOCK 6
    }  // B6
    // BLOCK 7 MERGES [6 5 ]
    i_48  =  get_local_id(0);
    i_49  =  get_local_size(0);
    d_3[i_48]  =  d_21;
    i_50  =  i_49 >> 31;
    i_51  =  i_50 + i_49;
    i_52  =  i_51 >> 1;
    // BLOCK 8 MERGES [7 25 ]
    i_53  =  i_52;
    for(;i_53 >= 1;)
    {
      // BLOCK 22
      barrier(CLK_LOCAL_MEM_FENCE);
      i_54  =  i_53 >> 1;
      z_55  =  i_48 < i_53;
      if(z_55)
      {
        // BLOCK 23
        d_56  =  d_3[i_48];
        i_57  =  i_53 + i_48;
        d_58  =  d_3[i_57];
        d_59  =  d_56 + d_58;
        d_3[i_48]  =  d_59;
        d_56  =  d_59;
      }  // B23
      else
      {
        // BLOCK 24
      }  // B24
      // BLOCK 25 MERGES [24 23 ]
      i_60  =  i_54;
      i_53  =  i_60;
    }  // B25
    // BLOCK 9
    barrier(CLK_GLOBAL_MEM_FENCE);
    z_61  =  i_48 == 0;
    if(z_61)
    {
      // BLOCK 10
      d_62  =  d_3[i_48];
      i_63  =  i_40 + 2;
      l_64  =  (long) i_63;
      l_65  =  l_64 << 3;
      l_66  =  l_65 + 24L;
      ul_67  =  ul_1 + l_66;
      *((__global double *) ul_67)  =  d_62;
    }  // B10
    else
    {
      // BLOCK 11
    }  // B11
    // BLOCK 12 MERGES [11 10 ]
    i_68  =  get_local_id(0);
    i_69  =  get_local_size(0);
    i_70  =  i_7 + 3;
    l_71  =  (long) i_70;
    l_72  =  l_71 << 3;
    l_73  =  l_72 + 24L;
    ul_74  =  ul_0 + l_73;
    l_75  =  *((__global long *) ul_74);
    l_4[i_68]  =  l_75;
    i_76  =  i_69 >> 31;
    i_77  =  i_76 + i_69;
    i_78  =  i_77 >> 1;
    // BLOCK 13 MERGES [12 21 ]
    i_79  =  i_78;
    for(;i_79 >= 1;)
    {
      // BLOCK 18
      barrier(CLK_LOCAL_MEM_FENCE);
      i_80  =  i_79 >> 1;
      z_81  =  i_68 < i_79;
      if(z_81)
      {
        // BLOCK 19
        l_82  =  l_4[i_68];
        i_83  =  i_79 + i_68;
        l_84  =  l_4[i_83];
        l_85  =  l_82 + l_84;
        l_4[i_68]  =  l_85;
        l_82  =  l_85;
      }  // B19
      else
      {
        // BLOCK 20
      }  // B20
      // BLOCK 21 MERGES [20 19 ]
      i_86  =  i_80;
      i_79  =  i_86;
    }  // B21
    // BLOCK 14
    barrier(CLK_GLOBAL_MEM_FENCE);
    i_87  =  get_global_size(0);
    i_88  =  i_87 + i_6;
    z_89  =  i_68 == 0;
    if(z_89)
    {
      // BLOCK 15
      l_90  =  l_4[i_68];
      i_91  =  i_40 + 3;
      l_92  =  (long) i_91;
      l_93  =  l_92 << 3;
      l_94  =  l_93 + 24L;
      ul_95  =  ul_1 + l_94;
      *((__global long *) ul_95)  =  l_90;
    }  // B15
    else
    {
      // BLOCK 16
    }  // B16
    // BLOCK 17 MERGES [16 15 ]
    i_96  =  i_88;
    i_6  =  i_96;
  }  // B17
  // BLOCK 30
  return;
}  //  kernel
