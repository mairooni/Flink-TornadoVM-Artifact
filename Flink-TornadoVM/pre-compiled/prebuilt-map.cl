__kernel void map(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global uchar *_private_region)
{
  ulong ul_20, ul_25, ul_15, ul_0, ul_1; 
  int i_4, i_5, i_6, i_7, i_16, i_2, i_3, i_28, i_8, i_9, i_10, i_11; 
  double d_21, d_27, d_26; 
  long l_24, l_23, l_22, l_12, l_14, l_13, l_19, l_18, l_17; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[6];
  ul_1  =  (ulong) _frame[7];
  printf("=== ul_0: %lu \n", ul_0);
  printf("=== ul_1: %lu \n", ul_1);
  i_2  =  get_global_size(0);
  printf("=== i2: %d \n", i_2);
  i_3  =  i_2 + 9;
  printf("=== i3: %d \n", i_3);
  i_4  =  i_3 / i_2;
  printf("=== i4: %d \n", i_4);
  i_5  =  get_global_id(0);
  printf("=== i5: %d \n", i_5);
  i_6  =  i_4 * i_5;
  printf("=== i6: %d \n", i_6);
  i_7  =  i_6 + i_4;
  printf("=== i7: %d \n", i_7);
  i_8  =  min(i_7, 10);
  printf("=== i8: %d \n", i_8);
  // BLOCK 1 MERGES [0 2 ]
  i_9  =  i_6;
  printf("=== i9: %d \n", i_9);
  for(;i_9 < i_8;)  {
    // BLOCK 2
    i_10  =  i_9 << 1;
    i_11  =  i_10 + 1;
    l_12  =  (long) i_11;
    l_13  =  l_12 << 3;
    l_14  =  l_13 + 24L;
    printf("--- offset for int: %ld\n", l_14);
    ul_15  =  ul_0 + l_14;
    printf("--- read int from memory address %lu \n", ul_15);
    i_16  =  *((__global int *) ul_15);
    printf("--- int: %d \n", i_16);
    l_17  =  (long) i_10;
    l_18  =  l_17 << 3;
    l_19  =  l_18 + 24L;
    printf("--- offset for double: %ld\n", l_19);
    ul_20  =  ul_0 + l_19;
    printf("--- read double from memory address %lu \n", ul_20);
    d_21  =  *((__global double *) ul_20);
    printf("--- double: %f \n", d_21);
    l_22  =  (long) i_9;
    l_23  =  l_22 << 3;
    l_24  =  l_23 + 24L;
    printf("+++ offset for double: %ld\n", l_24);
    ul_25  =  ul_1 + l_24;
	printf("+++ write double at address %lu \n", ul_25);
    d_26  =  (double) i_16;
    d_27  =  d_26 + d_21;
    printf("+++ double: %f\n", d_27);
    *((__global double *) ul_25)  =  d_27;
    i_28  =  i_9 + 1;
    i_9  =  i_28;
  }
  // BLOCK 3
  return;
}
