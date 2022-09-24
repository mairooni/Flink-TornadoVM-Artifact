#pragma OPENCL EXTENSION cl_khr_fp64 : enable  
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable  
__kernel void reduce(__global uchar *_heap_base, ulong _frame_base, __constant uchar *_constant_region, __local uchar *_local_region, __global int *_atomics)
{
  ulong ul_0, ul_1, ul_2; 
  bool z_4; 
  int i_3; 

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];


  // BLOCK 0
  ul_0  =  (ulong) _frame[3];
  ul_1  =  (ulong) _frame[4];
  ul_2  =  (ulong) _frame[5];
  i_3  =  get_global_id(0);
  z_4  =  i_3 < 64;
  if(z_4)
  {
    // BLOCK 1
    slots[0] = (ulong) -26891;
    return;
  }  // B1
  else
  {
    // BLOCK 2
    return;
  }  // B2
}  //  kernel
