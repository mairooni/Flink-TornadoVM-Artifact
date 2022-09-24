void reduceDouble (ulong input,
                   ulong output,
                   const int fieldOffset,
                   __local double localSums[]) {
               
  size_t idx = get_global_id(0);
  size_t localIdx = get_local_id(0);
  size_t group_size = get_local_size(0);
  size_t groupID = get_group_id(0);
	
  double field = *((__global double *) (fieldOffset + 32 * idx + 24L + input));
  localSums[localIdx] = field; 
  for (size_t stride = group_size / 2; stride > 0; stride /=2) {
      barrier(CLK_LOCAL_MEM_FENCE);
      if (localIdx < stride) {
   	   localSums[localIdx] += localSums[localIdx + stride];
      }
  }
  if (localIdx == 0) {
      ulong ul_3 = fieldOffset + 32 * groupID + 24L + output;
      *((__global double *) ul_3) = localSums[localIdx];
  }	

} 

void reduceLong (ulong input,
                 ulong output,
                 const int fieldOffset,
                 __local long localSums[]) {
               
  size_t idx = get_global_id(0);
  size_t localIdx = get_local_id(0);
  size_t group_size = get_local_size(0);
  size_t groupID = get_group_id(0);
  
  long field = *((__global long *) (fieldOffset + 32 * idx + 24L + input));
  localSums[localIdx] = field; 
  for (size_t stride = group_size / 2; stride > 0; stride /=2) {
      barrier(CLK_LOCAL_MEM_FENCE);
      if (localIdx < stride) {
   	   localSums[localIdx] += localSums[localIdx + stride];
      }
  }
  if (localIdx == 0) {
      ulong ul_3 = fieldOffset + 32 * groupID + 24L + output;
      *((__global long *) ul_3) = localSums[localIdx];
  }
        
}

#pragma OPENCL EXTENSION cl_khr_fp64 : enable  
__kernel void reduce(__global uchar *_heap_base,
                     ulong _frame_base,
                    __constant uchar *_constant_region,
                    __local uchar *_local_region,
                    __global int *_atomics) {

  __global ulong *_frame = (__global ulong *) &_heap_base[_frame_base];
  // Tuple3<Integer, Tuple2<Double, Double>, Long>
  // Frame:
  //          |----------------- 0 --------------|---------------- 1 ---------------| ... 
  // 3: INPUT [integer1, double1, double1, long1, integer2, double2, double2, long2, ...] 
  // 4: OUTPUT [integer1, double1, double1, long1, integer2, double2, double2, long2] 

  // INPUT
  ulong ul_0  =  (ulong) _frame[3];  // input
  ulong ul_1  =  (ulong) _frame[4];  // output

  __local double localSum1[1024];    // local memory for first reduction
  __local double localSum2[1024];    // local memory for second reduction
  __local long localSum3[1024];      // local memory for third reduction
  
  size_t idx = get_global_id(0); 
   
  // copy first field 
  if (idx == 0) {
  	int first = *((__global int *) (24L + ul_0));
  	ulong ul_3 = 24L + ul_1;
  	*((__global int *) ul_3) = first;
  }
  
  // 8: f0 
  reduceDouble(ul_0, ul_1, 8, localSum1);
  // 16: f0 + f1
  reduceDouble(ul_0, ul_1, 16, localSum2);
  // 24: f0 + f1 + f2
  reduceLong(ul_0, ul_1, 24, localSum3);
 
}
