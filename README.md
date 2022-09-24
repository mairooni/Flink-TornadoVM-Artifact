# Experimental Evaluation: Instructions
This README file contains information about how to perform the experimental evaluation presented in the paper titled *Enabling Transparent Acceleration of Big Data Frameworks Using Heterogeneous Hardware*.
It consists of the following sections:
1. Section **Compilation**, which provides instructions on how to compile TornadoVM and the Flink-TornadoVM integration.
2. Section **Setup**, which describes all the necessary steps that should be performed before execution.
3. Section **Benchmark Configuration**, which describes the different cluster configurations that were tested. 
4. Section **Scripts and Datasets**, which provides information about i) the scripts that should be used to run the experiments, ii) the datasets tested, iii) any additional flags required and, iv) how to run the operators of each computation on the hardware device (CPU,GPU,FPGA).  
5. Section **Evaluation**, which presents the class files used to collect the evaluation numbers. 

## Compilation

1. Compile TornadoVM:
    - Use the instructions in the file `Install_TornadoVM.md` to compile TornadoVM, in the `Flink-TornadoVM-Artifact/TornadoVM` directory.
    - The JDK used for all the experiments is JDK 1.8.0_302 which can be downloaded using this link: [https://github.com/graalvm/graal-jvmci-8/releases/tag/jvmci-21.2-b08](https://github.com/graalvm/graal-jvmci-8/releases/tag/jvmci-21.2-b08)
    - Make sure to run `source ./etc/sources.env` to load the necessary enviromental variables.
    
2. Compile Flink-TornadoVM: 

```bash
$ cd Flink-TornadoVM/
$ ./buildFlinkTornado.sh
```
## Setup 

First export the `TORNADO_ROOT` variable to your TornadoVM installation directory. For example:

```bash
export TORNADO_ROOT=path/to/Flink-TornadoVM-Artifact/TornadoVM
```

Then run the `flink-management` script, located in the `scripts`, directory with the argument `deploy-tornado`.

```bash
$ ./scripts/flink-management deploy-tornado
```

This script deploys your TornadoVM installation into the Flink distributed environment.

### Identify the hardware accelerators 

Run `tornado --devices` to identify the available devices for execution.
E.g.: 

```bash
$ tornado --devices

Number of Tornado drivers: 1
Driver: OpenCL
  Total number of OpenCL devices  : 3
  Tornado device=0:0
        OpenCL --  [Intel(R) OpenCL] -- Intel(R) Core(TM) i7-7700K CPU @ 4.20GHz
                Global Memory Size: 62.6 GB
                Local Memory Size: 32.0 KB
                Workgroup Dimensions: 3
                Total Number of Block Threads: 8192
                Max WorkGroup Configuration: [8192, 8192, 8192]
                Device OpenCL C version: OpenCL C 1.2

  Tornado device=0:1
        OpenCL --  [Intel(R) FPGA SDK for OpenCL(TM)] -- p385a_sch_ax115 : nalla_pcie (aclnalla_pcie0)
                Global Memory Size: 8.0 GB
                Local Memory Size: 16.0 KB
                Workgroup Dimensions: 3
                Total Number of Block Threads: 2147483647
                Max WorkGroup Configuration: [2147483647, 2147483647, 2147483647]
                Device OpenCL C version: OpenCL C 1.0

  Tornado device=0:2
        OpenCL --  [NVIDIA CUDA] -- Quadro GP100
                Global Memory Size: 15.9 GB
                Local Memory Size: 48.0 KB
                Workgroup Dimensions: 3
                Total Number of Block Threads: 1024
                Max WorkGroup Configuration: [1024, 1024, 64]
                Device OpenCL C version: OpenCL C 1.2

```

### Configuring Execution Properties and JVM Arguments

1. Execute `tornado --printFlags` and copy all flags starting from `-server`. 
 
2. Then paste the above flags in the `env.java.opts` field of the `flink-conf.yaml` file, which is located in the `Flink-TornadoVM-Artifact/Flink-TornadoVM/build-target/conf` directory. To indicate that the execution will go through TornadoVM, include also the flag `-Dtornado=true` . 

3. Finally, to specify on which device the computation will be executed include the option `-D<task_schedule-name>.<task-name>.device=<driverNumber>:<deviceNumber>`.

Below is presented an example of the `env.java.opts` field, if we assume that the computation to be executed has two Task Schedules, named s0.t0 and s1.t1 respectively. Let's also assume that both Task Schedules should be executed on the device 0:2 (which was a Quadro GP100 GPU in the example presented in the section **Identify the hardware accelerators**.) 
```bash
    ## Set the Java Home
    env.java.home: path/to/JDK1.8.0_302

    ## Set the JVM arguments
    env.java.opts: "-server -XX:-UseCompressedOops -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI -Djava.library.path=path/to/TornadoVM/bin/sdk/lib -Djava.ext.dirs=<path/to/>TornadoVM/bin/sdk/share/java/tornado -Dtornado.load.api.implementation=uk.ac.manchester.tornado.runtime.tasks.TornadoTaskSchedule -Dtornado.load.runtime.implementation=uk.ac.manchester.tornado.runtime.TornadoCoreRuntime -Dtornado.load.tornado.implementation=uk.ac.manchester.tornado.runtime.common.Tornado -Dtornado.load.device.implementation.opencl=uk.ac.manchester.tornado.drivers.opencl.runtime.OCLDeviceFactory -Dtornado.load.device.implementation.ptx=uk.ac.manchester.tornado.drivers.ptx.runtime.PTXDeviceFactory -Dtornado.load.device.implementation.spirv=uk.ac.manchester.tornado.drivers.spirv.runtime.SPIRVDeviceFactory -Dtornado.load.annotation.implementation=uk.ac.manchester.tornado.annotation.ASMClassVisitor -Dtornado.load.annotation.parallel=uk.ac.manchester.tornado.api.annotations.Parallel -XX:-UseJVMCIClassLoader -Dtornado.heap.allocation=2GB -Xmx32G -Xms32G -Dtornado=true -Dtornado.experimental.reduce=false -Ds0.t0.device=0:2 -Ds1.t1.device=0:2 -Ds2.t2.device=0:2"
```
3. Additionally, include the following options in the `flink-conf.yaml` file:

``` bash
akka.framesize: 335544320b

akka.ask.timeout: 10000s

heartbeat.timeout: 50000000

web.timeout: 10000000

taskmanager.memory.framework.off-heap.size: 500m


rest.client.max-content-length: 334772160
```
4. The JVM heap size can be set in the `env.java.opts` field, e.g.: 

```bash 
env.java.opts: "... -Xmx65G -Xms65G"
```

#### Other TornadoVM options

If tornado runs out of heap, you can increase its heap size by configuring the `-Dtornado.heap.allocation` flag (e.g `-Dtornado.heap.allocation=2048MB`)

### Flink-GPU setup
One of the experiments presented in the paper was to compare Flink-TornadoVM against Flink's current support for GPU execution (hereafter refered to as *Flink-GPU*).
Detailed information about Flink-GPU can be found here: https://flink.apache.org/news/2020/08/06/external-resource.html. 
 
To run Flink-GPU the following options should be included in the `flink-conf.yaml` file:

```bash
external-resources: gpu
# Define the driver factory class of gpu resource.
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory
# Define the amount of gpu resource per TaskManager.
external-resource.gpu.amount: 1
# Enable the coordination mode if you run it in standalone mode
external-resource.gpu.param.discovery-script.args: --enable-coordination 
```

The `-Dtornado` flag should be set to false. 

The CUDA version used for this evaluation was 9.0.176, with the corresponding version of JCuda being 0.9.0d (downloaded from http://javagl.de/jcuda.org/downloads/downloads.html). 
The JCuda jar files have to be downloaded and stored in the "build-target/lib" folder. 

JCuda and JCublas are linked to the project through the flink-examples-batch/pom.xml file as shown below.

```bash
<dependency>
  <groupId>org.jcuda</groupId>
  <artifactId>jcuda</artifactId>
  <version>0.9.0d</version>
  <scope>compile</scope>
</dependency>

<dependency>
  <groupId>org.jcuda</groupId>
  <artifactId>jcublas</artifactId>
  <version>0.9.0d</version>
  <scope>compile</scope>
</dependency>
```

If a different version of JCuda has to be used make sure the pom.xml file is updated.

## Benchmark Configurations
For the paper evaluation seven benchmarks were used in total. 
These experiments can be classified into two caterogies, (1) experiments that were conducted to compare Flink-TornadoVM with scale-out CPU Flink and, (2) experiments to compare Flink-TornadoVM with Flink-GPU.

I) Flink-TornadoVM vs Flink Scale-out

* Matrix Multiplication (GPU & FPGA acceleration)
* KMeans (GPU acceleration)
* DFT (FPGA acceleration)
* Logistic Regression (GPU acceleration)
* IoT use case (GPU acceleration) 

In all of the five experiments above, the baseline (Flink-CPU) was executed using 6 configurations. Each configuration was chosen by including in the `flink_conf.yaml file` the options below:

-  1 Task Manager Node - 1 Task Slot - 1 Parallel Thread
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
```
-  1 Task Manager Node - 2 Task Slot - 2 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
```
-  1 Task Manager Node - 4 Task Slot - 4 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
```
-  2 Task Manager Node - 1 Task Slot - 1 Parallel Thread
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
```
-  2 Task Manager Node - 2 Task Slot - 2 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
```
-  2 Task Manager Node - 4 Task Slot - 4 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
```
       
The GPU experiments were executed on Flink-TornadoVM using the following two configurations: 

-  1 Task Manager Node - 1 Task Slot - 1 Parallel Thread
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
```
- 2 Task Manager Node - 1 Task Slot - 2 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 2
```
NOTE: For the IoT use case, two additional configurations of Flink-TornadoVM were tested: 

-  1 Task Manager Node - 1 Task Slot - 4 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 4
```

-  1 Task Manager Node - 1 Task Slot - 8 Parallel Threads
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 8
```

Finally, the FPGA experiments were executed with the following configuration:  
-  1 Task Manager Node - 1 Task Slot - 1 Parallel Thread
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
```

II) Flink-GPU vs Flink-TornadoVM vs Flink 
* Pi Estimation
* Vector Addition 

For these two experiments the following configuration was used for all three implementations (Flink, Flink-GPU and Flink-TornadoVM) : 

-  1 Task Manager Node - 1 Task Slot - 1 Parallel Thread
    
```bash
taskmanager.numberOfTaskSlots: 1
parallelism.default: 1
```

### Define the Task Managers
To define the worker nodes, include the addresses of the worker machines in the `build-target/conf/worker` file.
 

## Scripts and Datasets
 
Download (https://drive.google.com/file/d/1F_1X3aH89pK7V13nSURiv33fXVq9yjL0/view?usp=sharing) and extract the directory `Datasets-Scripts.tar.xz` which contains one folder for each experiment. 

```bash
tar -xf Datasets-Scripts.tar.xz
```
### Matrix Multiplications
1. The datasets are in directory `Matrix_Multiplication/matrix_datasets`.

2. Script to run: Copy the script `run-matrix.sh`, which resides in the `Matrix_Multiplication` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets. Run the script with four arguments,  i) the parallelism, ii) the number of task managers, (iii) Flink or Flink-TornadoVM and, (iv) input size
E.g. 
```bash
./run-matrix.sh 1 1 Flink-TornadoVM 256
```

3. Execution Device: This use case has only one Task Schedule, identified as `t0.s0`, so in the `env.java.opts` field include the flag `-Ds0.t0.device=<driverNumber>:<deviceNumber>` to execute it on the apppropriate device. 

 * GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
 * FPGA Execution: Select the FPGA driver for the task schedule and set the flag `-Dtornado` to true. Follow the instructions in the file `7_FPGA.md` to set up the FPGA configuration file. Then copy the `TornadoVM/etc` directory, which includes the FPGA configuration file, in the `Flink-TornadoVM/build-target` folder. Initially, the FPGA bitstreams must be generated (JIT Mode). For each bitstream this process should take 2 hours approximately. The generated bitstream resides in the directory specified in the FPGA configuration file. A bistream should be generated for each dataset (128x128, 256x256, 512x512). 
 The bitstreams can be used for ahead-of-time execution to evaluate performance.
 * CPU Execution: Set the flag `-Dtornado` to false.
 
### KMeans
1. The datasets are in the directory `KMeans/kmeans_datasets`.

2. Script to run: Copy the script `run-kmeans.sh`, which resides in the `KMeans` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets.  Run the script with four arguments,  i) the parallelism, ii) the number of task managers, (iii) Flink or Flink-TornadoVM and, (iv) input size
E.g. 
```bash
./run-kmeans.sh 1 1 Flink 32768
```

3. Execution Device: This use case has four Task Schedules, identified as `s0.t0`, `s1.t1`, `s2.t2` and `s3.t3`, so in the `env.java.opts` field include the flags `-Ds0.t0.device=<driverNumber>:<deviceNumber> Ds1.t1.device=<driverNumber>:<deviceNumber> Ds2.t2.device=<driverNumber>:<deviceNumber> Ds3.t3.device=<driverNumber>:<deviceNumber>` to execute the code on the apppropriate device. 

  * GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
  * CPU Execution: Set the flag `-Dtornado` to false.
  
4. This benchmark additionally requires the flag `-Dtornado.experimental.reduce=false` in the `env.java.opts` field of the `flink-conf.env` file. 

### DFT
1. The datasets are in the directory `DFT/dft_datasets`

2. Script to run: Copy the script `run-dft.sh`, which resides in the `DFT` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets. Run the script with four arguments,  i) the parallelism, ii) the number of task managers, (iii) Flink or Flink-TornadoVM and, (iv) input size
E.g. 
```bash
./run-dft.sh 1 1 Flink-TornadoVM 2048
```

3. Execution Device: This use case has only one Task Schedule, identified as `t0.s0`, so in the `env.java.opts` field include the flag `-Ds0.t0.device=<driverNumber>:<deviceNumber>` to execute it on the apppropriate device. 
 * FPGA Execution: Select the FPGA driver for the task schedule and set the flag `-Dtornado` to true. Follow the instructions in the file `7_FPGA.md` to set up the FPGA configuration file. Then copy the `TornadoVM/etc` directory, which includes the FPGA configuration file, in the `Flink-TornadoVM/build-target` folder. Initially, the FPGA bitstreams must be generated (JIT Mode). For each bitstream this process should take 2 hours approximately. The generated bitstream resides in the directory specified in the FPGA configuration file. A bistream should be generated for each dataset (2048, 4096, 8192, 16384, 32768, 65536). 
 The bitstreams can be used for ahead-of-time execution to evaluate performance.
 * CPU Execution: Set the flag `-Dtornado` to false.
 
### Logistic Regression 
1. The datasets are in the directory `Logistic_Regression/lr_datasets`

2. Script to run: Copy the script `run-lr.sh`, which resides in the `Logistic_Regression` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets. Run the script with four arguments,  i) the parallelism, ii) the number of task managers, (iii) Flink or Flink-TornadoVM and, (iv) input size
E.g. 
```bash
./run-lr.sh 1 1 Flink /path/to/lr_datasets/data.csv
```

3. Execution Device: This use case has seven Task Schedules, identified as `s0.t0`, `s1.t1`, `s2.t2`, `s3.t3`, `s4.t4`, `s5.t5` and `s6.t6`, so in the `env.java.opts` field include the flags `-Ds0.t0.device=<driverNumber>:<deviceNumber> Ds1.t1.device=<driverNumber>:<deviceNumber> Ds2.t2.device=<driverNumber>:<deviceNumber> Ds3.t3.device=<driverNumber>:<deviceNumber> Ds4.t4.device=<driverNumber>:<deviceNumber> Ds5.t5.device=<driverNumber>:<deviceNumber> Ds6.t6.device=<driverNumber>:<deviceNumber>` to execute the code on the apppropriate device. 

  * GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
  * CPU Execution: Set the flag `-Dtornado` to false.

4. This benchmark additionally requires the flag `-Dtornado.experimental.reduce=false` in the `env.java.opts` field of the `flink-conf.env` file. 

5. Additional evaluation: 
The evaluation for this experiment includes two additional experiments, (a) a breakdown analysis and, (b) the evaluation of the execution on different dataset sizes. 
(a) For the breakdown analysis include the option `-Dflinktornado.breakdown=true`
(b) The additional datasets are in the directory `lr_additional_datasets`. To get the breakdown of the Task Schedule execution make sure to enable the TornadoVM profiler by including the flags `-Dtornado.profiler=True -Dtornado.profiler.dump.dir=FILENAME.json` in the `flink-conf.yaml` file.
Detailed information about the evaluation of these experiments will be provided in the Section **Evaluation** 

### IoT
1. Dataset is in the directory `iot_dataset`

2. Script to run: Copy the script `run-iot.sh`, which resides in the `IoT` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets.

3. Execution Device: This use case has only one Task Schedule, identified as `t0.s0`, so in the `env.java.opts` field include the flag `-Ds0.t0.device=<driverNumber>:<deviceNumber>` to execute it on the apppropriate device. 

 * GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
 * CPU Execution: Set the flag `-Dtornado` to false.
 
4. This benchmark additionally requires the flag `-Dtornado.experimental.reduce=false` in the `env.java.opts` field of the `flink-conf.env` file.  

### Vector Addition
1. Datasets: The datasets are generated by the program. 

2. Script to run: 
	- To run Flink-TornadoVM and Flink, copy the script `run_vadd.sh`, which resides in the `Vector_Addition` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets.
	- To run Flink-GPU, copy the script `run_vadd-flinkgpu.sh`, which resides in the `Vector_Addition` directory, to the `Flink-TornadoVM/flink-tornado-internal/build-target` folder. Edit the script to use the appropriate path for the datasets.

3. Execution Device:
	- Flink-TornadoVM
		* GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
	- Flink
		* CPU Execution: Set the flag `-Dtornado` to false.
	- Flink-GPU 
		* GPU Execution: Set the flag `-Dtornado` to false. Make sure to follow the steps in the **Flink-GPU setup** section before the execution. 

4. This use case uses precompiled kernels to ensure fair comparison. Therefore, it is necessary to copy the following file from the `precompiled_kernels` to the `build-target/bin` folder:
	- vadd-map.cl  
	
This benchmark additionally requires the flag `-Dprecompiled=true` in the `env.java.opts` field of the `flink-conf.env` file to run the precompiled kernels on the Flink-TornadoVM integration. 

### Pi Estimation
1. Datasets: The datasets are generated by the program.

2. Script to run: 
	- To run Flink-TornadoVM and Flink, copy the script `run_pi.sh`, which resides in the `Pi-Estimation` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets.
	- To run Flink-GPU, copy the script `run_pi-flinkgpu.sh`, which resides in the `Pi-Estimation` directory, to the `Flink-TornadoVM/build-target` folder. Edit the script to use the appropriate path for the datasets.

3. Execution Device:
	- Flink-TornadoVM
		* GPU Execution: Select the GPU driver for the task schedule and set the flag `-Dtornado` to true.
	- Flink
		* CPU Execution: Set the flag `-Dtornado` to false.
	- Flink-GPU 
		* GPU Execution: Set the flag `-Dtornado` to false. Make sure to follow the steps in the **Flink-GPU setup** section before the execution. 

4. This use case uses precompiled kernels to ensure fair comparison. Therefore, it is necessary to copy the following files from the `precompiled_kernels` to the `build-target/bin` folder: 
	- pi-map.cl
	- pi-reduce.cl
	- pi-map.ptx
	- pi-reduce.ptx 
	
This benchmark additionally requires the flag `-Dprecompiled=true` in the `env.java.opts` field of the `flink-conf.env` file to run the precompiled kernels on the Flink-TornadoVM integration. 

## Evaluation

### End-To-End Speedups
The end-to-end execution time is the **Job Runtime** outputted by Flink when the execution is completed. 
E.g., 

```bash 
Job has been submitted with JobID f1464184ed58c521003c7c0662d27887
Program execution finished
Job with JobID f1464184ed58c521003c7c0662d27887 has finished.
Job Runtime: 5608 ms <---------
Accumulator Results: 
- 22a734b5b61d03351aca43cce47f8772 (java.util.ArrayList) [2 elements]

```
A Java program called `Speedups.java` is provided in the Flink-TornadoVM-Artifact/Evaluation directory.

Compile it and run with the following options:
* --tornadoFlinkConf *the number of Task Managers used for the Flink-TornadoVM i.e. 1 or 2*
* --tornadoFlink *the path to end-to-end numbers of Flink-TornadoVM*
* --flink  *the path to end-to-end numbers of Flink*
* --usecase *the use case i.e. MM or KM or DFT or IoT or Pi or VAdd*
* --output *the path to store the results*

E.g. 

```bash
javac Speedups.java
java Speedups --tornadoFlinkConf 1 --tornadoFlink /home/user/results/flinkTornadoVM/DFT --flink /home/user/results/flink/DFT --usecase DFT --output /home/user/evaluation/DFT
```

A csv file containing the speedups of Flink-TornadoVM against Flink for all the datasets is generated. 

### Additional Experiments

1) Breakdown analysis
To get the breakdown analysis of the Logistic Regression benchmark for 1 Task Manager compile the `BreakdownEvaluation.java`  program (which resides in the Evaluation folder) and run it with the following options:
* --log *the Flink Task Manager out file that contains the timers for each computation*
* --directory *the directory to store the csv files that have the analysis*

E.g.
```bash
javac BreakdownEvaluation.java
java BreakdownEvaluation --log /home/user/flink-taskexecutor-1-silver1.out --directory /home/user/Flink-TornadoVM-Artifact/Evaluation/
```

This generates three csv files. The first one, `NumbersFilled.csv`, categorizes the numbers in the flink log per action. 
The second one, `NumbersFilled-AVG.csv` calculate the average of those numbers. Finally, the third one, `Breakdown-Simplified`, provides a simplified version of the numbers in the `NumbersFilled-AVG.csv` file.  

Note that, as mentioned in the previous section, the flag `` has to be in the java.env.opts in order to get the detailed timers. 

2) TornadoVM Profiler Analysis

Compile the `TornadoVMProfilerAnalysis.java` program (which resides in the Evaluation folder) and run it with the following options:

* --profiler *the output of the TornadoVM profiler*
* --directory *the directory to store the csv file that have the analysis*
* --size *the size of the dataset*

E.g.
```bash
javac TornadoVMProfilerAnalysis.java
java TornadoVMProfilerAnalysis --profiler /home/user/prof.json --directory /home/user/results --size 56
```

This generates a csv file named TornadoProfilerAnalysis-[dataset size].csv, which contains the simplified profiler numbers. 

## Known Issues

Sometimes (for both Flink-TornadoVM and Flink) either the Job Manager or the Task Manager JVM process might crash during execution. In this case, the cluster has to be restarted.
