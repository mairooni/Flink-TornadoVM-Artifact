# Installing TornadoVM

**Supported Platforms**

The following table includes the platforms that TornadoVM can be executed.

| OS                         | OpenCL Backend                                             | PTX Backend | SPIR-V Backend            | 
| -------------------------- | ---------------------------------------------------------------------------------------------------- |
| CentOS >= 7.3              | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Level-Zero >= 1.1.2       |
| Fedora >= 21               | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Level-Zero >= 1.1.2       |
| Ubuntu >= 16.04            | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Level-Zero >= 1.1.2       |
| Mac OS X Mojave 10.14.6    | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Not supported             |
| Mac OS X Catalina 10.15.3  | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Not supported             |
| Mac OS X Big Sur 11.5.1    | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Not supported             |
| Windows 10                 | OpenCL for GPUs and CPUs >= 1.2, OpenCL for FPGAs >= 1.0)  |  CUDA 9.0+  | Not supported/tested      |

Note: The SPIR-V backend is only supported for Linux OS. Besides, the SPIR-V backend with Level Zero runs on Intel HD Graphics (integrated GPUs). 

## 1. Installation

TornadoVM can be built with three compiler backends and is able to generate OpenCL, PTX and SPIR-V code. 

**Important [SPIR-V Backend Configuration]** Prior to the built with the SPIR-V backend, users have to ensure that Level Zero is installed in their system. Please follow the guidelines [here](22_SPIRV_BACKEND_INSTALL.md).

At least one backend must be specified at build time to the `make` command:

```bash
$ make BACKENDS=opencl,ptx,spirv
```

As well as being built with three compiler backends, TornadoVM can be executed with the following three configurations:

* TornadoVM with JDK 8 with JVMCI support: see the installation guide [here](11_INSTALL_WITH_JDK8.md).
* TornadoVM with GraalVM (either with JDK 8, JDK 11 and JDK 16): see the installation
  guide [here](10_INSTALL_WITH_GRAALVM.md).
* TornadoVM with JDK11+ (e.g. OpenJDK [11-16], Red Hat Mandrel, Amazon Corretto): see the installation
  guide [here](12_INSTALL_WITH_JDK11_PLUS.md).

Note: To run TornadoVM in Windows OS, install TornadoVM with GraalVM. More
information [here](assembly/src/docs/20_INSTALL_WINDOWS_WITH_GRAALVM.md).

Note: To run TornadoVM on ARM Mali, install TornadoVM with GraalVM and JDK 11. More information [here](18_MALI.md).

## 2. Running Examples

```bash
$ tornado uk.ac.manchester.tornado.examples.compute.MatrixMultiplication1D
```

Use the following command to identify the ids of the Tornado-compatible heterogeneous devices:

```bash
$ tornado --devices
```

Tornado device output corresponds to:

```bash
Tornado device=<driverNumber>:<deviceNumber>
```

Example output:

```bash
Number of Tornado drivers: 2
Total number of PTX devices  : 1
Tornado device=0:0
  PTX -- GeForce GTX 1650
      Global Memory Size: 3.8 GB
      Local Memory Size: 48.0 KB
      Workgroup Dimensions: 3
      Max WorkGroup Configuration: [1024, 1024, 64]
      Device OpenCL C version: N/A

Total number of OpenCL devices  : 4
Tornado device=1:0
  NVIDIA CUDA -- GeForce GTX 1650
      Global Memory Size: 3.8 GB
      Local Memory Size: 48.0 KB
      Workgroup Dimensions: 3
      Max WorkGroup Configuration: [1024, 1024, 64]
      Device OpenCL C version: OpenCL C 1.2

Tornado device=1:1
  Intel(R) OpenCL HD Graphics -- Intel(R) Gen9 HD Graphics NEO
      Global Memory Size: 24.8 GB
      Local Memory Size: 64.0 KB
      Workgroup Dimensions: 3
      Max WorkGroup Configuration: [256, 256, 256]
      Device OpenCL C version: OpenCL C 2.0

Tornado device=1:2
	Intel(R) OpenCL -- Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
		Global Memory Size: 31.0 GB
		Local Memory Size: 32.0 KB
		Workgroup Dimensions: 3
		Max WorkGroup Configuration: [8192, 8192, 8192]
		Device OpenCL C version: OpenCL C 1.2

Tornado device=1:3
	AMD Accelerated Parallel Processing -- Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
		Global Memory Size: 31.0 GB
		Local Memory Size: 32.0 KB
		Workgroup Dimensions: 3
		Max WorkGroup Configuration: [1024, 1024, 1024]
		Device OpenCL C version: OpenCL C 1.2

```

**The output might vary depending on which backends you have included in the build process. To run TornadoVM, you should
see at least one device.**

To run on a specific device use the following option:

```bash
 -D<s>.<t>.device=<driverNumber>:<deviceNumber>
```

Where `s` is the *schedule name* and `t` is the task name.

For example running on `driver:device` [1][1] (Intel HD Graphics in our example) will look like this:

```bash
$ tornado -Ds0.t0.device=1:1 uk.ac.manchester.tornado.examples.compute.MatrixMultiplication1D
```

The command above will run the MatrixMultiplication1D example on the integrated GPU (Intel HD Graphics).

## 3. Running Benchmarks

###### Running all benchmarks with default values

```bash
$ tornado-benchmarks.py
Running TornadoVM Benchmarks
[INFO] This process takes between 30-60 minutes
List of benchmarks: 
       *saxpy
       *addImage
       *stencil
       *convolvearray
       *convolveimage
       *blackscholes
       *montecarlo
       *blurFilter
       *renderTrack
       *euler
       *nbody
       *sgemm
       *dgemm
       *mandelbrot
       *dft
[INFO] TornadoVM options: -Xms24G -Xmx24G -server 
....
```

###### Running a specific benchmark

```bash
$ tornado uk.ac.manchester.tornado.benchmarks.BenchmarkRunner sgemm
```

## 4. Running Unittests

To run all unittests in Tornado:

```bash
$ make tests
```

To run an individual unittest:

```bash
$  tornado-test.py uk.ac.manchester.tornado.unittests.TestHello
```

Also, it can be executed in verbose mode:

```bash
$ tornado-test.py --verbose uk.ac.manchester.tornado.unittests.TestHello
```

To test just a method of a unittest class:

```bash
$ tornado-test.py --verbose uk.ac.manchester.tornado.unittests.TestHello#testHello
```

To see the OpenCL/PTX generated kernel for a unittest:

```bash
$ tornado-test.py --verbose -pk uk.ac.manchester.tornado.unittests.TestHello#testHello
```

To execute in debug mode:

```bash
$ tornado-test.py --verbose --debug uk.ac.manchester.tornado.unittests.TestHello#testHello
task info: s0.t0
	platform          : NVIDIA CUDA
	device            : GeForce GTX 1050 CL_DEVICE_TYPE_GPU (available)
	dims              : 1
	global work offset: [0]
	global work size  : [8]
	local  work size  : [8]
```

## 5. IDE Code Formatter

### Using Eclipse and Netbeans

The code formatter in Eclipse is automatically applied after generating the setting files.

```bash
$ mvn eclipse:eclipse
$ python scripts/eclipseSetup.py
```

For Netbeans, the Eclipse Formatter Plugin is needed.

### Using IntelliJ

Install plugins:

* Eclipse Code Formatter
* Save Actions

Then :

1. Open File > Settings > Eclipse Code Formatter
2. Check the `Use the Eclipse code` formatter radio button
2. Set the Eclipse Java Formatter config file to the XML file stored in /scripts/templates/eclise-settings/Tornado.xml
3. Set the Java formatter profile in Tornado

## 6. TornadoVM Maven Projects

To use the TornadoVM API in your projects, you can checkout our maven repository as follows:

```xml

<repositories>
    <repository>
        <id>universityOfManchester-graal</id>
        <url>https://raw.githubusercontent.com/beehive-lab/tornado/maven-tornadovm</url>
    </repository>
</repositories>

<dependencies>
<dependency>
    <groupId>tornado</groupId>
    <artifactId>tornado-api</artifactId>
    <version>0.11</version>
</dependency>

<dependency>
    <groupId>tornado</groupId>
    <artifactId>tornado-matrices</artifactId>
    <version>0.11</version>
</dependency>
</dependencies>
```

Notice that, for running with TornadoVM, you will need either the docker images or the full JVM with TornadoVM enabled.

#### Versions available

* 0.11
* 0.10
* 0.9
* 0.8
* 0.7
* 0.6
* 0.5
* 0.4
* 0.3
* 0.2
* 0.1.0
