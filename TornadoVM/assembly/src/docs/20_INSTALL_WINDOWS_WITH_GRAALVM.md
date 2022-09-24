## Build TornadoVM with Graal 21.2.0 JDK 8/11/16 on Windows 10 (x64)


_**[DISCLAIMER] Please, notice that, although TornadoVM can run on Windows10, it is still experimental.**_

#### 1. Install prerequisites

##### Maven
Download Apache Maven from the [official site](https://maven.apache.org/download.cgi) and extract it to the any location. Below it's assumed that Maven's home is `C:/Maven`.
##### MSys2
1. Download [MSys2](https://www.msys2.org/) installer from the official site and run it. You may choose any installation directory, below it will be referred as `<MSYS2>`. IMPORTANT: the only executable you should use as a terminal is `<MSYS2>/mingw64.exe`.

2. Update MSys2 _**system**_ packages. Start `<MSYS2>/mingw64.exe` and run the following command in the terminal:
```bash
pacman -Syu
```
You might need to execute it several times until you see that no updates found.

3. Update MSys2 _**default**_ packages. In the terminal window of `<MSYS2>/mingw64.exe` run:
```bash
pacman -Su
```
You might need to execute it several times until you see that no updates found.

4. Install necessary tools to MSys2. In the terminal window of `<MSYS2>/mingw64.exe` run:
```bash
pacman -S \
mingw-w64-x86_64-make           \
mingw-w64-x86_64-cmake          \
mingw-w64-x86_64-gcc            \
mingw-w64-x86_64-opencl-headers \
mingw-w64-x86_64-opencl-icd-git \
python make git
```

5. Create default Maven repository for MSys2 user:
```bash
cd ~
mkdir .m2
```

6. Create file `mvn` in `<MSYS2>/mingw64/bin` with any textual editor according to the template below:
```bash
#!/usr/bin/env bash
BASH_FILE=$(where bash)
SETTINGS=\"${BASH_FILE%\\*\\*\\*}${HOME}/.m2/settings.xml\"

C:/Maven/bin/mvn.cmd --settings ${SETTINGS} "$@"
```
You should only change path to Maven executable to the actual location of Maven in your system.

#### 2. Download the TornadoVM sources
Clone the latest TornadoVM sources from the GitHub [repository](https://github.com/beehive-lab/TornadoVM) using `<MSYS2>/mingw64.exe`:
```
cd D:/MyProjects
git clone https://github.com/beehive-lab/TornadoVM.git
cd TornadoVM
```
We will refer hereafter the directory with TornadoVM sources as `<TornadoVM>`.

#### 3. Download Graal 21.2.0

Download the version you prefer. TornadoVM can run with JDK 8, 11 and 16. Visit [GraalVM Community Edition 21.2.0](https://github.com/graalvm/graalvm-ce-builds/releases/tag/vm-21.2.0) and download either of the following builds:
- [Download for JDK 8](https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.2.0/graalvm-ce-java8-windows-amd64-21.2.0.zip)
- [Download for JDK 11](https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.2.0/graalvm-ce-java11-windows-amd64-21.2.0.zip)
- [Download for JDK 16](https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.2.0/graalvm-ce-java16-windows-amd64-21.2.0.zip) 

Extract the downloaded file to any directory.

#### 4. Install NVIDIA CUDA and NVIDIA driver
If you don't need PTX backend that depends on CUDA you may just download and install regular driver that corresponds to your GPU from [NVIDIA Driver Downloads](https://www.nvidia.com/Download/index.aspx) page. 

Otherwise you need complete CUDA Toolkit from [CUDA Toolkit Downloads](https://developer.nvidia.com/cuda-downloads?target_os=Windows&target_arch=x86_64) page. Note, that toolkit includes GPU drivers as well, so you may avoid downloading drivers separately. The only thing to note is that GPU driver you are currently using should be of the same or higher version than the one shipped with CUDA Toolkit. So if you have existing driver make sure that it's version is same or higher, otherwise update GPU driver during toolkit installation. Note, that NSight, BLAST libs and Visual Studio integration are irrelevant for TornadoVM builds, you need just SDK - so you may skip installing them.

#### 5. Configure the build
Using any text editor create file `<TornadoVM>/etc/sources.env`  with the following content:
```bash
#!/bin/bash

# UPDATE PATH TO ACTUAL LOCATION OF THE JDK OR GRAAL (REMEMBER OF UNIX_STYLE SLASHES AND SPACES!!!)
export JAVA_HOME="C:/graalvm-ce-java8-21.2.0"  

## NEXT TWO LINES NECESSARY TO BUILD PTX (NVIDIA CUDA) BACKEND
## COMMENT THEM OUT OR JUST IGNORE IF YOU ARE NOT INTERESTED IN PTX BUILD
## OTHERWISE UPDATE 'CUDA_PATH' WITH ACTUAL VALUE (REMEMBER OF UNIX_STYLE SLASHES AND SPACES!!!)
export CUDA_PATH="C:/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.2"
export PTX_LDFLAGS=-L\"$CUDA_PATH/lib/x64\"

# LEAVE THE REST OF FILE 'AS IS'
# DON'T ALTER!
export PATH=$PWD/bin/bin:$PATH               ## This directory will be automatically generated during Tornado compilation
export TORNADO_SDK=$PWD/bin/sdk              ## This directory will be automatically generated during Tornado compilation
CMAKE_FILE=$(where cmake | head -n 1)
export CMAKE_ROOT=${CMAKE_FILE%\\*\\*}
```
There are only 2 places you should adjust:
1. `JAVA_HOME` path that points to your Graal installation
2. `CUDA_PATH` pointing to your NVIDIA GPU Computing Toolkit (CUDA) - this one is necessary only for builds with PTX backend.

#### 6. Compile TornadoVM
Start `<MSYS2>/mingw64.exe` terminal, navigate to the `<TornadoVM>`  directory and execute build:
```bash
cd D:/MyProjects/TornadoVM
source etc/sources.env
make graal-jdk-8 BACKEND=ptx,opencl
```
The latest command, `make`, depends on the Graal version you are using:
1. For Graal JDK 8 use `make graal-jdk-8 BACKEND=ptx,opencl` (as above)
2. For Graal JDK 11 use `make graal-jdk-11 BACKEND=ptx,opencl` 

The `BACKEND` parameter has to be a comma-separated list of `ptx` and `opencl` options. You may build `ptx` only when NVIDIA GPU Computing Toolkit (CUDA) is installed.

#### 7. Check the installation 

Don't close `<MSYS2>/mingw64.exe` after the build. Run the following command to see that TornadoVM is working:
```bash
tornado --devices
```
You should see a list of OpenCL and/or CUDA devices available on your system.

Now try to run a simple test. To run examples with Graal JDK 11, TornadoVM uses modules:
```bash 
tornado -m tornado.examples/uk.ac.manchester.tornado.examples.compute.MatrixMultiplication2D 512
```

To run individual tests:
```bash
tornado -Dtornado.unittests.verbose=True -Xmx6g  -m  tornado.unittests/uk.ac.manchester.tornado.unittests.tools.TornadoTestRunner uk.ac.manchester.tornado.unittests.arrays.TestArrays
```

To run examples with Graal JDK 8:
```bash 
tornado uk.ac.manchester.tornado.examples.compute.MatrixMultiplication2D 512
```

To run individual tests:
```bash
tornado -Dtornado.unittests.verbose=True -Xmx6g  uk.ac.manchester.tornado.unittests.tools.TornadoTestRunner uk.ac.manchester.tornado.unittests.arrays.TestArrays
```

To run all unit-tests:
```bash
make tests
```

### Known issues
1. If you already have MSys2 installed and heavily customized you may experience issues with build or tests. We are suggesting to start with fresh MSys2 installation in this case and follow the instructions above. Most notably, make sure that you have no `mingw-w64-x86_64-python` installed - it prevents Python scripts that execute tests from running. Also, make sure that you have updated all GCC / Make / CMake packages mentioned.
2. If you see no output from `tornado --devices` this may be either of 2 reasons:
-- OpenCL / CUDA is misconfigured. Download any third-party tool for OpenCL / CUDA capabilities viewing and check that you can see your devices there. Sometimes order of installation of different OpenCL drivers matters - Intel OpenCL SDK may shadow NVIDIA OpenCL and alike.
-- You build native code of the library using wrong compiler, most probably you ran `<MSYS2>/msys2.exe` terminal instead of `<MSYS2>/mingw64.exe` . Please re-try with correct terminal (and therefore GCC) version.
3. If you see JVM crashes or `UnsatisfiedLinkError` or some `Error initializing DLL` during `tornado --devices` execution than it's definitely due to wrong GCC (and hence terminal) version used during build.
