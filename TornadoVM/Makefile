all: build

# Variable passed for the build process:
# BACKENDS=<ptx|opencl|spirv>. It specifies which backend/s to use { opencl, ptx, spirv }. The default one is `opencl`.
BACKEND?=opencl
BACKENDS?=$(BACKEND)

build jdk-8:
	./bin/compile.sh jdk-8 $(BACKENDS)

jdk-11-plus:
	./bin/compile.sh jdk-11-plus $(BACKENDS)

graal-jdk-8:
	./bin/compile.sh graal-jdk-8 $(BACKENDS)

graal-jdk-11-plus:
	./bin/compile.sh graal-jdk-11-plus $(BACKENDS)

ptx:
	./bin/compile.sh jdk-8 BACKENDS=ptx,opencl

spirv:
	./bin/compile.sh jdk-8 BACKENDS=spirv,ptx,opencl

offline:
	./bin/compile.sh jdk-8 $(BACKENDS) OFFLINE

# Variable passed for the preparation of the Xilinx FPGA emulated target device. The default device is `xilinx_u50_gen3x16_xdma_201920_3`.
# make xilinx_emulation FPGA_PLATFORM=<platform_name> NUM_OF_FPGA_DEVICES=<number_of_devices>
FPGA_PLATFORM?=xilinx_u50_gen3x16_xdma_201920_3
NUM_OF_FPGA_DEVICES?=1

xilinx_emulation:
	emconfigutil --platform $(FPGA_PLATFORM) --nd $(NUM_OF_FPGA_DEVICES) --od $(JAVA_HOME)/bin

clean: 
	mvn -Popencl-backend,ptx-backend clean

example:
	tornado --printKernel --debug uk.ac.manchester.tornado.examples.VectorAddInt 8192

tests:
	tornado --devices 
	tornado-test.py --ea --verbose
	tornado-test.py --ea -V -J"-Dtornado.heap.allocation=1MB" uk.ac.manchester.tornado.unittests.fails.HeapFail#test03
	test-native.sh

test-slam:
	tornado-test.py -V --fast uk.ac.manchester.tornado.unittests.slam.graphics.GraphicsTests 

test-hdgraphics:
	@[ "${HDGRAPHICS_ID}" ] || ( echo ">> HDGRAPHICS_ID is not set. Please do \`export HDGRAPHICS_ID=<dev-id>\`"; exit 1 )
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.reductions.TestReductionsFloats
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.reductions.TestReductionsDoubles
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.reductions.TestReductionsIntegers
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.reductions.TestReductionsLong
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.reductions.TestReductionsAutomatic
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.kernelcontext.matrices.TestMatrixMultiplicationKernelContext
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.kernelcontext.reductions.TestReductionsIntegersKernelContext
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.kernelcontext.reductions.TestReductionsFloatsKernelContext
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.kernelcontext.reductions.TestReductionsDoublesKernelContext
	tornado-test.py -V -J"-Ds0.t0.device=0:${HDGRAPHICS_ID} -Ds0.t1.device=0:${HDGRAPHICS_ID}" uk.ac.manchester.tornado.unittests.kernelcontext.reductions.TestReductionsLongKernelContext

eclipse:
	mvn eclipse:eclipse

clean-graphs:
	rm *.cfg *.bgv *.log
