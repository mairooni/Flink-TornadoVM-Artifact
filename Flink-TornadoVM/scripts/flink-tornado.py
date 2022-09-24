#!/usr/bin/env python

import sys
import os
from enum import Enum

class Colors:
	RED   = "\033[1;31m"  
	BLUE  = "\033[1;34m"
	CYAN  = "\033[1;36m"
	GREEN = "\033[0;32m"
	RESET = "\033[0;0m"
	BOLD    = "\033[;1m"
	REVERSE = "\033[;7m"

testListLocal = [
    #"org.apache.flink.examples.java.arrays.Arrays",
    "org.apache.flink.examples.java.tornadovm.TestCopyIntegers",
    "org.apache.flink.examples.java.tornadovm.TestCopyFloats",
    "org.apache.flink.examples.java.tornadovm.TestCopyDoubles",
    "org.apache.flink.examples.java.tornadovm.TestTupleInt",
    "org.apache.flink.examples.java.tornadovm.TestTupleDouble",
    "org.apache.flink.examples.java.tornadovm.TestVectorAddition",
	"org.apache.flink.examples.java.tornadovm.TestTuple2DiffInput",
	"org.apache.flink.examples.java.tornadovm.TestTuple3DiffInput",
	"org.apache.flink.examples.java.tornadovm.TestTuple3Double",
	"org.apache.flink.examples.java.tornadovm.TestTuple3Integer",
	"org.apache.flink.examples.java.tornadovm.TestTuple3ReturnType",
	"org.apache.flink.examples.java.tornadovm.TestTuple3Tuple3",
    "org.apache.flink.examples.java.tornadovm.TestTuple3DiffOutput",
    "org.apache.flink.examples.java.tornadovm.TestTupleWithArrayField",
    "org.apache.flink.examples.java.tornadovm.TestTuple3WithArrayField",
    "org.apache.flink.examples.java.tornadovm.TestReturnTupleWithArrayField",
    "org.apache.flink.examples.java.tornadovm.TestReturnTuple3WithArrayField",
    "org.apache.flink.examples.java.tornadovm.TestMatrixMultiplication",
    #"org.apache.flink.examples.java.clustering.KMeans"
]

testListDistributed = [
    #"build-target/examples/batch/Arrays.jar",
    "build-target/examples/batch/TestCopyIntegers.jar",
    "build-target/examples/batch/TestCopyFloats.jar",
    "build-target/examples/batch/TestCopyDoubles.jar",
    "build-target/examples/batch/TestTupleInt.jar",
    "build-target/examples/batch/TestTupleDouble.jar",
    "build-target/examples/batch/TestVectorAddition.jar",
	"build-target/examples/batch/TestTuple2DiffInput.jar",
	"build-target/examples/batch/TestTuple3DiffInput.jar",
	"build-target/examples/batch/TestTuple3DiffOutput.jar",
	"build-target/examples/batch/TestTuple3Double.jar",
	"build-target/examples/batch/TestTuple3Integer.jar",
	"build-target/examples/batch/TestTuple3ReturnType.jar",
	"build-target/examples/batch/TestTuple3Tuple3.jar",
    "build-target/examples/batch/TestTuple4Tuple4.jar",
    "build-target/examples/batch/TestTupleWithArrayField.jar",
    "build-target/examples/batch/TestTuple3WithArrayField.jar",
    "build-target/examples/batch/TestReturnTupleWithArrayField.jar",
    "build-target/examples/batch/TestReturnTuple3WithArrayField.jar",
    "build-target/examples/batch/TestMatrixMultiplication.jar",
    #"build-target/examples/batch/KMeans.jar"
]

class Command(Enum):
    DEFAULT = 0
    TEST    = 1
    LOCAL   = 2
    DIST    = 3

def runTestsLocal(option):

    print(Colors.BLUE + "\nTesting Local Environment\n\n" + Colors.RESET)

    command = "tornado -Dtornado.debug=True -XX:-UseJVMCINativeLibrary -Dtornado.ignore.nullchecks=True -cp build-target/lib/flink-dist_2.11-1.11.1.jar:build-target/lib/log4j-slf4j-impl-2.12.1.jar -Dtornado=true"
    for t in testListLocal:
        fullCommand = command + " " + t
        print(fullCommand)
        os.system(fullCommand)
        print("\n")

def restartCluster():
    command = "./scripts/flink-management restart"
    os.system(command)

def runTestDistributed(option):
    print(Colors.BLUE + "\nTesting Distributed Setting\n\n" + Colors.RESET)
    command = "./build-target/bin/flink run "
    for t in testListDistributed:
        os.system("./scripts/removeLogs.sh")
        restartCluster()
        fullCommand = command + " " + t
        print(fullCommand)
        os.system(fullCommand)

def printHelp():
    print("\nFlink-TornadoVM Runner Tool")
    print("\nUsage: flink-tornado [test]\n")
    print("Commands available:")
    print("\ttest : run all tests locally and via the Flink Cluster")
    print("\tlocal: run all tests locally")
    print("\tdist : run all tests via the Flink Cluster")

def parseArguments():
    """ Parse command line arguments """ 
    c = Command.DEFAULT
    option = None

    if (len(sys.argv) > 1) :
        option = sys.argv[1]
    if (option != None):
        if (option.startswith("test")):
            return Command.TEST
        if (option.startswith("local")):
            return Command.LOCAL
        if (option.startswith("dist")):
            return Command.DIST
        printHelp()
        sys.exit(0)
    else:
        printHelp()
        sys.exit(0)
    return c

if __name__ == "__main__":
    command = parseArguments()

    if (command == Command.TEST or command == Command.LOCAL):
        runTestsLocal(command)  
    if (command == Command.TEST or command == Command.DIST):
        runTestDistributed(command)   
    
