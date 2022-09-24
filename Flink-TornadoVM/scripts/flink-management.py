#!/usr/bin/env python

import sys
import os
from enum import Enum

class Command(Enum):
    DEFAULT = 0
    START = 1
    STOP = 2
    RESTART = 3
    DEPLOY_TORNADO = 4

def runCommands(command):

    if (command == Command.START):
        os.system("./build-target/bin/start-cluster.sh")

    elif (command == Command.STOP):
        os.system("./build-target/bin/stop-cluster.sh")

    elif (command == Command.RESTART):
        os.system("./build-target/bin/stop-cluster.sh")
        os.system("./build-target/bin/start-cluster.sh")

    elif (command == Command.DEPLOY_TORNADO):
        tornadoVMRoot = os.environ["TORNADO_ROOT"]
        if (tornadoVMRoot != None):

            #command = "mvn dependency:copy-dependencies -DoutputDirectory=/tmp/FLINK"
            #os.system(command)

            command = "rm -Rf build-target/lib/tornado" 
            os.system(command)

            command = "cp -R " + tornadoVMRoot + "/dist/tornado-sdk/tornado-sdk-*/ build-target/lib/tornado"
            os.system(command)

            print("TornadoVM copied into the Flink distribution ................. [OK]")
        
            print("""

            Next steps:

            1. Export the following variables

            export TORNADO_ROOT=<path/to/tornado/install>
            export TORNADO_BASE=<path/to/>flink-tornado-internal/build-target/lib/tornado/
            export PATH="${PATH}:${TORNADO_BASE}/bin/"
            export EXTERNAL_DIRS=/tmp/FLINK:$EXTERNAL_DIRS
            export TORNADO_SDK=${TORNADO_BASE}


            2. Execute `tornado --printFlags` and copy all flags plus 
            -Dtornado.ignore.nullchecks=True
            

            3. Then add the following variables in the flink-conf.yaml located in <path/to/>flink-tornado-internal/build-target/conf/flink-conf.yaml:

            ## Set the Java Home
            env.java.home: <path/to/java/>

            ## Set the JVM arguments
            env.java.opts: 


            4. Start the Flink-cluster
            Then run: `./scripts/flink-management start`


            5. Run an experiment:

            `./build-target/bin/flink run build-target/examples/batch/Arrays.jar`


            """)
        else:
            print("[ERROR] Please, export TORNADO_ROOT variable to your TornadoVM installation")


def printHelp():
    print("\nFlink-TornadoVM Management Tool")
    print("\nUsage: flink-management [start|stop|restart|deploy-tornado]\n")
    print("Commands available:")
    print("\tstart: start flink-servers")
    print("\tstop: stop all flink-servers")
    print("\trestart: restart all flink-servers")
    print("\tdeploy-tornado: configure Flink to use TornadoVM\n\n")


def parseArguments():
    """ Parse command line arguments """ 
    c = Command.DEFAULT
    option = None

    if (len(sys.argv) > 1) :
        option = sys.argv[1]

    if (option != None):
        if (option == "start"):
            c = Command.START
        elif (option == "stop"):
            c = Command.STOP
        elif (option == "restart"):
            c = Command.RESTART
        elif (option.startswith("deploy")):
            c = Command.DEPLOY_TORNADO
        elif (option == "help"):
            printHelp()
            sys.exit(0)
    else:
        printHelp()
        sys.exit(0)
    
    return c

if __name__ == "__main__":
    command = parseArguments()
    runCommands(command)

    

