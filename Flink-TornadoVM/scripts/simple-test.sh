#!/usr/bin/env bash

tornado --igv -Dtornado.ignore.nullchecks=True -cp build-target/lib/flink-dist_2.11-1.11.1.jar org.apache.flink.examples.java.tornadovm.TestCopyIntegers
