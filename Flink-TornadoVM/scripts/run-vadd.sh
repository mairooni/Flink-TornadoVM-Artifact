#!/bin/bash

#for j in 262144 524288 1048576 2097152 4194304 #8388608 16777216 33554432
#for j in 16777216
#do
	echo "SIZE: "$1
	./bin/start-cluster.sh

	for i in {1..10} 
	do
		echo $i"# RUN"
		./bin/flink run examples/batch/TestAddTwoVectors.jar --size $1 --parallelism $2 #>> "/home/maryxek/vadd-results/"$2"/res_"$1".txt"
	done
	./bin/stop-cluster.sh
#done
