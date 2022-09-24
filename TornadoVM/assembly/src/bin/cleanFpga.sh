#!/usr/bin/env bash

if [ "$#" -ne 2 ]
then
	echo "$0: invalid number of arguments."
	exit 1
fi

vendor=$1
fpga_directory=$2
BINARY="lookupBufferAddress"

## Back up current kernel 
mv $fpga_directory/lookupBufferAddress.cl $fpga_directory/backup_source.cl

## Move current generated directory
mv $fpga_directory/lookupBufferAddress $fpga_directory/intelFPGAFiles

## Create sym link to the original kernel
current=`pwd`
cd $fpga_directory

if [ $vendor = "intel" ]
then
	ln -s $BINARY.aocx $BINARY 
elif [ $vendor = "xilinx" ]
then
	ln -s $BINARY.xclbin $BINARY
else
	echo "$0: FPGA vendor is not supported yet."
fi

cd $current
