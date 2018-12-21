#!/bin/sh
fn=$(date "+%Y%m%d%H%M%S")".log"
for i in $(seq 1 $2)
do
	echo $i
	go test -run $1 >> $fn
	echo "=======================" >> $fn
done
