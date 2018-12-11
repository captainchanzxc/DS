#!/bin/sh
for i in {1..$2}
do
       	fn=${(date,"+%Y%m%d%H%M%S")}".log"
	go test -run $1 >> $fn
	echo "========================" >> $fn
done
