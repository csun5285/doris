#!/bin/bash

for i in `ls *_test`; do
	if [ "$1" != "" ]; then
		if [ "$1" != "${i}" ]; then
			continue;
		fi
	fi
	if [ -x ${i} ]; then
		echo "========== ${i} =========="
		fdb=`ldd ${i} | grep libfdb_c`
		if [ "${fdb}" != "" ]; then
			patchelf --set-rpath `pwd` ${i}
			patchelf --set-interpreter `pwd`/ld-linux-x86-64.so.2 ${i}
		fi
		./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml $@
		echo "--------------------------"
	fi
done
