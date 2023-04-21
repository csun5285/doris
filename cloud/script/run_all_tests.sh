#!/bin/bash

echo "input params: $@"

function usage() {
  echo "$0 [--fdb <fdb_conf>] [--test <test_binary>] [--filter <gtest_filter>]"
}
OPTS=$(getopt  -n $0 -o a:b:c: -l test:,fdb:,filter: -- "$@")
if [ "$?" != "0" ]; then
  usage
  exit 1
fi
set -eo pipefail
eval set -- "$OPTS"

test=""
fdb_conf=""
filter=""

if [ $# != 1 ] ; then
  while true; do 
    case "$1" in
      --test) test="$2"; shift 2;;
      --fdb) fdb_conf="$2"; shift 2;;
      --filter) filter="$2"; shift 2;;
      --) shift ;  break ;;
      *) usage ; exit 1 ;;
    esac
  done
fi
set +eo pipefail

echo "test=${test} fdb_conf=${fdb_conf} filter=${filter}"

# fdb memory leaks, we don't care the core dump of unit test
unset ASAN_OPTIONS

if [[ "${fdb_conf}" != "" ]]; then
  echo "update fdb_cluster.conf with \"${fdb_conf}\""
  echo "${fdb_conf}" > fdb.cluster
fi

for i in `ls *_test`; do
  if [ "${test}" != "" ]; then
    if [ "${test}" != "${i}" ]; then
      continue;
    fi
  fi
  if [ -x ${i} ]; then
    echo "========== ${i} =========="
    fdb=$(ldd ${i} | grep libfdb_c | grep found)
    if [ "${fdb}" != "" ]; then
      patchelf --set-rpath `pwd` ${i}
    fi
    if [ "${filter}" == "" ]; then
      ./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml
    else
      ./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml --gtest_filter=${filter}
    fi
    echo "--------------------------"
  fi
done

# vim: et ts=2 sw=2:
