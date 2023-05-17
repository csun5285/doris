#!/bin/bash

echo "input params: $@"

function usage() {
  echo "$0 [--fdb <fdb_conf>] [--test <test_binary>] [--filter <gtest_filter>]"
}
OPTS=$(getopt  -n $0 -o a:b:c: -l test:,fdb:,filter:,coverage -- "$@")
if [ "$?" != "0" ]; then
  usage
  exit 1
fi
set -eo pipefail
eval set -- "$OPTS"

test=""
fdb_conf=""
filter=""
ENABLE_CLANG_COVERAGE="OFF"
if [ $# != 1 ] ; then
  while true; do 
    case "$1" in
      --coverage) ENABLE_CLANG_COVERAGE="ON"; shift 1;;
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
# unset ASAN_OPTIONS

if [[ "${fdb_conf}" != "" ]]; then
  echo "update fdb_cluster.conf with \"${fdb_conf}\""
  echo "${fdb_conf}" > fdb.cluster
fi

# report converage for unittest
# input param is unittest binary file list
function report_coverage()
{
  local binary_objects=$1
  local profdata="./report/selectdb_cloud.profdata"
  local profraw=$(ls ./report/*.profraw)
  local binary_objects_options=()
  for object in ${binary_objects[@]}; do
    binary_objects_options[${#binary_objects_options[*]}]="-object ${object}"
  done
  llvm-profdata merge -o ${profdata} ${profraw}
  llvm-cov show -output-dir=report -format=html \
      -ignore-filename-regex='(.*gensrc/.*)|(.*_test\.cpp$)' \
      -instr-profile=${profdata} \
      ${binary_objects_options[*]}
}

export LSAN_OPTIONS=suppressions=./lsan_suppression.conf
unittest_files=()
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
        LLVM_PROFILE_FILE="./report/${i}.profraw" ./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml
    else
        LLVM_PROFILE_FILE="./report/${i}.profraw" ./${i} --gtest_print_time=true --gtest_output=xml:${i}.xml --gtest_filter=${filter}
    fi
    unittest_files[${#unittest_files[*]}]="${i}"
    echo "--------------------------"
  fi
done

if [[ "_${ENABLE_CLANG_COVERAGE}" == "_ON" ]];then
  report_coverage "${unittest_files[*]}"
fi

# vim: et ts=2 sw=2:
