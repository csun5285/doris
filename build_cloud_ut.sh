#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#####################################################################
# This script is used to run unit test of Doris Backend
# Usage: $0 <options>
#  Optional options:
#     --clean            clean and build ut
#     --run              build and run all ut
#     --run --filter=xx  build and run specified ut
#     -j                 build parallel
#     -h                 print this help message
#
# All BE tests must use "_test" as the file suffix, and add the file
# to cloud/test/CMakeLists.txt.
#
# GTest result xml files will be in "cloud/ut_build_ASAN/gtest_output/"
#####################################################################

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean            clean and build ut
     --run              build and run all ut
     --run --filter=xx  build and run specified ut
     -j                 build parallel
     -h                 print this help message

  Eg.
    $0                                                              build tests
    $0 --run                                                        build and run all tests
    $0 --run --filter=*                                             also runs everything
    $0 --run --filter=FooTest.*                                     runs everything in test suite FooTest
    $0 --run --filter=*Null*:*Constructor*                          runs any test whose full name contains either 'Null' or 'Constructor'
    $0 --run --filter=-*DeathTest.*                                 runs all non-death tests
    $0 --run --filter=FooTest.*-FooTest.Bar                         runs everything in test suite FooTest except FooTest.Bar
    $0 --run --filter=FooTest.*:BarTest.*-FooTest.Bar:BarTest.Foo   runs everything in test suite FooTest except FooTest.Bar and everything in test suite BarTest except BarTest.Foo
    $0 --clean                                                      clean and build tests
    $0 --clean --run                                                clean, build and run all tests
  "
  exit 1
}

OPTS=$(getopt  -n $0 -o vhj:f: -l run,clean,filter: -- "$@")
if [ "$?" != "0" ]; then
  usage
fi

set -eo pipefail

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/5+1]

CLEAN=0
RUN=0
BUILD_BENCHMARK_TOOL=OFF
FILTER=""
if [ $# != 1 ] ; then
    while true; do 
        case "$1" in
            --clean) CLEAN=1 ; shift ;;
            --run) RUN=1 ; shift ;;
            -f | --filter) FILTER="--gtest_filter=$2"; shift 2;;
            -j) PARALLEL=$2; shift 2 ;;
            --) shift ;  break ;;
            *) usage ; exit 0 ;;
        esac
    done
fi

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE^^}"

echo "Get params:
    PARALLEL            -- $PARALLEL
    CLEAN               -- $CLEAN
"
echo "Build SelectDB Cloud UT"

. ${DORIS_HOME}/env.sh

CMAKE_BUILD_DIR=${DORIS_HOME}/cloud/ut_build_${CMAKE_BUILD_TYPE}
if [ ${CLEAN} -eq 1 ]; then
    rm ${CMAKE_BUILD_DIR} -rf
    rm ${DORIS_HOME}/cloud/output/ -rf
fi

if [ ! -d ${CMAKE_BUILD_DIR} ]; then
    mkdir -p ${CMAKE_BUILD_DIR}
fi

if [[ -z ${GLIBC_COMPATIBILITY} ]]; then
    GLIBC_COMPATIBILITY=ON
fi

if [[ -z ${USE_DWARF} ]]; then
    USE_DWARF=OFF
fi

MAKE_PROGRAM="$(which "${BUILD_SYSTEM}")"
echo "-- Make program: ${MAKE_PROGRAM}"

cd ${CMAKE_BUILD_DIR}
${CMAKE_CMD} -G "${GENERATOR}" \
    -DCMAKE_MAKE_PROGRAM="${MAKE_PROGRAM}" \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DMAKE_TEST=ON \
    -DGLIBC_COMPATIBILITY="${GLIBC_COMPATIBILITY}" \
    -DUSE_DWARF=${USE_DWARF} \
    -DUSE_MEM_TRACKER=ON \
    -DUSE_JEMALLOC=OFF \
    -DSTRICT_MEMORY_USE=OFF \
    ${CMAKE_USE_CCACHE} ${DORIS_HOME}/cloud/
${BUILD_SYSTEM} -j ${PARALLEL}

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

# TODO
# echo "******************************"
# echo "   Running Backend Unit Test  "
# echo "******************************"
