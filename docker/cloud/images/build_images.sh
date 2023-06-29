#!/bin/bash

USE_AVX=0
BRANCH_NAME=selectdb-cloud-dev
REPO_URL=git@github.com:selectdb/selectdb-core.git
while [[ $# > 0 ]]; do
    cmd=$1
    shift
    case $cmd in
        --branch);
            BRANCH_NAME=$1
            shift
            ;;
        --use-avx2)
            USE_AVX=1
            ;;
        --repo)
            REPO_URL=$1
            shift
            ;;
        *);
            echo "unknown cmd $cmd"
            exit 1
            ;;
    esac
done

rm -rf selectdb-core
git clone --depth=1 --branch ${BRANCH_NAME} ${REPO_URL} selectdb-core

docker build \
    --progress=plain \
    --target selectdb-base \
    -t selectdb-base \
    .

docker build \
    --build-arg SELECTDB_CORE_URL=./selectdb-core \
    --build-arg USE_AVX2=${USE_AVX} \
    --progress=plain \
    --target selectdb-output \
    -t selectdb-output \
    .

docker build \
    --build-arg SELECTDB_CORE_URL=./selectdb-core \
    --build-arg USE_AVX2=${USE_AVX} \
    --progress=plain \
    --target selectdb-fe \
    -t selectdb-fe \
    .

docker build \
    --build-arg SELECTDB_CORE_URL=./selectdb-core \
    --build-arg USE_AVX2=${USE_AVX} \
    --progress=plain \
    --target selectdb-be \
    -t selectdb-be \
    .

docker build \
    --build-arg SELECTDB_CORE_URL=./selectdb-core \
    --build-arg USE_AVX2=${USE_AVX} \
    --progress=plain \
    --target selectdb-ms \
    -t selectdb-ms \
    .

docker build \
    --build-arg SELECTDB_CORE_URL=./selectdb-core \
    --build-arg USE_AVX2=${USE_AVX} \
    --progress=plain \
    --target selectdb-recycler \
    -t selectdb-recycler \
    .

