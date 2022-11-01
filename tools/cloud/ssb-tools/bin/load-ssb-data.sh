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

##############################################################
# This script is used to load generated ssb data set to Doris
# Only for 1 fact table: lineorder
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
SSB_DATA_DIR="${CURDIR}/ssb-data/"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
    -c             parallelism to load data of lineorder table, default is 5.

  Eg.
    $0              load data using default value.
    $0 -c 10        load lineorder table data using parallelism 10.
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hc:' \
    -- "$@")

eval set -- "${OPTS}"

PARALLEL=5
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -c)
        PARALLEL=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Parallelism: ${PARALLEL}"

# check if ssb-data exists
if [[ ! -d ${SSB_DATA_DIR}/ ]]; then
    echo "${SSB_DATA_DIR} does not exist. Run sh gen-ssb-data.sh first."
    exit 1
fi

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD}; then
        echo "${NAME} is missing. This script depends on cURL to load data to Doris."
        exit 1
    fi
}

run_sql() {
    sql="$*"
    echo "${sql}"
    mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" -D"${DB}" -e "$@"
}

load_lineitem_flat() {
    # Loading data in batches by year.
    for con in 'lo_orderdate<19930101' 'lo_orderdate>=19930101 and lo_orderdate<19940101' 'lo_orderdate>=19940101 and lo_orderdate<19950101' 'lo_orderdate>=19950101 and lo_orderdate<19960101' 'lo_orderdate>=19960101 and lo_orderdate<19970101' 'lo_orderdate>=19970101 and lo_orderdate<19980101' 'lo_orderdate>=19980101'; do
        echo -e "\n${con}"
        run_sql "
INSERT INTO lineorder_flat
SELECT
    LO_ORDERDATE,
    LO_ORDERKEY,
    LO_LINENUMBER,
    LO_CUSTKEY,
    LO_PARTKEY,
    LO_SUPPKEY,
    LO_ORDERPRIORITY,
    LO_SHIPPRIORITY,
    LO_QUANTITY,
    LO_EXTENDEDPRICE,
    LO_ORDTOTALPRICE,
    LO_DISCOUNT,
    LO_REVENUE,
    LO_SUPPLYCOST,
    LO_TAX,
    LO_COMMITDATE,
    LO_SHIPMODE,
    C_NAME,
    C_ADDRESS,
    C_CITY,
    C_NATION,
    C_REGION,
    C_PHONE,
    C_MKTSEGMENT,
    S_NAME,
    S_ADDRESS,
    S_CITY,
    S_NATION,
    S_REGION,
    S_PHONE,
    P_NAME,
    P_MFGR,
    P_CATEGORY,
    P_BRAND,
    P_COLOR,
    P_TYPE,
    P_SIZE,
    P_CONTAINER
FROM (
    SELECT
        lo_orderkey,
        lo_linenumber,
        lo_custkey,
        lo_partkey,
        lo_suppkey,
        lo_orderdate,
        lo_orderpriority,
        lo_shippriority,
        lo_quantity,
        lo_extendedprice,
        lo_ordtotalprice,
        lo_discount,
        lo_revenue,
        lo_supplycost,
        lo_tax,
        lo_commitdate,
        lo_shipmode
    FROM lineorder
    WHERE ${con}
) l
INNER JOIN customer c
ON (c.c_custkey = l.lo_custkey)
INNER JOIN supplier s
ON (s.s_suppkey = l.lo_suppkey)
INNER JOIN part p
ON (p.p_partkey = l.lo_partkey);
"
    done
}

check_prerequest "curl --version" "curl"

# load lineorder
source "${CURDIR}/../conf/selectdb-cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "HOST: ${HOST}"
echo "HTTP_PORT: ${HTTP_PORT}"
echo "QUERY_PORT: ${QUERY_PORT}"
echo "USER: ${USER}"
echo "PASSWORD: ${PASSWORD}"

date
echo "==========Start to load data into ssb tables=========="
echo 'Loading data for table: part'
curl -u "${USER}":"${PASSWORD}" \
    -H "fileName:part.tbl" \
    -T "${SSB_DATA_DIR}"/part.tbl \
    -L http://"${HOST}":"${HTTP_PORT}"/copy/upload

mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" \
    -e "COPY INTO ${DB}.part FROM @~(\"part.tbl\") PROPERTIES (\"copy.async\"=\"false\",\"file.type\"=\"csv\",\"file.column_separator\"=\"|\");"

echo 'Loading data for table: date'
curl -u "${USER}":"${PASSWORD}" \
    -H "fileName:date.tbl" \
    -T "${SSB_DATA_DIR}"/date.tbl \
    -L http://"${HOST}":"${HTTP_PORT}"/copy/upload

mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" \
    -e "COPY INTO ${DB}.dates FROM @~(\"date.tbl\") PROPERTIES (\"copy.async\"=\"false\",\"file.type\"=\"csv\",\"file.column_separator\"=\"|\");"

echo 'Loading data for table: supplier'
curl -u "${USER}":"${PASSWORD}" \
    -H "fileName:supplier.tbl" \
    -T "${SSB_DATA_DIR}"/supplier.tbl \
    -L http://"${HOST}":"${HTTP_PORT}"/copy/upload

mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" \
    -e "COPY INTO ${DB}.supplier FROM @~(\"supplier.tbl\") PROPERTIES (\"copy.async\"=\"false\",\"file.type\"=\"csv\",\"file.column_separator\"=\"|\");"

echo 'Loading data for table: customer'
curl -u "${USER}":"${PASSWORD}" \
    -H "fileName:customer.tbl" \
    -T "${SSB_DATA_DIR}"/customer.tbl \
    -L http://"${HOST}":"${HTTP_PORT}"/copy/upload

mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" \
    -e "COPY INTO ${DB}.customer FROM @~(\"customer.tbl\") PROPERTIES (\"copy.async\"=\"false\",\"file.type\"=\"csv\",\"file.column_separator\"=\"|\");"

echo "Loading data for table: lineorder, with ${PARALLEL} parallel"
function load() {
    echo "$*"
    curl -u "${USER}":"${PASSWORD}" \
        -H "fileName:$(basename $*)" \
        -T "$*" \
        -L http://"${HOST}":"${HTTP_PORT}"/copy/upload

    mysql -h"${HOST}" -u"${USER}" -P"${QUERY_PORT}" \
        -e "COPY INTO ${DB}.lineorder FROM @~(\"$(basename $*)\") PROPERTIES (\"copy.async\"=\"false\",\"file.type\"=\"csv\",\"file.column_separator\"=\"|\");"
}

# set parallelism
[[ -e /tmp/fd1 ]] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1

for ((i = 1; i <= PARALLEL; i++)); do
    echo >&3
done

date
for file in "${SSB_DATA_DIR}"/lineorder.tbl.*; do
    read -r -u3
    {
        load "${file}"
        echo >&3
    } &
done

# wait for child thread finished
wait
date

echo "==========Start to insert data into ssb flat table=========="
echo "change some session variables before load, and then restore after load."
origin_query_timeout=$(
    set -e
    run_sql 'select @@query_timeout;' | sed -n '3p'
)
origin_parallel=$(
    set -e
    run_sql 'select @@parallel_fragment_exec_instance_num;' | sed -n '3p'
)
# set parallel_fragment_exec_instance_num=1, loading maybe slow but stable.
run_sql "set global query_timeout=7200;"
run_sql "set global parallel_fragment_exec_instance_num=1;"
echo '============================================'
date
load_lineitem_flat
date
echo '============================================'
echo "restore session variables"
run_sql "set global query_timeout=${origin_query_timeout};"
run_sql "set global parallel_fragment_exec_instance_num=${origin_parallel};"
echo '============================================'

echo "DONE."
