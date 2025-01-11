#!/bin/bash

set -eu -o pipefail

ARTIFACT_DIR="./artifact"

CMD="./substrait-java/isthmus-cli/build/graal/isthmus"
TPCH="./substrait-java/isthmus/src/test/resources/tpch"
# JOB="./join-order-benchmark-copy"

DDL_TPCH=$(cat ${TPCH}/schema.sql)
# DDL_JOB=$(cat ${JOB}/queries/schema.sql)
QUERIES_TPCH="${TPCH}/queries"
# QUERIES_JOB="${JOB}/queries"

mkdir -p ${ARTIFACT_DIR}

# TPC-H.
TPCH_ARTIFACT_DIR="${ARTIFACT_DIR}/tpch"
mkdir -p ${TPCH_ARTIFACT_DIR}
QUERY_TO_RUN=(1 2 3 4 5 6 7 9 10 11 13 14 16 17 18 19 20 21 22)
for QUERY_NUM in "${QUERY_TO_RUN[@]}"; do
  if [ "${QUERY_NUM}" -lt 10 ]; then
    QUERY=$(cat "${QUERIES_TPCH}/0${QUERY_NUM}.sql")
    else
    QUERY=$(cat "${QUERIES_TPCH}/${QUERY_NUM}.sql")
  fi

  echo "Isthmus TPC-H: ${QUERY_NUM}"
  echo "${QUERY}"
  $CMD "${QUERY}" --create "${DDL_TPCH}" --outputformat=BINARY > "${TPCH_ARTIFACT_DIR}/${QUERY_NUM}.bin"
  $CMD "${QUERY}" --create "${DDL_TPCH}" --outputformat=PROTOJSON > "${TPCH_ARTIFACT_DIR}/${QUERY_NUM}.protojson"
  $CMD "${QUERY}" --create "${DDL_TPCH}" --outputformat=PROTOTEXT > "${TPCH_ARTIFACT_DIR}/${QUERY_NUM}.prototext"
done

# JOB.
# TODO(WAN): Seems like MIN(string) is not supported.
# JOB_ARTIFACT_DIR="${ARTIFACT_DIR}/job"
# mkdir -p ${JOB_ARTIFACT_DIR}
# for fname in $(ls -v ${QUERIES_JOB}); do
#   if [ "${fname}" == "README" ] || [ "${fname}" == "fkindexes.sql" ] || [ "${fname}" == "schema.sql" ]; then
#     continue
#   fi

#   query=$(cat "${QUERIES_JOB}/${fname}")
#   echo "Isthmus JOB: ${fname}"
#   echo "${query}"
#   $CMD "${query}" --create "${DDL_JOB}" --outputformat=BINARY > "${JOB_ARTIFACT_DIR}/${fname}.bin"
#   $CMD "${query}" --create "${DDL_JOB}" --outputformat=PROTOJSON > "${JOB_ARTIFACT_DIR}/${fname}.protojson"
#   $CMD "${query}" --create "${DDL_JOB}" --outputformat=PROTOTEXT > "${JOB_ARTIFACT_DIR}/${fname}.prototext"
# done
