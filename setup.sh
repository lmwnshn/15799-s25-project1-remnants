#!/usr/bin/env bash

set -euxo pipefail

mkdir -p artifact
mkdir -p data

git submodule update --init --recursive

cd ./substrait-java
./gradlew build -x test
./gradlew nativeImage
cd -

cd ./duckdb-substrait-extension
make -j4
cd -

# Load TPC-H into DuckDB.
./duckdb-substrait-extension/build/release/duckdb data/tpch.db -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf = 1);"
# Export TPC-H into Parquet for DataFusion.
./duckdb-substrait-extension/build/release/duckdb data/tpch.db -c "INSTALL tpch; LOAD tpch; EXPORT DATABASE './data/' (FORMAT PARQUET);"

# Fix the Parquet files for compatibility.
python3 ./fix_parquet.py

# Generate Substrait plans.
./run_substrait.sh
# Run the Substrait plans.
python3 ./run_duckdb.py
python3 ./run_datafusion.py

# # Extract IMDB data for JOB.
# cd ./join-order-benchmark-copy/raw_data
# cat imdb.tgz.part* > imdb.tgz
# cd -
# mkdir -p ./join-order-benchmark-copy/imdb_data
# cd ./join-order-benchmark-copy/imdb_data
# tar xzvf ../raw_data/imdb.tgz
# cd -

# # Load JOB into DuckDB.
# ./duckdb-substrait-extension/build/release/duckdb job.db -f ./join-order-benchmark-copy/queries/schema.sql
# ./duckdb-substrait-extension/build/release/duckdb job.db -f ./copy_job_csv.sql
# ./duckdb-substrait-extension/build/release/duckdb job.db -f ./join-order-benchmark-copy/queries/fkindexes.sql
