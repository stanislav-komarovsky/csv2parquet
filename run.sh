#!/usr/bin/env bash
/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit \
--class "com.tekcomms.spark.utils.csv2parquet.Csv2Parquet" \
--packages com.databricks:spark-csv_2.10:1.1.0 \
--master local[4] target/csv2parquet-0.0.1-SNAPSHOT.jar \
S6A_LR28_15_USER_CTO-20150719-WE-87.csv
