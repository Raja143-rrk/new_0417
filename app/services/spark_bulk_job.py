import argparse
import json
import os
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser(description="Spark bulk extraction job")
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-driver", required=True)
    parser.add_argument("--jdbc-jar", required=True)
    parser.add_argument("--source-user", required=True)
    parser.add_argument("--source-password", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--jdbc-fetch-size", required=True)
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--output-partitions", type=int, default=1)
    parser.add_argument("--partition-column")
    parser.add_argument("--lower-bound")
    parser.add_argument("--upper-bound")
    parser.add_argument("--num-partitions", type=int)
    parser.add_argument("--count-rows", action="store_true")
    return parser.parse_args()


def _write_result_file(payload):
    result_file = os.getenv("DBM_SPARK_RESULT_FILE")
    if result_file:
        Path(result_file).write_text(json.dumps(payload), encoding="utf-8")


def main():
    args = _parse_args()
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise SystemExit(f"pyspark is not installed: {exc}")

    spark = (
        SparkSession.builder.appName(f"dbm-bulk-{args.table_name}")
        .master(args.spark_master)
        .config("spark.jars", args.jdbc_jar)
        .config("spark.driver.extraClassPath", args.jdbc_jar)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    row_count = None
    try:
        reader = (
            spark.read.format("jdbc")
            .option("url", args.jdbc_url)
            .option("dbtable", f"(SELECT * FROM {args.table_name}) AS dbm_src")
            .option("user", args.source_user)
            .option("password", args.source_password)
            .option("driver", args.jdbc_driver)
            .option("fetchsize", args.jdbc_fetch_size)
        )
        if (
            args.partition_column
            and args.lower_bound is not None
            and args.upper_bound is not None
            and args.num_partitions
        ):
            reader = (
                reader.option("partitionColumn", args.partition_column)
                .option("lowerBound", args.lower_bound)
                .option("upperBound", args.upper_bound)
                .option("numPartitions", args.num_partitions)
            )

        df = reader.load()
        if args.count_rows:
            row_count = int(df.count())

        df.coalesce(max(1, args.output_partitions)).write.mode("overwrite").option("header", "true").csv(args.output_dir)
        payload = {
            "status": "success",
            "output_dir": args.output_dir,
            "row_count": row_count,
            "partition_column": args.partition_column,
            "num_partitions": args.num_partitions,
        }
        _write_result_file(payload)
        print(json.dumps(payload))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
