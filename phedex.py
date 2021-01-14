#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types

# stolen from CMSSpark
import schemas

def get_df_phedex(spark, dates):
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField("file_lfn", types.StringType(), True),
                        types.StructField("end_time", types.LongType(), True),
                        types.StructField("app_info", types.StringType(), True),
                        types.StructField("site_name", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/cmssw_pop/raw/metric/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df.show(5)
    df = (
        df.filter(df.file_lfn.startswith("/store/"))
        .withColumn("is_crab", fn.when(df.app_info.contains(":crab:"),fn.lit("1")).otherwise(fn.lit("0")))
        .drop("app_info")
        .withColumnRenamed("end_time", "timestamp")
    )
    df.show(5)
    return df


def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS PHedex tally")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST")
    )
    phedex_info = csvreader.schema(schemas.schema_phedex()).load(
        "/project/awg/cms/phedex/block-replicas-snapshots/csv/time={2019,2020}-*-*"
    )

    phedex_info.show(15)

    phedex_df = (
        phedex_info
        .groupBy("block_id","replica_time_create","node_name","br_user_group_id","dataset_name","dataset_id")
        .agg(fn.max("now_sec").alias("max_time"),
             fn.min("now_sec").alias("min_time"),
             fn.avg("block_bytes").alias("block_rep_size")
         )
        .withColumn('min_time',fn.date_format(fn.from_unixtime(col('min_time').cast(types.LongType())),'yyyyMMdd'))
        .withColumn('max_time',fn.date_format(fn.from_unixtime(col('max_time').cast(types.LongType())),'yyyyMMdd'))
        .withColumn('replica_time_create',fn.date_format(fn.from_unixtime(col('replica_time_create').cast(types.LongType())),'yyyyMMdd'))
        .groupBy("dataset_name","dataset_id","replica_time_create","node_name","max_time","min_time","br_user_group_id")
        .agg(fn.sum("block_rep_size").alias("rep_size"))
        .select("dataset_name","dataset_id", "replica_time_create","node_name","rep_size","min_time","max_time","br_user_group_id")
    )
    phedex_df.show(35)

    phedex_df.write.option("compression","snappy").parquet(args.out+"/phedex",mode="overwrite")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Computes working set (unique blocks accessed) per day, partitioned by various fields."
    )
    defpath = "hdfs://analytix/user/dlange/working_set_day"
    parser.add_argument(
        "--out",
        metavar="OUTPUT",
        help="Output path in HDFS for result (default: %s)" % defpath,
        default=defpath,
    )

#ignored for now
#    parser.add_argument("--dates", help="Date portion of file path", default="2020/*/*")

    args = parser.parse_args()
    run(args)
