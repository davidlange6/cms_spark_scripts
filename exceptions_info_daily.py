#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types

# stolen from CMSSpark
import schemas

def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS merge access info")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    spark.conf.set("spark.executor.instances", 4)
#    spark.conf.set("spark.executor.cores", 4)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    print("Spark info")
    print(sc._conf.get('spark.executor.instances'))
    print(sc._conf.get('spark.executor.cores'))
    print(sc._conf.get('spark.executor.memory'))


    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST").option("header",True)
    )

    parquetreader = (
        spark.read.format("parquet").option("nullValue", "null").option("mode", "FAILFAST")
    )

    jsonreader = (
        spark.read.format("json").option("nullValue", "null").option("mode", "FAILFAST")
    )

    exceptions = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/exceptions")
    exceptions.show(15)
    dates_df=spark.sql("SELECT sequence(to_date('2019-01-01'), to_date('2021-01-01'), interval 1 day) as date").withColumn("date", fn.explode(col("date")))

    phedex_df = parquetreader.load("/user/dlange/working_set_day/phedex_daily")#.repartition(repart_num,'date')
    phedex_df.show(15)

    total_data = (
        phedex_df
        .join(exceptions, phedex_df.dataset_id == exceptions.d_dataset_id)
        .groupBy(col('date'))
        .agg(fn.sum(col("allOpsSize")).alias('size'))
    )
    print("total_data size")
    print(total_data.count())
    total_data.show(15)

    total_data.repartition(1).write.csv(args.out+"/exceptions_daily",header=True,mode="overwrite")

               
    return


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

    parser.add_argument("--dates", help="Date portion of file path", default="2020/1*/*")

    args = parser.parse_args()
    run(args)
