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


    debug_dataset="/BsToMuMu_SoftQCDnonD_TuneCP5_13TeV-pythia8-evtgen/RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v1/MINIAODSIM"

    merged_reads_df = parquetreader.load("/user/dlange/working_set_day/merged_reads")
    merged_reads_df = (
        merged_reads_df
        .withColumn("day",fn.to_date(col("day"),"yyyyMMdd"))
        .select("day","d_dataset_id","fract_read")
    )
    merged_reads_df.show(15,False)

    dates_df=spark.sql("SELECT sequence(to_date('2019-01-01'), to_date('2021-01-01'), interval 1 day) as date").withColumn("date", fn.explode(col("date")))

    repart_num=400

    phedex_df = parquetreader.load("/user/dlange/working_set_day/phedex_daily").repartition(repart_num,'date')
    phedex_df.show(15)

    total_data = (
        phedex_df
        .groupBy(col('date'))
        .agg(fn.sum(col("allOpsSize")).alias('size'))
    )
    print("total_data size")
    print(total_data.count())
    total_data.show(15)

    windows=[30,90,180]#[30]#,90,180]
    need_to_fix_memory=True
    if need_to_fix_memory:
        old_val = spark.conf.get("spark.sql.broadcastTimeout")
        old_val2 = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        print(old_val)
        print(old_val2)
        spark.conf.set("spark.sql.broadcastTimeout" ,"-1")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold" ,"-1")

    for w in windows:
        print("Doing window "+str(w))
        df = (
            merged_reads_df
            .crossJoin(dates_df)
            .filter( col('date').between(col('day'),fn.date_add(col('day'),w)))
            .select("date","d_dataset_id")
            .drop_duplicates()
#            .groupBy( col('date'), col('dataset_id'))
        ).repartition(repart_num,'date')
        df.show(15,False)
#        print(df.count())
#        print(phedex_df.count())
 
        read_data = (
            phedex_df
            .join(df, (df.date == phedex_df.date) & (df.d_dataset_id == phedex_df.dataset_id) )
            .groupBy(df.date)
            .agg(fn.sum(col("allOpsSize")).alias('size'))
        )
        read_data.show(15,False)

        read_data= (
            read_data
            .join(total_data, read_data.date == total_data.date)
            .withColumn("fraction",read_data.size / total_data.size )
            .select(total_data.date,"fraction",total_data.size)
        )

        read_data.show(15,False)

        read_data.repartition(1).write.csv(args.out+"/rolling_popularity/"+str(w),header=True,mode="overwrite")

               
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
