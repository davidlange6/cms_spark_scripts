#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types

import schemas

def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS PHedex tally")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    dates_df=spark.sql("SELECT sequence(to_date('2019-01-01'), to_date('2021-01-01'), interval 1 day) as date").withColumn("date", fn.explode(col("date")))

    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST").option("header",True)
    )
    parquetreader = (
        spark.read.format("parquet").option("nullValue", "null").option("mode", "FAILFAST")
    )

#run phedex_windows to get this remade..
#    recreate_t1t2disk=False
#    if recreate_t1t2disk:
#        phedex_info = parquetreader.load("/user/dlange/working_set_day/phedex")
#        phedex_info = (
#            phedex_info
#            #for debug..
#            #.limit(100)
#            .filter(~col("node_name").startswith("T3"))
#            .filter(~col("node_name").startswith("X"))
#            .filter(~col("node_name").endswith("Buffer"))
#            .filter(~col("node_name").endswith("MSS"))
#            .filter(~col("node_name").endswith("Export"))
#            .withColumn("min_time",fn.to_date("min_time","yyyyMMdd"))
#            .withColumn("max_time",fn.to_date("max_time","yyyyMMdd"))
#        )
#        phedex_info.write.option("compression","snappy").parquet(args.out+"/phedex_t1t2disk",mode="overwrite")
#        #phedex_info.write.csv(args.out+"/phedex_t1t2disk",header=True,mode="overwrite")
#    else:
#        phedex_info = parquetreader.load("/user/dlange/working_set_day/phedex_t1t2disk")



    phedex_info = parquetreader.load("/user/dlange/working_set_day/phedex_t1t2disk")
    phedex_info.show(10, False)
    print(phedex_info.count())

#good for debugging
#    phedex_info = (
#        phedex_info
#        .filter(phedex_info.dataset_name=="/ZNuNuGJets_MonoPhoton_PtG-40to130_TuneCP5_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM")
#    )
#    phedex_info.show(15,False)
#    print(phedex_info.count())

    dbs_datasets = (
        csvreader.schema(schemas.schema_datasets())
        .load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")
        .withColumn(
            "input_campaign",
            fn.regexp_extract(
                col("d_dataset"),
                r"^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/",
                1,
            ),
        )
#        .filter(col("d_dataset")=="/ZNuNuGJets_MonoPhoton_PtG-40to130_TuneCP5_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM")
    )
    dbs_datasets.show(15,False)

    dbs_cols = dbs_datasets.columns
    dbs_cols.remove("d_dataset_id")
    phedex_info = (
        phedex_info
        .join(dbs_datasets, col("dataset_name")==col("d_dataset"))
        .drop(*dbs_cols)
        .drop("dataset_id")
        .withColumnRenamed("d_dataset_id","dataset_id")
        .withColumn("allOpsSize", fn.when( ((col("br_user_group_id")==42) | (col("br_user_group_id")==18) | (col("br_user_group_id")==19)), col("rep_size")).otherwise(0))
    )
    phedex_info.show(10, False)

    df = (
        phedex_info
        .crossJoin(dates_df)
        .filter( col('date').between(col('min_time'), col('max_time')))
        .groupBy( col('date'), col('dataset_id'))
        .agg(fn.sum("allOpsSize").alias("allOpsSize"))
    )

    print(phedex_info.count())
    print(df.count())
    df.show(20,False)

    df.write.sortBy('date').option("compression","snappy").parquet(args.out+"/phedex_daily",mode="overwrite")
# this should be useful..
#    df.write.bucketBy(200,'date').sortBy('date').option("compression","snappy").parquet(args.out+"/phedex_daily",mode="overwrite")
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

    args = parser.parse_args()
    run(args)
