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

    recreate_t1t2disk=True
    if recreate_t1t2disk:
        phedex_info = csvreader.load("/user/dlange/working_set_day/phedex")
        tmp = fn.split(fn.split(col("dataset_name"), '/').getItem(2), '-')
        tmp2 = fn.concat(tmp.getItem(0),fn.lit('-'),tmp.getItem(fn.size(tmp)-2))#,fn.lit('-'),tmp.getItem(fn.size(tmp)-1))
#        tmp3 = 
        phedex_info = (
            phedex_info
            #for debug..
#            .limit(100)
            .filter(col("node_name").endswith("MSS"))
            #.filter(col("node_name").endswith("Buffer")) #export
            .filter(col("max_time") > fn.lit(201120) )
            .withColumn( "tmp", tmp2 )
            .withColumn( "input_campaign", fn.when(col("tmp").endswith("_ext1"), fn.expr("substring(tmp,0,length(tmp)-5)")).otherwise(col("tmp")))
#                                                   fn.substring(col("tmp"),0,fn.size(col("tmp"))-5)).otherwise(col("tmp")))
#                fn.regexp_extract(
#                    col("dataset_name"),
#                    r"^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/",
#                    1,
#                ),
            #)
            .withColumn("tier",fn.split(col("dataset_name"), '/').getItem(3))
        )
#        phedex_info.write.csv(args.out+"/phedex_tapes",header=True,mode="overwrite")
    else:
        phedex_info = csvreader.load("/user/dlange/working_set_day/phedex_tapes")
    phedex_info.show(10, False)
    print(phedex_info.count())
    
    phedex_df = (
        phedex_info
        .groupBy("tier","input_campaign")
        .agg(fn.sum("rep_size")).alias("size")
    )

    phedex_df.show(100,False)
    print(phedex_df.count())
    phedex_df.write.csv(args.out+"/phedex_tapes",header=True,mode="overwrite")


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
