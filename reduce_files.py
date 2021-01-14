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
    print(args.basedir)
    print(args.dir)
    print(args.nfiles)
    parquetreader = (
        spark.read.format("parquet").option("nullValue", "null").option("mode", "FAILFAST")
    )

    df = parquetreader.load(args.basedir+'/'+args.dir)#"/user/dlange/working_set_day/phedex_t1t2disk")
    df.show(10,False)
    if df.count() < 100:
        print("Likely a problem reading??")
        return
    df.repartition(int(args.nfiles)).write.option("compression","snappy").parquet(args.basedir+"/"+args.dir+"_group",mode="overwrite")

    return




#def isAnaOps(keyInfo):
#    if keyInfo[2]=="42": return True
#    return False
#def isAllOps(keyInfo):
#    if keyInfo[2]=="42": return True
#    if keyInfo[2]=="18": return True
#    if keyInfo[2]=="19": return True
#    return False
#
#    phedex_


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Computes working set (unique blocks accessed) per day, partitioned by various fields."
    )
    defpath = "hdfs://analytix/user/dlange/working_set_day"
    parser.add_argument(
        "--basedir",
        help="Base path in HDFS for result (default: %s)" % defpath,
        default=defpath
    )

    parser.add_argument(
        "--dir",
        help="end of path in HDFS for result", 
        required=True
    )

    parser.add_argument(
        "--nfiles",
        help="how many files to write",
        required=True
    )

    args = parser.parse_args()
    run(args)
