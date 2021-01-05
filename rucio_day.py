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
import os    



def run(args):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.11:2.4.3 pyspark-shell'    
    spark_conf = SparkConf().setMaster("local").setAppName("app")\
                                               .set('spark.jars.packages', 'org.apache.spark:spark-avro_2.11:2.4.3')
    sc = SparkContext(conf=spark_conf)
    spark = SparkSession(sc)

#    conf = SparkConf().setMaster("yarn").setAppName("CMS Rucio tally").set('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:2.4.4')

#    sc = SparkContext(conf=conf)
#    spark = SparkSession(sc)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )


    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST")
    )

    avroreader = (
        spark.read.format("avro")#.option("nullValue", "null").option("mode", "FAILFAST")
    )


    rucio_info = avroreader.load(
        "/project/awg/cms/rucio/"+args.dates+"/replicas/part*.avro"
    ).withColumn("filename",fn.input_file_name())
    print(rucio_info.dtypes)
    rucio_info.show(15, False)

    dbs_files = csvreader.schema(schemas.schema_files()).load(
        "/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/FILES/part-m-00000"
    ).select("f_logical_file_name","f_dataset_id")
    dbs_files.show(15,False)

    rucio_df = (
        rucio_info
        .withColumn("tmp1",fn.substring_index("filename","/rucio/",-1))
        .withColumn("tally_date",fn.substring_index("tmp1","/",1))
        .withColumn('create_day', fn.date_format(fn.to_date((rucio_info.CREATED_AT / fn.lit(1000)).cast(types.LongType()).cast(types.TimestampType())),'yyyyMMdd'))
        .withColumn('tally_day', fn.date_format(fn.to_date("tally_date","yyyy-MM-dd"), 'yyyyMMdd'))
        .select("RSE_ID","BYTES","NAME","SCOPE","tally_day","create_day")
    )
    rucio_df.show(15,False)

    rucio_df = (
        rucio_df
        .join(dbs_files,dbs_files.f_logical_file_name==rucio_df.NAME)
        .groupBy("RSE_ID","f_dataset_id","SCOPE","tally_day","create_day")
        .agg(fn.sum("BYTES").alias("rep_size"))
    )
    rucio_df.show(15,False)
    
    sp=args.dates.split('-')
    label=""
    for s in sp:
        if "*" not in s: label=label+"/"+s

    rucio_df.write.option("compression","snappy").parquet(args.out+"/rucio/"+label,mode="overwrite")

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Processes rucio daily information and creates df at dataset level"
    )
    defpath = "hdfs://analytix/user/dlange/working_set_day"
    parser.add_argument(
        "--out",
        metavar="OUTPUT",
        help="Output path in HDFS for result (default: %s)" % defpath,
        default=defpath,
    )

    parser.add_argument("--dates", help="Date portion of file path (eg, 2020-12-11)", default="2020-12-11")

    args = parser.parse_args()
    run(args)
