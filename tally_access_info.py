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

def get_df_cmssw(spark, dates):
    """CMSSW UDP collector popularity
    Output:
    DataFrame[file_lfn: string, timestamp: bigint, site_name: string, is_crab: boolean]
    timestamp is UNIX timestamp (seconds)
    """
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

def get_df_condor(spark, dates):
    """Condor ClassAd popularity
    Output:
    DataFrame[dataset_name: string, site_name: string, CMS_SubmissionTool: string, timestamp: double]
    timestamp is a UNIX timestamp (seconds)
    """
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField(
                            "DESIRED_CMSDataset", types.StringType(), True
                        ),
                        types.StructField("RecordTime", types.LongType(), True),
                        types.StructField("KEvents", types.FloatType(), True),
                        types.StructField("Site", types.StringType(), True),
                        types.StructField("InputData", types.StringType(), True),
                        types.StructField(
                            "CMS_SubmissionTool", types.StringType(), True
                        ),
                        types.StructField("Status", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/condor/raw/metric/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df.show(5)
    df = (
        df.filter(df.DESIRED_CMSDataset.isNotNull() & (df.Status == "Completed"))
        .drop("Status")
        .withColumn('day', fn.date_format(fn.to_date((df.RecordTime / fn.lit(1000)).cast(types.LongType()).cast(types.TimestampType())),'yyyyMMdd'))
        .drop("RecordTime")
        .withColumnRenamed("DESIRED_CMSDataset", "dataset_name")
        .withColumn("site_name", fn.when(df.InputData == "Onsite",fn.col("Site")).otherwise(fn.lit("Remote")))
        .drop("Site")
    )
    df.show(5)
    return df

def get_df_xrootd(spark, dates):
    """XRootD GLED collector popularity
    Outputs:
    DataFrame[file_lfn: string, client_domain: string, timestamp: double]
    timestamp is UNIX timestamp (seconds)
    """
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField("file_lfn", types.StringType(), True),
                        types.StructField("end_time", types.LongType(), True),
                        types.StructField("client_domain", types.StringType(), True),
                        types.StructField("vo", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/xrootd/raw/gled/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df = (
        df.filter((df.vo == "cms") & df.file_lfn.startswith("/store/"))
        .drop("vo")
        .withColumn('day', fn.date_format(fn.to_date((df.end_time / fn.lit(1000)).cast(types.LongType()).cast(types.TimestampType())),'yyyyMMdd'))
#        .withColumn("timestamp", df.end_time / fn.lit(1000))
        .drop("end_time")
        .withColumn(
            "client_domain",
            fn.when(df.client_domain.endswith("]"), None).otherwise(df.client_domain),
        )
    )
    return df


def get_cmssw(spark, dbs_files, dbs_datasets, dbs_dataset_sizes, args):
    working_set_day = (
        get_df_cmssw(spark, args.dates)
        .withColumn('day', fn.date_format(fn.to_date((col("timestamp")).cast(types.LongType()).cast(types.TimestampType())),'yyyyMMdd'))
#        .withColumn('day', fn.from_unixtime(col("timestamp"), 'yyyyMMdd'))
#        .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
        .join(dbs_files, col("file_lfn") == col("f_logical_file_name"))
        .join(dbs_datasets, col("f_dataset_id") == col("d_dataset_id"))
        .groupBy("day", "d_dataset_id", "site_name", "is_crab")
        .agg(fn.sum("f_file_size").alias("size_read"), fn.count("*").alias("nfiles_read"))
        .join(dbs_dataset_sizes, dbs_datasets.d_dataset_id == dbs_dataset_sizes.d_dataset_id)
        .withColumn("fract_read",col("size_read")/col("dsize"))
        .select(dbs_datasets.d_dataset_id,col("day"),col("nfiles_read"),col("size_read"),col("fract_read"),col("site_name"))
#        .count()
    )
    return working_set_day

def get_classads(spark, dbs_files, dbs_datasets, dbs_dataset_sizes, args):
    working_set_day = (
        get_df_condor(spark, args.dates)
        .join(dbs_datasets, col("dataset_name") == col("d_dataset"))
        .groupBy("day", "d_dataset_id", "site_name")
        .agg(fn.sum("KEvents").alias("sum_evts"), 
             fn.count("*").alias("nrecs")
         )
#why?        .withColumn("sum_evts", fn.ceil(col("sum_evts")))
        .join(dbs_dataset_sizes, dbs_datasets.d_dataset_id == dbs_dataset_sizes.d_dataset_id)
        .withColumn("fract_read",1000*col("sum_evts")/(1+col("devts"))) #whoops its Kevts, not events!
        .select(dbs_datasets.d_dataset_id,col("day"),col("sum_evts"),col("nrecs"),col("site_name"),col("fract_read"))
    )
    return working_set_day

def get_xrootd(spark, dbs_files, dbs_datasets, dbs_dataset_sizes, args):
    working_set_day = (
        get_df_xrootd(spark, args.dates)
#        .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
        .join(dbs_files, col("file_lfn") == col("f_logical_file_name"))
        .join(dbs_datasets, col("f_dataset_id") == col("d_dataset_id"))
        .groupBy("day", "d_dataset_id",  "client_domain")
        .agg(fn.sum("f_file_size").alias("size_read"), 
             fn.count('*').alias("nfiles_read")
         )
        .join(dbs_dataset_sizes, dbs_datasets.d_dataset_id == dbs_dataset_sizes.d_dataset_id)
        .withColumn("fract_read",col("size_read")/col("dsize"))
        .select(dbs_datasets.d_dataset_id,col("day"),col("client_domain"),col("nfiles_read"),col("size_read"),col("fract_read"))
    )

    return working_set_day

def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST")
    )
    dbs_files = csvreader.schema(schemas.schema_files()).load(
        "/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/FILES/part-m-00000"
    )
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
    )

    dbs_dataset_sizes = (
        dbs_files
        .groupBy("f_dataset_id")
        .agg(fn.sum("f_file_size").alias("dsize"), 
             fn.sum("f_event_count").alias("devts"), 
             fn.count('*').alias("nfiles")
         )
        .join(dbs_datasets,dbs_datasets.d_dataset_id == col("f_dataset_id"))
        .select(dbs_datasets.d_dataset,dbs_datasets.d_dataset_id,col("dsize"),col("nfiles"),col("devts"))
    ) #should also look at valid files..

      
    dbs_dataset_sizes.show(15)
#    return

    func=None
    if args.source == "cmssw":
        func = get_cmssw
    if args.source == "classads":
        func = get_classads
    if args.source == "xrootd":
        func = get_xrootd

    working_set_day = func(spark, dbs_files, dbs_datasets, dbs_dataset_sizes, args)

    working_set_day.show(15)
    sp=args.dates.split('/')
    label=""
    for s in sp:
        if "*" not in s: label=label+"/"+s
    working_set_day.write.option("compression","snappy").parquet(args.out+"/"+args.source+"/"+label,mode="overwrite")
#    working_set_day.write.option("compression","gzip").csv(args.out+"/"+args.source+"/"+label,header=True,mode="overwrite")
#df.write.option("compression","gzip").csv("path")
#    working_set_day.write.parquet(args.out)


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
    parser.add_argument(
        "--source",
        help="Source",
        default="cmssw",
        choices=["classads", "cmssw", "xrootd", "fwjr"],
    )
#    parser.add_argument(
#        "--label",
#        help="Additional label (eg, 2020/01/) to output dir"
#        default=""
#    )
    parser.add_argument("--dates", help="Date portion of file path", default="2020/1*/*")

    args = parser.parse_args()
    run(args)
