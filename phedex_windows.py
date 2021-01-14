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
    parquetreader = (
        spark.read.format("parquet").option("nullValue", "null").option("mode", "FAILFAST")
    )

    recreate_t1t2disk=True
    if recreate_t1t2disk:
        phedex_info = parquetreader.load("/user/dlange/working_set_day/phedex")
        phedex_info = (
            phedex_info
            #for debug..
#            .limit(100)
            .filter(~col("node_name").startswith("T3"))
            .filter(~col("node_name").startswith("X"))
            .filter(~col("node_name").endswith("Buffer"))
            .filter(~col("node_name").endswith("MSS"))
            .filter(~col("node_name").endswith("Export"))
            .withColumn("min_time",fn.to_date("min_time","yyyyMMdd"))
            .withColumn("max_time",fn.to_date("max_time","yyyyMMdd"))
        )
        phedex_info.write.option("compression","snappy").parquet(args.out+"/phedex_t1t2disk",mode="overwrite")
        #phedex_info.write.csv(args.out+"/phedex_t1t2disk",header=True,mode="overwrite")
    else:
        phedex_info = parquetreader.load("/user/dlange/working_set_day/phedex_t1t2disk")
    phedex_info.show(10, False)
    print(phedex_info.count())

# a good sanity check that the size is ok. it is currently..
    phedex_info.filter(col("max_time")==fn.to_date(fn.lit("20201126"),"yyyyMMdd")).agg(fn.sum("rep_size")).show(10, False)

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
    )


# make a dataframe with the oldest date 
    age_df = (
        phedex_info
        .groupBy("dataset_id")
        .agg(fn.min("replica_time_create").alias("create_date"))
        .withColumn("create_date",fn.to_date("create_date","yyyyMMdd"))
    )
    print("Age DF")
    age_df.show(10, False)


    end_date="2021-01-01"
    end_date_date = fn.to_date(fn.lit(end_date))
    windows=[90,180,365]
    w=windows[-1]
    phedex_daily_info = (
        phedex_info
        .withColumn("minrep_age",fn.datediff(end_date_date,col("min_time")))
        .withColumn("maxrep_age",fn.datediff(end_date_date,col("max_time")))
        .filter(col("maxrep_age") < fn.lit(w+1))
        .join(age_df, "dataset_id")
        .withColumn("dataset_age",fn.datediff(end_date_date,col("create_date")))
        .withColumn("allOpsSize", fn.when( ((col("br_user_group_id")==42) | (col("br_user_group_id")==18) | (col("br_user_group_id")==19)), col("rep_size")).otherwise(0))
        .withColumn("anaOpsSize", fn.when( (col("br_user_group_id")==42), col("rep_size")).otherwise(0))
        .withColumn("tier",fn.split(col("dataset_name"), '/').getItem(3))
    )

    print(phedex_daily_info.count())
    phedex_daily_info.show(100, False)

    #lets sanity check this on a few dates
    phedex_daily_info.filter(col("max_time")==fn.to_date(fn.lit("20201126"),"yyyyMMdd")).agg(fn.sum("rep_size"), fn.sum("allOpsSize"), fn.sum("anaOpsSize")).show(10, False)

#    return

#if maxrep_age > w :0
#if minrep_age < 0 : 0
#(max(w, minrep_age) - min(w, maxrep_age)) /w

    for w in windows:
        phedex_daily_info = (
            phedex_daily_info
            .withColumn("w"+str(w), fn.lit(w))
            .withColumn("allSize"+str(w), fn.when ( (col("maxrep_age") > col("w"+str(w))) | (col("minrep_age") < fn.lit(0)), 0.)
                                            .otherwise( ((fn.least(col("w"+str(w)), col("minrep_age")) -  
                                                          fn.greatest(fn.lit(0), col("maxrep_age")) ) / col("w"+str(w))) * col("rep_size")
#                                                          fn.least(col("w"+str(w)), col("maxrep_age")) ) / col("w"+str(w))) * col("rep_size")
                                                    )
                    )
            .withColumn("allOpsSize"+str(w), fn.when ( (col("maxrep_age") > col("w"+str(w))) | (col("minrep_age") < fn.lit(0)), 0.)
                                              .otherwise( ((fn.least(col("w"+str(w)), col("minrep_age")) -
                                                          fn.greatest(fn.lit(0), col("maxrep_age")) ) / col("w"+str(w))) * col("allOpsSize")
                                                    )
                    )
            .withColumn("anaOpsSize"+str(w), fn.when ( (col("maxrep_age") > col("w"+str(w))) | (col("minrep_age") <fn.lit(0)), 0.)
                                            .otherwise( ((fn.least(col("w"+str(w)), col("minrep_age")) -
                                                          fn.greatest(fn.lit(0), col("maxrep_age")) ) / col("w"+str(w))) * col("anaOpsSize")
                                                    )
                    )
            .withColumn("oldAllSize"+str(w), fn.when( col("dataset_age") > col("w"+str(w)), col("allSize"+str(w))).otherwise(0.))
            .withColumn("oldAllOpsSize"+str(w), fn.when( col("dataset_age") > col("w"+str(w)), col("allOpsSize"+str(w))).otherwise(0.))
            .withColumn("oldAnaOpsSize"+str(w), fn.when( col("dataset_age") > col("w"+str(w)), col("anaOpsSize"+str(w))).otherwise(0.))

        )

    print(phedex_daily_info.count())
    phedex_daily_info.show(100)

    phedex_daily_info.agg(fn.sum("allSize90"),fn.sum("allSize180"),fn.sum("allSize365"),fn.sum("anaOpsSize90"),fn.sum("allOpsSize90")).show(10, False)


#    return

#|        dataset_name|replica_time_create|          node_name|        rep_size|  min_time|  max_time|br_user_group_id|minrep_age|maxrep_age|create_date|dataset_age|      allOpsSize|      anaOpsSize|      tier|w30|           allSize30|        allOpsSize30|        anaOpsSize30|        oldAllSize30|     oldAllOpsSize30|     oldAnaOpsSize30|w90|           allSize90|        allOpsSize90|        anaOpsSize90|        oldAllSize90|     oldAllOpsSize90|     oldAnaOpsSize90|w365|allSize365|allOpsSize365|anaOpsSize365|oldAllize365|oldAllOpsSize365|oldAnaOpsSize365|

#    exprs = {x: fn.sum for x in phedex_daily_info.columns if "Size" in x}
    exprs = {x: "sum" for x in phedex_daily_info.columns if "Size" in x and x[-1].isdigit()}
    exprs["tier"]="first"
    print(exprs)

    phedex_daily_info = (
        phedex_daily_info
        .groupBy("dataset_id")
        .agg( exprs )
    )
    for w in windows:

        phedex_daily_info = (
            phedex_daily_info
            .withColumnRenamed("sum(allSize"+str(w)+")","allSize"+str(w))
            .withColumnRenamed("sum(anaOpsSize"+str(w)+")","anaOpsSize"+str(w))
            .withColumnRenamed("sum(allOpsSize"+str(w)+")","allOpsSize"+str(w))
            .withColumnRenamed("sum(oldAllSize"+str(w)+")","oldAllSize"+str(w))
            .withColumnRenamed("sum(oldAnaOpsSize"+str(w)+")","oldAnaOpsSize"+str(w))
            .withColumnRenamed("sum(oldAllOpsSize"+str(w)+")","oldAllOpsSize"+str(w))
        )
    phedex_daily_info = phedex_daily_info.withColumnRenamed("first(tier)","tier")

    print(phedex_daily_info.count())
    phedex_daily_info.show(100)
    phedex_daily_info.write.option("compression","snappy").parquet(args.out+"/phedex_windows",mode="overwrite")
    #phedex_daily_info.write.csv(args.out+"/phedex_windows",header=True,mode="overwrite")

    #sanity check - how much data is there in each window?
    exprs = {x: "sum" for x in phedex_daily_info.columns if "Size" in x and x[-1].isdigit()}
    phedex_tmp = (
        phedex_daily_info
        .agg(exprs)
    )
    phedex_tmp.show(30, False)

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
        "--out",
        metavar="OUTPUT",
        help="Output path in HDFS for result (default: %s)" % defpath,
        default=defpath,
    )

#not used
#    parser.add_argument("--dates", help="Date portion of file path", default="2020/*/*")

    args = parser.parse_args()
    run(args)
