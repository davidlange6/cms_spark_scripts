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
    import json
    f=open("./overrides.json")
    data=json.load(f)
    f.close()
    data_list=[]
    for d in data.keys():
        data_list.append( (d, data[d]))
    print(data_list)

    conf = SparkConf().setMaster("yarn").setAppName("Get exceptions")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    exceptColumns = ["dataset_name","value"]
    except_df = spark.createDataFrame(data=data_list, schema=exceptColumns)
    except_df.show(100, False)

    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST").option("header",True)
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
#        .filter(col("d_dataset")=="/ZNuNuGJets_MonoPhoton_PtG-40to130_TuneCP5_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM")
    )
    dbs_datasets.show(15,False)

    except_df = (
        except_df
        .join(dbs_datasets, dbs_datasets.d_dataset == except_df.dataset_name)
        .select("d_dataset_id","value")
    )
    except_df.show(15, False)

    except_df.write.csv(args.out+"/exceptions",header=True,mode="overwrite")

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

    parser.add_argument("--dates", help="Date portion of file path", default="2020/1*/*")

    args = parser.parse_args()
    run(args)
