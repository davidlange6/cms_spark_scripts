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
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )


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

    remake_step1=True #merges read information from cmssw/xroot/classad
    remake_step2=True
    remake_step3=True

    if remake_step1:

#        cmssw_info = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/cmssw/2020/*")
#        xrootd_info = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/xrootd/2020/*")
#        classad_info = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/classads/2020/*")
        cmssw_info = parquetreader.load("/user/dlange/working_set_day/cmssw/{2019,2020}/*")
        xrootd_info = parquetreader.load("/user/dlange/working_set_day/xrootd/{2019,2020}/*")
        classad_info = parquetreader.load("/user/dlange/working_set_day/classads/{2019,2020}/*")
#        classad_info = parquetreader.load("/user/dlange/working_set_day/classadstest/2020/12")
#        classad_info.show(5, False)
#        return
        cmssw_df = (
            cmssw_info
            #        .filter(col("d_dataset")==debug_dataset)
            .groupBy("d_dataset_id","day")
            .agg(fn.sum("fract_read").alias("read")
             )
        )
        cmssw_df.show(15, False)

        xrootd_df = (
            xrootd_info
            #        .filter(col("d_dataset")==debug_dataset)
            .groupBy("d_dataset_id","day")
            .agg(fn.sum("fract_read").alias("read")
            )
        )
        xrootd_df.show(15, False)

        classad_df = (
            classad_info
            #        .filter(col("d_dataset")==debug_dataset)
            .groupBy("d_dataset_id","day")
            .agg(fn.sum("fract_read").alias("read")
             )
        )
        classad_df.show(15, False)
        
        merged_df = ( 
            cmssw_df.withColumnRenamed('read','cmssw_read')
            .join(xrootd_df.withColumnRenamed('read','xroot_read'), on=['d_dataset_id','day'], how='outer') # are these joins doing what I want?
            .join(classad_df.withColumnRenamed('read','classad_read'), on=['d_dataset_id','day'], how='outer')
            .withColumn("fract_read",fn.greatest(col('cmssw_read'), col('xroot_read'), col('classad_read')))  
            #        .select("d_dataset","day","fract_read")
        )
        print("Merged df")
        merged_df.show(30, False)
#        merged_df.na.fill(-1)
        #I need this for the rolling windows work..
        from datetime import datetime
        print(datetime.now())
        merged_df.write.option("compression","snappy").parquet(args.out+"/merged_reads",mode="overwrite")
        #merged_df.write.option("compression","gzip").csv(args.out+"/merged_reads",header=True,mode="overwrite")
        print(datetime.now())
        #return
    else:
        if remake_step2:
#            merged_df = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/merged_reads")
            merged_df = parquetreader.load("/user/dlange/working_set_day/merged_reads")


    def quantize_fract(f):
# i eliminated the nulls. - if there are nulls its a bug
        if not f: return 0 #strange error?
        if f>=0. and f<1.5: return 1
        if f>15.: return 15
        return int(f)




    quantudf = fn.udf(lambda z: quantize_fract(z),types.LongType())
    end_date="2020-11-24"
    windows=[90,180,365]

    if remake_step1 or remake_step2:

        merged_df = (
            merged_df
            .withColumn("days_prior",fn.datediff(fn.to_date(fn.lit(end_date)),fn.to_date(col("day"),"yyyyMMdd")))
        )

        for w in windows:
            for met in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
                merged_df = (
                    merged_df
                    .withColumn( met+str(w), fn.when(col("days_prior")<fn.lit(w), col(met) ).otherwise(0.))
                )

        merged_df.show(30)

        exprs = {x: "sum" for x in merged_df.columns if "read" in x and x[-1].isdigit()}

        merged_df = (
            merged_df
            .groupBy("d_dataset_id")
            .agg(exprs)
        )
        merged_df.show(30)
        merged_df.na.fill(-1)
        merged_df.show(30)

        for w in windows:
            for met in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
                merged_df = (
                    merged_df
                    .withColumn(met+str(w),quantudf("sum("+met+str(w)+")"))
                    .drop("sum("+met+str(w)+")")
                )
            
        print(merged_df.count())            
        merged_df.show(30)

#        merged_df.write.option("compression","gzip").csv(args.out+"/read_windows",header=True,mode="overwrite")
        merged_df.write.option("compression","snappy").parquet(args.out+"/read_windows",mode="overwrite")

    else:
        #merged_df = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/read_windows")
        merged_df = parquetreader.load("/user/dlange/working_set_day/read_windows")
        merged_df.show(30)


    if remake_step1 or remake_step2 or remake_step3:

        exceptions = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/exceptions")
  
        #phedex_windows_df = csvreader.option("inferSchema", "true").load("/user/dlange/working_set_day/phedex_windows")
        phedex_windows_df = parquetreader.load("/user/dlange/working_set_day/phedex_windows")
        print(phedex_windows_df.count(),merged_df.count())

        popularity_window_df = (
            phedex_windows_df
            .join(merged_df, merged_df.d_dataset_id == phedex_windows_df.dataset_id, how='full')
            .withColumn("name_", fn.coalesce("dataset_id", "d_dataset_id"))
            .drop("dataset_id")
            .drop("d_dataset_id")
            .withColumnRenamed("name_","dataset_id")
        )

        for w in windows:
            for pop_type in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
                col_name = pop_type+str(w)
                popularity_window_df = popularity_window_df.fillna({col_name:0})
#sanity check that I have the join done correctly...
#        popularity_window_df.select([fn.count(fn.when(col(c).isNull(), c)).alias(c) for c in popularity_window_df.columns]).show(100,False)

        popularity_window_df.show(30)

        popularity_window_df = (
            popularity_window_df
            .join(exceptions, popularity_window_df.dataset_id == exceptions.d_dataset_id, how="full")
        )
        print("Show before exceptions")
        popularity_window_df.show(15,False)
        for w in windows:
            for pop_type in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
                popularity_window_df = (
                    popularity_window_df
                    .withColumn(pop_type+str(w), fn.coalesce("value", pop_type+str(w)))
                )
        print("Show after exceptions")
        popularity_window_df.show(15,False)
#        return
#        .select("d_dataset_id","value")

#        return

# lets find stuff that is unused in the last six months and is still big
        top_unused = (
            popularity_window_df
            .filter(col("fract_read180")<fn.lit(1)).sort(col("allSize180"), ascending=False)
            .select("dataset_id","allSize90","allSize180","allSize365")
            .limit(5000)
        )
#        print(top_unused.dtypes)

        dbs_datasets = (
            csvreader.schema(schemas.schema_datasets())
            .load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")
            #        .filter(col("d_dataset")=="/ZNuNuGJets_MonoPhoton_PtG-40to130_TuneCP5_13TeV-madgraph/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v1/MINIAODSIM")
        )

        top_unused =  ( 
            top_unused
            .join(dbs_datasets, top_unused.dataset_id == dbs_datasets.d_dataset_id)
            .select("d_dataset","allSize90","allSize180","allSize365")
            .sort(col("allSize180"), ascending=False)
        )
        top_unused.show(200, False)
        top_unused.write.csv(args.out+"/top_unused",header=True,mode="overwrite")
#        print("Stopping as its all I need")
#        return
#        top_unused.show(100, False)

#        return


        data_cat_list={}
        data_cat_list["All"]=fn.lit(True)
        data_cat_list["sAODs"]=fn.lower(col("tier")).contains("aod")
        data_cat_list["sMINIAODs"]=fn.lower(col("tier")).contains("miniaod")
        data_cat_list["AODs"]=  fn.lower(col("tier")).startswith("aod") 
        data_cat_list["notsAODs"] = ~data_cat_list["sAODs"]

        agg_list=[]
        for w in windows:
            for data_type in ["allSize", "allOpsSize", "anaOpsSize","oldAllSize","oldAllOpsSize","oldAnaOpsSize"]:
                for pop_type in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
                    for data_cat in data_cat_list:
                        this_agg = fn.array(*[fn.sum( fn.when( (col(pop_type+str(w))==i) & (data_cat_list[data_cat]), col(data_type+str(w)) ).otherwise(0.)) for i in range(16)]).alias("sums_"+data_type+"_"+pop_type+"_"+data_cat+"_"+str(w))
                        agg_list.append(this_agg)
        print(len(agg_list),len(windows)*6*4*len(data_cat_list))
#    exp1 = popularity_window_df.agg(fn.array(*[fn.sum( fn.when(col("fract_read30")==i, col("allSize30")).otherwise(0.)) for i in range(16)]).alias("sums30"))
        exp1 = popularity_window_df.agg(*agg_list)
        print("after aggs",exp1.count(), len(exp1.dtypes))
        exp1.show(10,False)
        exp1.write.json(args.out+"/popularity_dump1",mode="overwrite")
    else:
        exp1 = jsonreader.load("/user/dlange/working_set_day/popularity_dump1")


    def flatten_table(column_names, column_values):
        row = zip(column_names, column_values)
#        _, key = next(row)  # Special casing retrieving the first column
        key= row[0][1]
        return [
#            Row(Key=key, ColumnName=column, ColumnValue=value)
            Row(column, value)
            for column, value in row#[1:]
        ]



    from functools import partial
    exp2=exp1.rdd.flatMap(partial(flatten_table, exp1.columns)).toDF()

    print(exp2.columns)

    
    exp2.show(30,False)
    names=exp2.columns

    exp2 = exp2.select(names[0], *[exp2[names[1]][i] for i in range(16)])
    for i in range(16):
        exp2 = exp2.withColumnRenamed(names[1]+"["+str(i)+"]",str(i))


    exp2.show(1000,False)
    exp2.write.csv(args.out+"/popularity_dump2",header=True,mode="overwrite")

                                   

#   popularity_windows .agg(fn.collect_set("fract_read30"))

#    exprs_pop = {x: "sum" for x in merged_df.columns if "Size" in x and x[-1].isdigit()}
#
#    popularity_window_df = (
#        popularity_window_df
#        .groupBy 
#
#    for w in windows:
#        for met in ["fract_read", "cmssw_read", "xroot_read", "classad_read"]:
#            popularity_window_df = (
#                popularity_window_df
#                .withColumn("size_"+met+str(w), col(


                
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
