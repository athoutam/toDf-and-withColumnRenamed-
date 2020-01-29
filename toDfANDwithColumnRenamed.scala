package com.bigdata.spark.Git

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object toDfANDwithColumnRenamed {
  def main(args Array[String]) {
    val spark = SparkSession.builder.master(local[]).appName(toDfANDwithColumnRenamed).config(spark.sql.warehouse.dir, homehadoopworkwarehouse).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master(local[]).appName(toDfANDwithColumnRenamed).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = CworkdatasetsTXTasl.csv
    val df = spark.read.format(csv).option(header,true).option(inferSchema,true).load(data)
    In the above data, we dont have column headers. We just have data.
    df.show(5, false)
    It will consider first row as column header. That's y we are using below toDF().
    df.printSchema()
    val ndf = df.toDF(name,age,city)
    
    toDF() method is used for 2 purposes
    a) It creates a dataframe, but the data must be rowstructured format
    b) In dataframe, to rename columns. But we need to rename all columns here. Cant rename a single column with this.
     
    val ndf1 = ndf.withColumnRenamed(city,loc)
    withColumnRenamed() method is used to rename single individual column name. 
    ndf1.show(5)
    ndf1.printSchema()
    spark.stop()
  }
}


Summary 
---------
toDf() and withColumnRenamed() methods examples.
 