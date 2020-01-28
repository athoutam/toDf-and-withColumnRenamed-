package com.bigdata.spark.Git

import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object CopyS3CSVDataToORACLEDB {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("CopyS3CSVDataToORACLEDB").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("CopyS3CSVDataToORACLEDB").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val host = "jdbc:oracle:thin:@//oracledb.conbyj3qndaj.ap-south-1.rds.amazonaws.com:1521/ORACLED"
    val prop = new java.util.Properties()
    prop.setProperty("User","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    //Above code illustrates, oracledb connection properties
    val input = args(0)
    val output = args(1)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(input)
    //Above code illustrates,
    //header = true  ===> Letting them to treat first line of data as Column header
    //inferSchema = true ===> Automatically converting to appropiate datatypes, default is String
    df.createOrReplaceTempView("bank")
    val res = spark.sql("select * from bank")
    res.write.mode(SaveMode.Overwrite).jdbc(host,output,prop)
    //Above code illustrates, writing data to Oracledb
    res.show(5)
    spark.stop()
  }
}


/*
Summary :
---------
I have data in S3. I want to copy this data to Oracledb.

dependency :
-----------
Add oracle jdbc jar file.

Spark Submit command :
----------------------
spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]

spark-submit
  --class com.bigdata.spark.Git.CopyS3CSVDataToORACLEDB
  --master local
  --deploy-mode client
  --jars s3://athoutam/Driver/ojdbc7.jar
  s3://athoutam/Jar/spark-project_2.11-0.1.jar
  s3://athoutam/Input/bank-full.csv akshay_bank

 */