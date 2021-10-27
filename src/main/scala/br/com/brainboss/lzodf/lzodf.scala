package br.com.brainboss.lzodf

import org.apache.spark.sql.SparkSession

object lzodf extends App {
  val spark = SparkSession.builder().appName("lzodf").getOrCreate()

  val inputPath = args(0)
  val outputPath = args(1)

  //spark.conf.set("spark.sql.parquet.compression.codec","codec")
  val df = spark.read.csv(inputPath)

  df.show(10)

  df
    .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
    .option("compression", "snappy")
    .format("parquet")
    .saveAsTable(outputPath)

}