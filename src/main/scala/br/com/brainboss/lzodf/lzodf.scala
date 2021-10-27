package br.com.brainboss.lzodf

import org.apache.spark.sql.SparkSession

object lzodf extends App {
  val usage = """
    Usage: lzodf <input-path> <output-path> <table name> [delimiter]
  """

  if (args.length < 3)
    println(usage)
  else {
    val spark = SparkSession.builder().appName("lzodf").getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)
    val tableName = args(2)
    val delimiter = if (args.length == 4) args(3) else ","

    val df = spark
      .read
      .option("delimiter", delimiter)
      .csv(inputPath)

    df.show(10)

    df
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .option("compression", "snappy")
      .option("path", outputPath)
      .format("parquet")
      .saveAsTable(tableName)
  }
}