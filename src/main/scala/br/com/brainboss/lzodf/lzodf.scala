package br.com.brainboss.lzodf

import org.apache.spark.sql.SparkSession

object lzodf extends App {
  val usage = """
    Usage: lzodf <source table name> <output-path> <parquet table name>
  """

  if (args.length < 3)
    println(usage)
  else {
    val spark = SparkSession.builder().appName("lzodf").getOrCreate()

    val srcTableName = args(0)
    val outputPath = args(1)
    val tableName = args(2)

    val df = spark.sql(s"SELECT * FROM ${srcTableName}")

    df.show(10)

    df
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .option("compression", "snappy")
      .option("path", outputPath)
      .format("parquet")
      .saveAsTable(tableName)
  }
}