package br.com.brainboss.lzodf

import org.apache.spark.sql.SparkSession

object lzodf extends App {
  val usage = """
    Usage: lzodf <parquet/snappy file destination> <original table name>
  """

  if (args.length < 2)
    println(usage)
  else {
    val spark = SparkSession.builder().appName("lzodf").getOrCreate()

    val outputPath = args(0)
    val tableName = args(1)

    val df = spark.sql(s"SELECT * FROM ${tableName}")

    df
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .option("compression", "snappy")
      .option("path", outputPath)
      .format("parquet")
      .saveAsTable(s"${tableName}_snappy")
  }
}