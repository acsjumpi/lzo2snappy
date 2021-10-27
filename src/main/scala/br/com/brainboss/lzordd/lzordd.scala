package br.com.brainboss.lzordd

import org.apache.spark.sql.SparkSession

object lzordd extends App {
  val usage = """
    Usage: lzordd <input-path> <output-path> <table name> [delimiter]
  """

  if (args.length < 3)
    println(usage)
  else {
    val spark = SparkSession.builder().appName("lzordd").getOrCreate()
    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)
    val tableName = args(2)
    val delimiter = if (args.length == 4) args(3) else ","

    // TODO: Need to catch the correct exceptions
    try {

      val read = readLzo(sc, inputPath, delimiter)
      val tableSchema = createAndWriteSnappy(spark, tableName, read, outputPath)

      readAndCreateTable(spark, tableSchema, tableName, outputPath)

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}

