package br.com.brainboss.lzodf

import br.com.brainboss.util._
import org.apache.spark.sql.functions.{col, concat_ws, sum}

object lzodf extends App {
  val usage = """
    Usage: lzodf <parquet/snappy file destination> <original table name>
  """

  if (args.length < 2)
    println(usage)
  else {
    val spark = startSession()

    val outputPath = args(0)
    val tableName = args(1)
    val outputFile = getOutputFile(outputPath, tableName)

    try {
      val df = spark.sql(s"SELECT * FROM $tableName")
      val columns = df.columns.map(colName => col(colName))

      val hashColumns = df.withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      //TODO - Need to implement log level to show checksum column
      //hashColumns.show(10)
      val hashSum = hashColumns.select(sum("checksum") as "hash_sum").head().getAs[Long]("hash_sum")

      val tableSchema = getTableSchema(spark, tableName)
      createTable(spark, tableSchema, tableName, outputFile)

      df
        .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
        .option("compression", "snappy")
        .option("path", outputFile)
        .format("parquet")
        .save()

      val hashSumSnappy = hashAndSum(spark, s"${tableName}_snappy", columns)

      if (hashSum != hashSumSnappy) {
        throw IncompatibleTablesException("LZO and Snappy tables are incompatible. Rolling back changes.")
      }
    } catch {
      case e: Exception => {
        rollback(spark, tableName, outputFile, spark.sparkContext.hadoopConfiguration)
        e.printStackTrace()
      }
    } finally {
      spark.close()
    }
  }
}