package br.com.brainboss.lzodf

import br.com.brainboss.util._
import org.apache.spark.sql.functions.{col, concat_ws, sum}
import org.slf4j.{Logger, LoggerFactory}

object lzodf extends App {
  val usage = """
    Usage: lzodf <parquet/snappy file destination> <original database> <original table name>
  """
  lazy val LOGGER: Logger = LoggerFactory.getLogger(lzodf.getClass)
  if (args.length < 2)
    println(usage)
  else {
    val spark = startSession()

    val outputPath = args(0)
    val db = args(1)
    val table = args(2)
    val tableName = s"$db.$table"
    val outputFile = getOutputFile(outputPath, tableName)

    try {
      val df = spark.sql(s"SELECT * FROM $tableName")
      val columns = df.columns.map(colName => col(colName))

      val hashColumns = df.withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      //TODO - Need to implement log level to show checksum column

//      hashColumns.show(10)
      val hashSum = hashColumns.select(sum("checksum") as "hash_sum").head().getAs[Long]("hash_sum")

      //val tableSchema = getTableSchema(spark, tableName)
      //createTable(spark, tableSchema, tableName, outputFile)
      val partitions = mkCreateTable(spark, db, table, outputFile, LOGGER)

      partitions.foreach(LOGGER.warn)
      if(partitions.isEmpty)
        df
          .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
          .option("compression", "snappy")
          .option("path", outputFile)
          .format("parquet")
          .save()
      else
        df
          .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
          .partitionBy(partitions:_*)
          .option("compression", "snappy")
          .option("path", outputFile)
          .format("parquet")
          .saveAsTable(s"${tableName}_snappy")


      val hashSumSnappy = hashAndSum(spark, s"${tableName}_snappy", columns, LOGGER)

      if (hashSum != hashSumSnappy) {
        throw IncompatibleTablesException("LZO and Snappy tables are incompatible. Rolling back changes.")
      }
    } catch {
      case e: Exception => {
        //rollback(spark, tableName, outputFile, spark.sparkContext.hadoopConfiguration)
        e.printStackTrace()
      }
    } finally {
      spark.close()
    }
  }
}