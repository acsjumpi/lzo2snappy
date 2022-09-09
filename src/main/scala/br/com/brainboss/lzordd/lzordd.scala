package br.com.brainboss.lzordd

import br.com.brainboss.util._
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

object lzordd extends App {
  val usage = """
    Usage: lzordd <lzo file location> <parquet/snappy file destination> <original database> <original table name> [delimiter]
  """

  lazy val LOGGER: Logger = LoggerFactory.getLogger(lzordd.getClass)
  if (args.length < 3)
    println(usage)
  else {
    val spark = startSession()

    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)
    val db = args(2)
    val table = args(3)
    val delimiter = if (args.length == 5) args(4) else ","
    val tableName = s"$db.$table"
    val outputFile = getOutputFile(outputPath, tableName)

    try {
      val read = readLzo(sc, inputPath, delimiter)

      val hashSum = createHashSum(read)
      val tableSchema = createAndWriteSnappy(spark, tableName, read, outputFile)

      //createTable(spark, tableSchema, tableName, outputFile)
      mkCreateTable(spark, db, table, outputFile, LOGGER)

      val columns = tableSchema.map(field => col(field.getAs[String](0)))
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

