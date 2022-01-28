package br.com.brainboss.lzordd

import br.com.brainboss.util._
import org.apache.spark.sql.functions.col

object lzordd extends App {
  val usage = """
    Usage: lzordd <lzo file location> <parquet/snappy file destination> <original table name> [delimiter]
  """

  if (args.length < 3)
    println(usage)
  else {
    val spark = startSession()

    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)
    val tableName = args(2)
    val delimiter = if (args.length == 4) args(3) else ","
    val outputFile = getOutputFile(outputPath, tableName)

    try {
      val read = readLzo(sc, inputPath, delimiter)

      val hashSum = createHashSum(read)
      val tableSchema = createAndWriteSnappy(spark, tableName, read, outputFile)

      createTable(spark, tableSchema, tableName, outputFile)

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

