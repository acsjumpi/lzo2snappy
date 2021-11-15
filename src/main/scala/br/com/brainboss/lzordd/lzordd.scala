package br.com.brainboss.lzordd

import br.com.brainboss.util.hashAndSum
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object lzordd extends App {
  val usage = """
    Usage: lzordd <lzo file location> <parquet/snappy file destination> <original table name> [delimiter]
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

    // TODO: Needs to catch the correct exceptions
    try {

      val read = readLzo(sc, inputPath, delimiter)
      val hashSum = createHashSum(read)
      val tableSchema = createAndWriteSnappy(spark, tableName, read, outputPath)

      createTable(spark, tableSchema, tableName, outputPath)

      val columns = tableSchema.map((field) => col(field.getAs[String](0)))
      val hashSumSnappy = hashAndSum(spark, s"${tableName}_snappy", columns)

      if (hashSum != hashSumSnappy) {
        // TODO: Rollback
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}

