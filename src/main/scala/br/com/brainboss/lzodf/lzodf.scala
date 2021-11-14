package br.com.brainboss.lzodf

import br.com.brainboss.util.hashAndSum
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, sum}

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
    val columns = df.columns.map(colName => col(colName))
    
    val hashColumns = df.withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
    val hashSum = hashColumns.select(sum("checksum") as "hash_sum").head().getAs[Int]("hash_sum")

    df
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .option("compression", "snappy")
      .option("path", outputPath)
      .format("parquet")
      .saveAsTable(s"${tableName}_snappy")
    
    val hashSumSnappy = hashAndSum(spark, s"${tableName}_snappy", columns)
    
    if (hashSum != hashSumSnappy) {
      // TODO: Rollback
    }
  }
}