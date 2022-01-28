package br.com.brainboss

import br.com.brainboss.lzodf.hashUdf
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{concat_ws, sum}
import org.apache.spark.sql.{Column, Row, SparkSession}

import java.net.URI
import scala.util.hashing.MurmurHash3

package object util {
  def startSession(): SparkSession = {
    val conf = ConfigFactory.load()
    val builder = SparkSession
      .builder()
      .master(conf.getString("submit.master"))
      .config("spark.sql.catalogImplementation", conf.getString("submit.catalog"))
    
    if (conf.getString("submit.catalog").equals("hive")) {
      if (conf.getBoolean("submit.kerberized")) {
        builder.config("spark.hadoop.hive.metastore.uris", conf.getString("submit.metastore_uri"))
          .config("spark.hadoop.hive.metastore.sasl.enabled", conf.getString("submit.kerberized"))
          .config("spark.hadoop.hive.metastore.kerberos.principal", conf.getString("submit.principal"))
          .config("spark.driver.extraLibraryPath", conf.getString("submit.extraLibraryPath"))
          .config("spark.executor.extraLibraryPath", conf.getString("submit.extraLibraryPath"))
          .appName("lzodf")
          .getOrCreate()
      } else {
        builder.config("spark.hadoop.hive.metastore.uris", conf.getString("submit.metastore_uri"))
          .config("spark.driver.extraLibraryPath", conf.getString("submit.extraLibraryPath"))
          .config("spark.executor.extraLibraryPath", conf.getString("submit.extraLibraryPath"))
          .appName("lzodf")
          .getOrCreate()
      }
    } else {
      builder.appName("lzodf")
        .getOrCreate()
    }
  }
  
  // Auxiliary function to guarantee only positive hash from MurmurHash3 return
  def positiveHash(h: Int): Int = {
    if (h < 0) -1 * (h + 1) else h
  }

  def hashStr (str: String): Int = {
    // Uses MurmurHash3 (very fast and lower collision rate)
    val hash = MurmurHash3.stringHash(str)
    positiveHash(hash)
  }

  def hashAndSum(spark: SparkSession, tableName: String, columns: Array[Column]): Long ={
    spark.sql(s"SELECT * FROM $tableName")
      .withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      .select(sum("checksum") as "hash_sum")
      .head()
      .getAs[Long]("hash_sum")
  }
  
  def getOutputFile(outputPath: String, tableName: String): String = {
    if (outputPath.last.equals('/'))
      s"$outputPath${tableName.split('.').last}_snappy"
    else
      s"$outputPath/${tableName.split('.').last}_snappy"
  }
  
  def getTableSchema(spark: SparkSession, tableName: String): Array[Row] = {
    spark.sql(s"DESCRIBE $tableName").collect()
  }

  def createTable (spark: SparkSession, tableSchema: Array[Row], tableName:String,
                   outputPath:String) = {

    // get table fields from generated schema
    val tableFields = tableSchema.map(r=>s"${r.get(0)} ${r.get(1)}").mkString(",")

    // create parquet/snappy external table using collected table fields
    spark.sql(
      s"""CREATE EXTERNAL TABLE ${tableName}_snappy (${tableFields})
         |STORED AS PARQUET
         |LOCATION '${outputPath}'
         |TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")
         |""".stripMargin)

    println(s"External table created: ${tableName}_snappy")
  }
  
  def rollback(spark: SparkSession, tableName: String, path: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(URI.create(path), conf)
    
    if (fs.exists(new Path(path))) {
      fs.delete(new Path(path), true)
    }
    
    spark.sql(s"DROP TABLE IF EXISTS ${tableName}_snappy")
  }

  case class IncompatibleTablesException(s: String) extends Exception(s)
}
