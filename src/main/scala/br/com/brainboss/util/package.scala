package br.com.brainboss

import br.com.brainboss.lzodf.hashUdf
import br.com.brainboss.lzodf.lzodf.LOGGER
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{concat_ws, sum}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import scala.util.hashing.MurmurHash3

package object util {
  def startSession(): SparkSession = {
    val conf = ConfigFactory.load()
    val builder = SparkSession
      .builder()
      .master(conf.getString("master"))
      .config("spark.sql.catalogImplementation", conf.getString("catalog"))
    
    if (conf.getString("catalog").equals("hive")) {
      if (conf.getBoolean("kerberized")) {
        builder.config("spark.hadoop.hive.metastore.uris", conf.getString("metastore_uri"))
          .config("spark.hadoop.hive.metastore.sasl.enabled", conf.getString("kerberized"))
          .config("spark.hadoop.hive.metastore.kerberos.principal", conf.getString("principal"))
          .config("spark.driver.extraLibraryPath", conf.getString("extraLibraryPath"))
          .config("spark.executor.extraLibraryPath", conf.getString("extraLibraryPath"))
          .appName("lzodf")
          .getOrCreate()
      } else {
        builder.config("spark.hadoop.hive.metastore.uris", conf.getString("metastore_uri"))
          .config("spark.driver.extraLibraryPath", conf.getString("extraLibraryPath"))
          .config("spark.executor.extraLibraryPath", conf.getString("extraLibraryPath"))
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

  def hashAndSum(spark: SparkSession, tableName: String, columns: Array[Column], LOGGER: Logger): Long ={
    val df = spark.sql(s"SELECT * FROM $tableName")
    LOGGER.warn(s"DFNEW SHOW $tableName")
    df.show(10)
      df.withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
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
    val numColumns = spark.sql(s"SHOW COLUMNS IN $tableName").collect().length
    spark.sql(s"DESCRIBE $tableName").collect().slice(0, numColumns)
  }

  def mkCreateTable(spark: SparkSession, db: String, table: String, outputPath:String, LOGGER:Logger): List[String] = {
    val createtabstmt = spark.sql(s"SHOW CREATE TABLE $db.$table").collect().head.getString(0)
    val until = createtabstmt.indexOf("ROW FORMAT SERDE")
    val newstmt = createtabstmt.slice(0, until).replace(s"`$db`.`$table`(", s"`$db`.`${table}_snappy`(")

    spark.sql(
      s"""$newstmt STORED AS PARQUET
         |LOCATION '${outputPath}'
         |TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")
         |""".stripMargin)

    println(s"External table created: $db.${table}_snappy")

    if(newstmt.contains("PARTITIONED BY")){
      val from = newstmt.indexOf("PARTITIONED BY")
      val partitions = newstmt.slice(from, newstmt.length)
      val pattern = "(?<=`).*(?=`)".r
      pattern.findAllIn(partitions).toList
    }else
      List.empty
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
