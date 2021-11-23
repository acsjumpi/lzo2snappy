package br.com.brainboss

import br.com.brainboss.lzodf.hashUdf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, concat_ws, sum, udf}
import org.apache.spark.sql.{Column, SparkSession}

import java.net.URI

package object util {
  def hashStr (str: String): Int = {
    // TODO: Replace with hash function
    val hash = str
    hash.hashCode
  }

  def hashAndSum(ss: SparkSession, tableName: String, columns: Array[Column]): Long ={
    ss.sql(s"SELECT * FROM ${tableName}")
      .withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      .select(sum("checksum") as "hash_sum")
      .head()
      .getAs[Long]("hash_sum")
  }
  
  def rollback(ss: SparkSession, tableName: String, path: String, conf: Configuration): Unit = {
    val fs = FileSystem.get(URI.create(path), conf)
    
    if (fs.exists(new Path(path))) {
      fs.delete(new Path(path), true)
    }
    
    ss.sql(s"DROP TABLE IF EXISTS ${tableName}_snappy")
  }

  case class IncompatibleTablesException(s: String) extends Exception(s)
}
