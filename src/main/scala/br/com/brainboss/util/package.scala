package br.com.brainboss

import br.com.brainboss.lzodf.hashUdf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, concat_ws, sum, udf}
import org.apache.spark.sql.{Column, SparkSession}
import scala.util.hashing.MurmurHash3
import java.net.URI

package object util {
  // Auxiliary function to guarantee only positive hash from MurmurHash3 return
  def positiveHash(h: Int): Int = {
    if (h < 0) -1 * (h + 1) else h
  }

  def hashStr (str: String): Int = {
    // Uses MurmurHash3 (very fast and lower collision rate)
    val hash = MurmurHash3.stringHash(str)
    positiveHash(hash)
  }

  def hashAndSum(ss: SparkSession, tableName: String, columns: Array[Column]): Long ={
    ss.sql(s"SELECT * FROM ${tableName}")
      .withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      .select(sum("checksum") as "hash_sum")
      .head()
      .getAs[Long]("hash_sum")
  }
  
  def getOutputFile(outputPath: String, tableName: String): String = {
    if (outputPath.last.equals('/'))
      s"$outputPath${tableName}_snappy"
    else
      s"$outputPath/${tableName}_snappy"
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
