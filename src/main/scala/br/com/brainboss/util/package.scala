package br.com.brainboss

import br.com.brainboss.lzodf.hashUdf
import org.apache.spark.sql.functions.{concat_ws, sum}
import org.apache.spark.sql.{Column, SparkSession}

package object util {
  def hashStr (str: String): Int = {
    // TODO: Replace with hash function
    val hash = str
    hash.hashCode
  }
  
  def hashAndSum(ss: SparkSession, tableName: String, columns: Array[Column]): Int ={
    ss.sql(s"SELECT * FROM ${tableName}")
      .withColumn("checksum", hashUdf(concat_ws(",", columns:_*)))
      .select(sum("checksum") as "hash_sum")
      .head()
      .getAs[Int]("hash_sum")
  }
}
