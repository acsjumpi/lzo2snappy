package br.com.brainboss

import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.github.mjakubowski84.parquet4s.ParquetWriter.Options
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


package object lzordd {

  def readLzo (sc:SparkContext, inputPath:String) = {
    val files = sc.newAPIHadoopFile( s"$inputPath/*.lzo",
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])

    println(s"LZO lido de: $inputPath")
    files.map(_._2.toString)
  }

  def createAndWriteSnappy (lzoRdd:RDD[String], outputPath:String) ={
    //lzoRdd.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    case class tblFields(i:Int, s:String)
    val tblRegister = Seq(tblFields(1,"Foo"), tblFields(2,"Bar"), tblFields(3,"Baz"))

    ParquetWriter.writeAndClose(
      s"$outputPath/data",
      tblRegister,
      Options(compressionCodecName = CompressionCodecName.SNAPPY)
    )
    println(s"Arquivo parquet criado em: $outputPath")
  }

  def readAndCreateTable (ss:SparkSession, tableName:String, outputPath:String, delimiter:String) = {
    val tableSchema = ss.sql(s"DESCRIBE FORMATTED $tableName").collect()
    val formatTableSchema = tableSchema.takeWhile(l=>l.getAs[String]("col_name").length > 0)
    val tableFields = formatTableSchema.map(r=>s"${r.get(0)} ${r.get(1)}").mkString(",")

    ss.sql(
      s"""CREATE EXTERNAL TABLE ${tableName}_snappy (${tableFields})
         |STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")
         |LOCATION '${outputPath}'""".stripMargin)

    println(s"Tabela externa criada: ${tableName}_snappy")
  }
}
