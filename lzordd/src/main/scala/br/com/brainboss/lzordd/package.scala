package br.com.brainboss

import com.github.mjakubowski84.parquet4s.{ParquetWriter, RowParquetRecord, ValueCodecConfiguration}
import com.github.mjakubowski84.parquet4s.ParquetWriter.Options
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, DOUBLE, FLOAT, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.OPTIONAL

import java.time.ZoneOffset
import java.util.TimeZone


package object lzordd {
  def readLzo (sc:SparkContext, inputPath:String, delimiter: String) = {
    val files = sc.newAPIHadoopFile( s"$inputPath/*.lzo",
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])

    println(s"LZO lido de: $inputPath")
//    Split text by delimiter
    files.map(_._2.toString.split(delimiter))
  }
  
  def createParquetSchema(tableSchema: Array[Row]): Unit = {
    val fields = tableSchema.map(field => {
      val fieldType = field.get(1) match {
        case "int" => Types.primitive(INT32, OPTIONAL)
          .as(LogicalTypeAnnotation.intType(32, true))
        case "bigint" => Types.primitive(INT64, OPTIONAL)
          .as(LogicalTypeAnnotation.intType(64, true))
        case "float" => Types.primitive(FLOAT, OPTIONAL)
        case "double" => Types.primitive(DOUBLE, OPTIONAL)
        case _ => Types.primitive(BINARY, OPTIONAL)
          .as(LogicalTypeAnnotation.stringType())
      }

      fieldType.named(field.getAs[String]("col_name"))
    })
    
    Types.buildMessage().addFields(fields:_*)
  }

  def createAndWriteSnappy (ss: SparkSession, tableName: String, lzoRdd:RDD[Array[String]], outputPath:String) ={
    // Get table schema
    val tableSchema = ss.sql(s"DESCRIBE FORMATTED $tableName").collect()
    val filteredTableSchema = tableSchema.takeWhile(l=>l.getAs[String]("col_name").length > 0)
    
    // Set parquet schema
    implicit val schema = createParquetSchema(filteredTableSchema)

    // Casting data and creating parquet records
    val vcc = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))
    val parquetRecords = lzoRdd.map(recordFields => {
      recordFields.zipWithIndex.foldLeft(RowParquetRecord.empty)((parquetRecord, recordField) => {
        val value = filteredTableSchema(recordField._2).get(0) match {
          case "int" => recordField._1.toInt
          case "bigint" => recordField._1.toLong
          case "float" => recordField._1.toFloat
          case "double" => recordField._1.toDouble
          case _ => _
        }
        parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), value, vcc)
      })
    })

    // Each worker writes its on parquet records, so we don't need to collect RDD
    parquetRecords.foreachPartition(workerParquetRecords => {
      ParquetWriter.writeAndClose(
        s"$outputPath/data",
        workerParquetRecords.toList,
        Options(compressionCodecName = CompressionCodecName.SNAPPY)
      )
    })
    println(s"Arquivo parquet criado em: $outputPath")
    
    filteredTableSchema
  }

  def readAndCreateTable (ss: SparkSession, tableSchema: Array[Row], tableName:String, outputPath:String) = {
    val tableFields = tableSchema.map(r=>s"${r.get(0)} ${r.get(1)}").mkString(",")

    ss.sql(
      s"""CREATE EXTERNAL TABLE ${tableName}_snappy (${tableFields})
         |STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")
         |LOCATION '${outputPath}'""".stripMargin)

    println(s"Tabela externa criada: ${tableName}_snappy")
  }
}
