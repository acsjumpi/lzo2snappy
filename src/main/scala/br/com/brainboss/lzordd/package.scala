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

    // Read LZO files
    val files = sc.newAPIHadoopFile( s"$inputPath/*.lzo",
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])

    println(s"Input LZO from: $inputPath")
    // Split text by delimiter
    files.map(_._2.toString.split(delimiter))
  }
  
  def createParquetSchema(tableSchema: Array[Row]) = {

    // Generate schema fields
    // TODO : Need to map more fields, today is mapped only basic fields
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

      // Schema Type Definition based on Describe Table return
      fieldType.named(field.getAs[String]("col_name"))
    })

    // Return MessageType for Schema
    Types.buildMessage().addFields(fields:_*).named("schema")
  }

  def createAndWriteSnappy (ss: SparkSession, tableName: String, lzoRdd:RDD[Array[String]], outputPath:String) ={
    // Get table schema
    val tableSchema = ss.sql(s"DESCRIBE FORMATTED $tableName").collect()
    val filteredTableSchema = tableSchema.takeWhile(l=>l.getAs[String]("col_name").length > 0)

    // Casting data and creating parquet records
    val vcc = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))
    val parquetRecords = lzoRdd.map(recordFields => {
      recordFields.zipWithIndex.foldLeft(RowParquetRecord.empty)((parquetRecord, recordField) => {
        println(s"Table Schema output: ${filteredTableSchema(recordField._2).get(1)}")
        val value = if (recordField._1.length == 0)
          parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), "", vcc)
        else filteredTableSchema(recordField._2).get(1) match {
          case "int" => parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toInt, vcc)
          case "bigint" => parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toLong, vcc)
          case "float" => parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toFloat, vcc)
          case "double" => parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toDouble, vcc)
          case _ => parquetRecord.add(filteredTableSchema(recordField._2).getAs[String]("col_name"), recordField._1, vcc)
        }
        value
      })
    })

    // Each worker writes its on parquet records, so we don't need to collect RDD
    parquetRecords.foreachPartition(workerParquetRecords => {

      // Set parquet schema
      implicit val schema = createParquetSchema(filteredTableSchema)

      // Create parquet/snappy raw file
      ParquetWriter.writeAndClose(
        s"$outputPath/data.parquet",
        workerParquetRecords.toList,
        Options(compressionCodecName = CompressionCodecName.SNAPPY)
      )
    }
  )
    println(s"Parquet file generated at: $outputPath")
    filteredTableSchema
  }

  def readAndCreateTable (ss: SparkSession, tableSchema: Array[Row], tableName:String, outputPath:String) = {

    // get table fields from generated schema
    val tableFields = tableSchema.map(r=>s"${r.get(0)} ${r.get(1)}").mkString(",")

    // create parquet/snappy external table using collected table fields
    ss.sql(
      s"""CREATE EXTERNAL TABLE ${tableName}_snappy (${tableFields})
         |STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")
         |LOCATION '${outputPath}'""".stripMargin)

    println(s"External table created: ${tableName}_snappy")
  }
}
