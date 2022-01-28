package br.com.brainboss

import br.com.brainboss.util.{getTableSchema, hashStr}
import com.github.mjakubowski84.parquet4s.ParquetWriter.Options
import com.github.mjakubowski84.parquet4s.{ParquetWriter, RowParquetRecord, ValueCodecConfiguration}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import java.time.ZoneOffset
import java.util.TimeZone


package object lzordd {
  def readLzo(sc:SparkContext, inputPath:String, delimiter: String) = {

    // Read LZO files
    val files = sc.newAPIHadoopFile( s"$inputPath/*.lzo",
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])

    println(s"Input LZO from: $inputPath")
    // Split text by delimiter
    files.map(_._2.toString.split(delimiter))
  }
  
  def createHashSum(lzoRdd:RDD[Array[String]]) = {
    lzoRdd
      .map(row => row.filter(field => !field.equals("\\N") && field.length > 0))
      .map(row => hashStr(row.mkString(",")).toLong)
      .reduce(_ + _)
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

  def createAndWriteSnappy (spark: SparkSession, tableName: String, lzoRdd:RDD[Array[String]], outputPath:String) ={
    // Get table schema
    val tableSchema = getTableSchema(spark, tableName)

    // Casting data and creating parquet records
    val vcc = ValueCodecConfiguration(TimeZone.getTimeZone(ZoneOffset.UTC))
    val parquetRecords = lzoRdd.map(recordFields => {
      recordFields
        .zipWithIndex
        .filter((recordField) => recordField._1.length > 0 && !recordField._1.equals("\\N"))
        .foldLeft(RowParquetRecord.empty)((parquetRecord, recordField) => {
          val value = tableSchema(recordField._2).get(1) match {
            case "int" => parquetRecord.add(tableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toInt, vcc)
            case "bigint" => parquetRecord.add(tableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toLong, vcc)
            case "float" => parquetRecord.add(tableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toFloat, vcc)
            case "double" => parquetRecord.add(tableSchema(recordField._2).getAs[String]("col_name"), recordField._1.toDouble, vcc)
            case _ => parquetRecord.add(tableSchema(recordField._2).getAs[String]("col_name"), recordField._1, vcc)
          }
          value
        })
    })

    // Each worker writes its on parquet records, so we don't need to collect RDD
    parquetRecords.foreachPartition(workerParquetRecords => {
      // Set parquet schema
      implicit val schema = createParquetSchema(tableSchema)
  
      // Create parquet/snappy raw file
      ParquetWriter.writeAndClose(
        s"$outputPath/data_${TaskContext.getPartitionId()}.parquet",
        workerParquetRecords.toList,
        Options(compressionCodecName = CompressionCodecName.SNAPPY)
      )
    })
    
    println(s"Parquet file generated at: $outputPath")
    tableSchema
  }
}
