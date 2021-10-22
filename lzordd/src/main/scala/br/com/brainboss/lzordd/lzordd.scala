package br.com.brainboss.lzordd

import org.apache.spark.sql.SparkSession

object lzordd extends App {
      val spark = SparkSession.builder().appName("lzordd").getOrCreate()
      val sc = spark.sparkContext

      val inputPath = args(0)
      val outputPath = args(1)
      val tableName = args(2)
      val delimiter = args(3)

      val read = readLzo(sc, inputPath)
      createAndWriteSnappy(read, outputPath)
      readAndCreateTable(spark, tableName, outputPath, delimiter)

    //read.collect().foreach(println)
}

