package uk.ac.city.services

import org.apache.spark.sql.SparkSession

import scala.sys.process._

object PrepareData {

  val spark = SparkSession.builder.getOrCreate

  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFile = args(1)
    val numOfPartitions = args(2).toInt

    "hdfs dfs -rm -r " + outputFile!

    val input = spark.sparkContext.textFile(inputFile, numOfPartitions)
    
    input
    .flatMap({
      item =>
          val items = item.split(" ")
          val id:String = items(0)
          val refrigerator = ((id, "REFRIGERATOR"), items(4).toDouble)
          val stove = ((id, "STOVE"), items(5).toDouble)
          val kettle = ((id, "KETTLE"), items(6).toDouble)
          val television= ((id, "TELEVISION"), items(7).toDouble)
          val heater = ((id, "HEATER"), items(8).toDouble)

          List(refrigerator,stove,kettle,television,heater).toList
    })
    .saveAsTextFile(outputFile)

    spark.stop
  }
}
