package uk.ac.city.services

import org.apache.spark.sql.SparkSession

import scala.sys.process._

object ComputeAverage {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.getOrCreate

    val inputFile = args(0)
    val outputFile = args(1)
    val numOfPartitions = args(2).toInt

    "hdfs dfs -rm -r " + outputFile!

    val createCombiner = (c:Double) => (c,1)
    val mergeValue = (acc:(Double, Int), c:Double) => (acc._1 + c, acc._2 + 1)
    val mergeCombiners = (acc1:(Double, Int), acc2:(Double, Int)) =>(acc1._1 + acc2._1, acc1._2 + acc2._2)

    val input = spark.sparkContext.textFile(inputFile, numOfPartitions)
    
    input
    .map(x =>{
      val temp = x.replace("(","").replace(")","").split(",")
      val gtId = temp(0)
      val appliance = temp(1)
      val consumption = temp(2).toDouble

      ((gtId, appliance), consumption)
    })
    .combineByKey(createCombiner, mergeValue, mergeCombiners, 4)
    .map(item => (item._1, (item._2._1 / item._2._2)))
    .saveAsTextFile(outputFile)
    
    spark.stop
  }
}
