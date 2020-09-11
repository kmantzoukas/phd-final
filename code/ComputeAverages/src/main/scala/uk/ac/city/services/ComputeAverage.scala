package uk.ac.city.services

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ComputeAverage {

  val spark = SparkSession.builder.getOrCreate

  def main(args: Array[String]) {

    val createCombiner = (c:Double) => (c,1)
    val mergeValue = (acc:(Double, Int), c:Double) => (acc._1 + c, acc._2 + 1)
    val mergeCombiners = (acc1:(Double, Int), acc2:(Double, Int)) =>(acc1._1 + acc2._1, acc1._2 + acc2._2)

    val input = spark.sparkContext.textFile("hdfs://10.207.1.102:54310/output/output-2",8)
    
    input
    .map(x =>{
      val temp = x.replace("(","").replace(")","").split(",")
      val gtId = temp(0)
      val applicance = temp(1)
      val consumption = temp(2).toDouble

      ((gtId, applicance), consumption)
    })
    .combineByKey(createCombiner, mergeValue, mergeCombiners, 4)
    .map(item => (item._1, (item._2._1 / item._2._2)))
    .saveAsTextFile("hdfs://10.207.1.102:54310/output/output-3")
    
    spark.stop
  }
}
