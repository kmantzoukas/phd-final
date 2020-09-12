package uk.ac.city.services

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object PrepareData {

  val spark = SparkSession.builder.getOrCreate

  def main(args: Array[String]) {

    val input = spark.sparkContext.textFile("gs://dataproc-staging-us-central1-658776204196-yztfnikr/output/output-1",8)
    
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
    .saveAsTextFile("gs://dataproc-staging-us-central1-658776204196-yztfnikr/output/output-2")

    spark.stop
  }
}
