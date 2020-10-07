package uk.ac.city.services

import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.sql.SparkSession
import scala.sys.process._

object AnonymizeData {

  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFile = args(1)
    val numOfPartitions = args(2).toInt

    "hdfs dfs -rm -r " + inputFile!

    val spark = SparkSession.builder.getOrCreate

    val input = spark.sparkContext.textFile(inputFile, numOfPartitions)
    input.map(m => {
      /*
      Read every line of the input and split it
       */
      val temp = m.split(" ")
      /*
      Get a SHA-256 message digest
       */
      val md = MessageDigest.getInstance("SHA-256")
      /*
      Replace name with a hashed version of the string the represents it
       */
      temp(1) = String.format("%032x",
        new BigInteger(1, md.digest(temp(1).getBytes("UTF-8"))))
      /*
      Reset the message digest
       */
      md.reset
      /*
      Replace surname with a hashed version of the string the represents it
       */
      temp(2) = String.format("%032x",
        new BigInteger(1, md.digest(temp(2).getBytes("UTF-8"))))

      /*
      Return the same line as the original except for the name and surname that has been SHA-256 hashed
       */
      temp.mkString(" ")
    }).saveAsTextFile(outputFile)

    spark.stop
  }
}
