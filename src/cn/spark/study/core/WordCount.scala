package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class WordCount {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("WordCount")
        .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("C://Users/CTWLPC/Desktop/spark.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey( _ + _)
    
    wordCounts.foreach(wordCount => print(wordCount._1 + " appeared " + wordCount._2 + " times. "))
    
  }

}