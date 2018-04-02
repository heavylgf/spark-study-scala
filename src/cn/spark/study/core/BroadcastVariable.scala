package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 共享变量
 */
object BroadcastVariable {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("BroadcastVariable")
        .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val factor = 3
    val factorBroadcast = sc.broadcast(factor)
    
    val numbersArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numbersArray, 1)
    
    val multipleNumbers = numbers.map { num => num * factorBroadcast.value }
    
    // 打印
    multipleNumbers.foreach { num => println(num) }
    
  }
  
}