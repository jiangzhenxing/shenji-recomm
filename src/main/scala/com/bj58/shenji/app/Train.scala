package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.bj58.shenji.data._
import com.bj58.shenji.wanted.LRModel
import com.bj58.shenji.wanted.DTModel

/**
 * 模型训练
 */
object Train 
{
  def main(args: Array[String]): Unit = 
  {
//    val conf = new SparkConf().setAppName("Train " + args(0))
//    val sc = new SparkContext(conf)
    
    if (args(0) == "LR")
      LRModel.train
      
//    if (args(0) == "DT")
//      DTModel.train(sc)
//    if (args(0) == "dt")
//      Range(1,16).foreach(dt => extractAction(sc, bcookies, dt))
//    sc.stop()
  }
}