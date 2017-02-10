package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.bj58.shenji.data._
import com.bj58.shenji.wanted._

/**
 * 模型训练
 * @author jiangzhenxing
 */
object Train 
{
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Train " + args(0))
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    if (args(0) == "train")
      LRModel.trainAll(sc)
    
    if (args(0) == "LRPart")
      trainLRPart(sc, args(1).toInt)
    
    if (args(0) == "DTPart") // 0,1,2
      trainDTPart(sc, args(1).toInt)
    
    if (args(0) == "CF") {
      trainCF(sc)
    }
    
    if (args(0) == "SVMPart") {
      trainSVMPart(sc, args(1).toInt)
    }
    
    if (args(0) == "ComprehensivePart") {
      trainComprehensive(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + args(1))
    }
    
    sc.stop()
  }
}