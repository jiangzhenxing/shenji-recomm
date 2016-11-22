package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.bj58.shenji.data._
import com.bj58.shenji.wanted._

/**
 * 模型训练
 */
object Train 
{
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Train " + args(0))
    conf.set("spark.port.maxRetries","100")  // --conf spark.ui.port=424$dateBegin \
    
    val sc = new SparkContext(conf)
    
    if (args(0) == "train")
      LRModel.trainAll(sc)
      
      
    if (args(0) == "LRPart")
      LRModel.trainPart(sc, args(1).toInt)
      
    if (args(0) == "DTPart") // 0,1,2
      DTModel.trainPart(sc, args(1).toInt)
      
    if (args(0) == "CF") {
      CFModel.train(sc)
    }
    
    if (args(0) == "SVMPart") {
      SVMModel.trainPart(sc, args(1).toInt)
    }
      
    if (args(0) == "MODELS") {
      trainModels(sc)
    }
    
    if (args(0) == "ComprehensivePart") {
      Comprehensive.train(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + args(1))
    }
      
//    if (args(0) == "DT")
//      DTModel.train(sc)
//    if (args(0) == "dt")
//      Range(1,16).foreach(dt => extractAction(sc, bcookies, dt))
    sc.stop()
  }
}