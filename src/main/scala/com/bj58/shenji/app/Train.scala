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
    
    if (args(0).toUpperCase() == "LRPART") {
      val part = args(1)
      trainLR(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + part,
              "/home/team016/middata/stage2/model/lr_clean/part" + part)
    }
    
    if (args(0).toUpperCase() == "DTPART") {
      val part = args(1)
      trainDT(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + part,
              "/home/team016/middata/stage2/model/dt_last/part" + part)
    }
    
    if (args(0).toUpperCase() == "CF") {
      trainCF(sc, "/home/team016/middata/click_count_with_code/", "home/team016/middata/model/cf2/")
    }
    
    if (args(0).toUpperCase() == "SVMPart".toUpperCase()) {
      val part = args(1)
      trainSVM(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + part,
              "/home/team016/middata/stage2/model3/svm/part" + part)
    }
    
    if (args(0).toUpperCase() == "ComprehensivePart".toUpperCase()) {
      trainComprehensive(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + args(1))
    }
    
    sc.stop()
  }
}