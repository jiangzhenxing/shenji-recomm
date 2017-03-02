package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

/**
 * 协同过滤模型
 * @author jiangzhenxing
 */
object CFModel 
{
  /**
   * 训练
   */
  def train(sc: SparkContext, input: String, output: String) = 
  {
    val sep = "\t"
    val click_count = sc.textFile(input, 80)
                        .map(_.split(sep).map(_.toInt)) // cookieid_index, infoid_index, count
                        .map { case Array(cookie,info,count) => Rating(cookie, info, count) }
                        .cache
    ALS.trainImplicit(ratings=click_count, rank=16, iterations=40, lambda=0.01, alpha=40)
       .save(sc, output)
  }
  
}