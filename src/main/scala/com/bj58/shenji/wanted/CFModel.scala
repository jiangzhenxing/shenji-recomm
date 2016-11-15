package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

/**
 * 协同过滤模型
 */
object CFModel 
{
  /**
   * 训练
   */
  def train(sc: SparkContext) = 
  {
    val sep = "\t"
    val click_count = sc.textFile("/home/team016/middata/click_count_with_code/")
                        .repartition(80)
                        .map(_.split(sep).map(_.toInt)) // cookieid_index, infoid_index, count
                        .map { case Array(cookie,info,count) => Rating(cookie, info, count) }
                        .cache
    ALS.trainImplicit(ratings=click_count, rank=16, iterations=40, lambda=0.01, alpha=40)
       .save(sc, "home/team016/middata/model/cf2/")
  }
  
}