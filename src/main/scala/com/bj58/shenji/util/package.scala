package com.bj58.shenji

import org.apache.spark.SparkContext

package object util 
{
  /**
   * 计算向量的模长
   */
  def vecmodel(v: Array[Double]) =
  {
    math.sqrt(v.map(d ⇒ d * d).sum)
  }

  /**
   * 向量的点积
   */
  def vecdot(v1: Array[Double], v2: Array[Double]): Double = 
    v1.zip(v2).map { case (d1,d2) => d1 * d2 }.sum
  
  /**
   * logistic函数值
   */
  def logistic(x: Double): Double = 1 / (1 + math.exp(-x))
  
  /**
   * cmc_local表中的数据
   * @return Map(areaid, area_full_path)
   */
  def cmcLocal(sc: SparkContext) 
               = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/ds_dict_cmc_local")
                   .map(_.split("\001"))
                   .map(values => (values(0), values(3).split("\002").slice(0, 2).mkString(",")))
                   .collectAsMap
  /**
   * 用户点击或投递过的职位类别
   */
  def userJobcates(sc: SparkContext) = sc.textFile("/home/team016/middata/stage2/user_job_cates/")
                                         .map(_.split("\001"))
                                         .map(values => (values(0), values(1).split(";")))
                                         .collectAsMap()
        
  /**
   * 用户点击或投递过的地区（二级地域）
   */
  def userLocals(sc: SparkContext) = sc.textFile("/home/team016/middata/stage2/user_locals/")
                                       .map(_.split("\001"))
                                       .map(values => (values(0), values(1).split(";")))
                                       .collectAsMap()
                                       
                                       
}