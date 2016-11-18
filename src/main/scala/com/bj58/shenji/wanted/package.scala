package com.bj58.shenji


import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import com.bj58.shenji.data._
import java.util.concurrent.Executors
import java.util.concurrent.Callable


/**
 * 根据用户行为来进行推荐
 * 主要方法有：逻辑回归，决策树，协同过滤（矩阵分解）
 * @author jiangzhenxing
 * @date 2016-10-31
 */
package object wanted 
{
  
  /**
   * 对不同模型的得分进行逻辑回归训练
   */
  def trainModels(sc: SparkContext) 
  {
    val executor = Executors.newFixedThreadPool(16)
    
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect
    testCookies.map { cookieid =>
      executor.submit(new Callable[String]() with Serializable {
                          def call = {
                            val datas = sc.textFile("/home/team016/middata/result10/all/" + cookieid)
                                          .map(labelPoint)
                                          .repartition(4)
                                          .cache()
                           LogisticRegressionWithSGD.train(input=datas, numIterations=200, stepSize=1)
                                    .save(sc, "/home/team016/middata/model10/models/" + cookieid)
                                    
                           datas.unpersist(false)
                             cookieid
                          }
            })
    }.foreach(f => f.get)
    
    executor.shutdown()
  }
  
  def labelPoint(line: String) =
  {
    val values = line.split("\t")  // cookieid, infoid, lrscore, dtscore, avgClick, label
    val cookieid = values(0)
    val infoid = values(1)
    val lrScore = values(2).toDouble
    val dtScore = values(3).toDouble
    val cfScore = 0         // 暂时没打出来
    val clicks = values(4).toDouble
    val label = values(5).toDouble
           
    val features = Vectors.dense(lrScore, dtScore, cfScore, clicks)
    LabeledPoint(label, features)
  }
}