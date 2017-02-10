package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD

/**
 * 组合模型
 * 即用各模型的分数为属性，使用LR或SVM进行训练
 * @author jiangzhenxing
 */
object Comprehensive 
{
  var n = 0
  def train(sc: SparkContext, cookiePath: String) =
  {
    val cookies = sc.textFile(cookiePath).collect().toSet
    
    val weights = cookies.map(cookieid => cookieid + "\t" + trainUser(sc, cookieid)).toSeq
    
    sc.parallelize(weights)
      .saveAsTextFile("/home/team016/middata/stage2/model3/comprehensive_svm/part/" + cookiePath.substring(cookiePath.lastIndexOf("/")) + "_" + System.currentTimeMillis())
  }
  
  def trainUser(sc: SparkContext, cookieid: String): String =
  {
    n = n + 1
    println(cookieid + "\ttrained " + n + " *****************************************") // /home/team016/middata/stage2/train_result/clickbyuser/
    
    val train_score = sc.textFile("/home/team016/middata/stage2/train_result/allscorebyuser/" + cookieid + "-*")
                        .map(_.split("\t")) // cookie, info, lrscore, dtscore, svmscore, cfscore, clickscore, label
                        .map { case Array(cookieid, infoid, lrscore, dtscore, svmscore, cfscore, clickscore, label) => 
                               val features = Array(lrscore, dtscore, svmscore, cfscore, clickscore).map(_.toDouble)
                               LabeledPoint(label.toDouble, Vectors.dense(features)) }
                        .cache()
    SVMWithSGD.train(input=train_score, numIterations=158)
              .weights
              .toArray
              .mkString(",")
  }
}