package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import java.io._

import com.bj58.shenji.data._
import com.bj58.shenji.util._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
 * 评分
 */
object Evaluate 
{
  
//  var lrModel: LogisticRegressionModel = null;
//  var dtModel: DecisionTreeModel = null
//  var preCookie: String = ""
  
  def evaluate(sc: SparkContext, record: String, 
                click_score: scala.collection.Map[String, Double], 
                lrModels: scala.collection.Map[String, LogisticRegressionModel],
                dtModels: scala.collection.Map[String, DecisionTreeModel]) =
  {
    val sep = "\t"
    val values = record.split(sep)
    val cookieid = values(0)
    
    var scorelr = 0.5
    var scoredt = 0.5
    var scoreclick = 0d
    var score = 0.5
    
    try {
      scoreclick = click_score(cookieid + sep + values(1))
      if (values.length > 15) {
        val p = position(values)
        val lrModel = lrModels(cookieid)
        val dtModel = dtModels(cookieid)
        scorelr = logistic(vecdot(lrModel.weights.toArray, p.lrFeatures))
        scoredt = dtModel.predict(Vectors.dense(p.lrFeatures))
        score = scorelr + scoredt * 0.4 + scoreclick * 0.1
      }
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    (values(0), values(1), scorelr, scoredt, scoreclick, score)
  }
  
  def lrEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    var preCookieid = ""
    var model: LogisticRegressionModel = null
    
    val test_data = sc.textFile("/home/team016/middata/test_user_position/")

    val scores = test_data.sortBy(_.split(sep, 2)(0))
                          .toLocalIterator
                          .map { record =>
                            val values = record.split(sep)
                            val cookieid = values(0)
                            if (cookieid != preCookieid) {
                              model = LogisticRegressionModel.load(sc, "/home/team016/middata/model/lr2/" + cookieid)
                              preCookieid = cookieid
                            }
                            var score = 0.5
                            if (values.length > 15) {
                              val p = position(values)
                              score = logistic(vecdot(model.weights.toArray, p.lrFeatures))
                            }
                            Array(cookieid, values(1), score).mkString(sep)
                          }
                          .toSeq
        
    sc.parallelize(scores, 1)
      .saveAsTextFile("/home/team016/middata/result/lr/")
  }
  
  def dtEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    var preCookieid = ""
    var model: DecisionTreeModel = null
    
    val test_data = sc.textFile("/home/team016/middata/test_user_position/")
    
    val scores = test_data.sortBy(_.split(sep, 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split(sep)
                             val cookieid = values(0)
                             if (cookieid != preCookieid) {
                                model = DecisionTreeModel.load(sc, "/home/team016/middata/model/dt2/" + cookieid)
                                preCookieid = cookieid
                              }
                             var score = 0.5
                             if (values.length > 15) {
                                val p = position(values)
                                score = model.predict(Vectors.dense(p.lrFeatures))
                              }
                             Array(cookieid, values(1), score).mkString(sep)
                          }
                          .toSeq
                          
    sc.parallelize(scores, 1)
      .saveAsTextFile("/home/team016/middata/result/dt/")
  }
//  def dtEvaluate(sc: SparkContext, cookieid: String, p: Position) =
//  {
//    if (cookieid != preCookie) {
//      dtModel = DecisionTreeModel.load(sc, "/home/team016/middata/model/dt2/" + cookieid)
//      preCookie = cookieid
//    }
//    dtModel.predict(Vectors.dense(p.lrFeatures))
//  }
  
  def clickEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    val testData = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
                     .map(_.split("\001"))
                     .map(values => (values(1), values(0))) // infoid, cookieid
                     
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")  // infoid, avg_click
                     .map(_.split("\001"))
                     .map(values => (values(0), values(1)))
                     
    testData.leftOuterJoin(avgClick)
//            .map { case (infoid, (cookieid, score)) => Array(cookieid, infoid, if (score == None) "-" else score.get).mkString("\t") }
            .map { case (infoid, (cookieid, score)) => (cookieid, infoid, if (score == None) 0 else score.get.toDouble) }
            .groupBy(_._1)
            .flatMap { case (cookieid, iter) => { 
              val values = iter.toArray
              val clicks = values.map(_._3).filter(_ > 0)
              
              val avg_click = if (clicks.isEmpty) 0 else clicks.sum / clicks.size
              var max_click = if (clicks.isEmpty) 1 else clicks.max
              
              values.map { case (cookieid,infoid,score) => cookieid + sep + infoid + sep + (if (score == 0) avg_click / max_click else score / max_click) }
//              val values = .sortBy(_._3).reverse
             }}
            .repartition(1)
            .saveAsTextFile("/home/team016/middata/click_evaluate") // 961957
  }
  
  def cfEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    val model = MatrixFactorizationModel.load(sc, "home/team016/middata/model/cf/")
    val testdata = sc.textFile("/home/team016/middata/testdata_with_code/")
                     .map(_.split(sep).map(_.toInt))
                     .map(values => (values(0), values(1)))
    model.predict(testdata)
         .map(rating => Array(rating.user, rating.product, rating.rating).mkString(sep))
         .saveAsTextFile("/home/team016/middata/result/cf")
  }
  
  /**
   * (cookieid,0),(p.infoid,1),(p.userid,2),(p.scate1,3),(p.scate2,4),(p.scate3,5),(p.title,6),(p.local,7),(p.salary,8),
   * (p.education,9),(p.experience,10),(p.trade,11),(p.enttype,12),(p.fuli,13),(p.fresh,14),(p.additional,15)
   */
  def position(values: Array[String]) = 
  {
    Position(infoid = values(1),
             scate1 = values(3),
             scate2 = values(4),
             scate3 = values(5),
             title = values(6),
             userid = values(2),
             local = values(7),
             salary = values(8),
             education = values(9),
             experience = values(10),
             trade = values(11),
             enttype = values(12),
             fresh = values(14),
             fuli = values(13),
             additional = values(15)
             )
  }
  
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Evaluate " + args(0).toUpperCase())
    val sc = new SparkContext(conf)
    val sep = "\t"
    
    if (args(0).toUpperCase() == "CF")
      cfEvaluate(sc)
      
    if (args(0).toUpperCase() == "LR")
      lrEvaluate(sc)
    
    if (args(0).toUpperCase() == "DT")
      dtEvaluate(sc)
    /*
    val click_score = sc.textFile("/home/team016/middata/click_evaluate")
                        .map(_.split(sep))
                        .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score.toDouble) }
                        .collectAsMap
    val lrModels = scala.collection.mutable.Map[String, LogisticRegressionModel]()
    val dtModels = scala.collection.mutable.Map[String, DecisionTreeModel]()
    
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect
    testCookies.foreach { cookieid => 
                              lrModels.put(cookieid, LogisticRegressionModel.load(sc, "/home/team016/middata/model/lr2/" + cookieid))
                              dtModels.put(cookieid, DecisionTreeModel.load(sc, "/home/team016/middata/model/dt2/" + cookieid)) }
    
//    val out = new File("/home/team016/shenji/result/result_jiangzhenxing_20161115_v41.txt")
//    imp
    val test_data = sc.textFile("/home/team016/middata/test_user_position/")
                      .sortBy(_.split(sep, 2)(0))
                      .toLocalIterator
                      .map(record => evaluate(sc, record, click_score,lrModels,dtModels))
                      .map { case (cookieid,infoid,scorelr, scoredt, scoreclick, score) => 
                                Array(cookieid,infoid,scorelr,scoredt,scoreclick,score).mkString(sep) }
//                      .foreach(r => writer.write(r + "\n"))
//    writer.close()
    sc.parallelize(test_data.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/result_1115_41_2")
      * 
      */
    sc.stop()
  }
}