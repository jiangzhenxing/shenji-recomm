package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import com.bj58.shenji.data._
import com.bj58.shenji.util._
import org.apache.spark.mllib.linalg.Vectors

object Evaluate 
{
  
  var lrModel: LogisticRegressionModel = null;
  var dtModel: DecisionTreeModel = null
  var preCookie: String = ""
  
  def evaluate(sc: SparkContext, record: String) =
  {
    val sep = "\t"
    val values = record.split(sep)
    val cookieid = values(0)
    val p = position(values)
    (values(0), values(1), lrEvaluate(sc, cookieid, p) + dtEvaluate(sc, cookieid, p) * 0.4)
  }
  
  
  def lrEvaluate(sc: SparkContext, cookieid: String, p: Position) =
  {
    if (cookieid != preCookie) {
      lrModel = LogisticRegressionModel.load(sc, "/home/team016/middata/model/lr/" + cookieid)
      preCookie = cookieid
    }
    logistic(vecdot(lrModel.weights.toArray, p.lrFeatures))
  }
  
  def dtEvaluate(sc: SparkContext, cookieid: String, p: Position) =
  {
    if (cookieid != preCookie) {
      dtModel = DecisionTreeModel.load(sc, "/home/team016/middata/model/lr/" + cookieid)
      preCookie = cookieid
    }
    dtModel.predict(Vectors.dense(p.lrFeatures))
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
    val conf = new SparkConf().setAppName("Evaluate ")
    val sc = new SparkContext(conf)
    
    val sep = "\t"
    
    val test_data = sc.textFile("/home/team016/middata/test_user_position/")
                      .sortBy(_.split(sep, 2)(0))
                      .toLocalIterator
                      .map(record => evaluate(sc, record))
                      .map { case (cookieid,infoid,score) => cookieid + sep + infoid + sep + score }
    sc.stop()
  }
}