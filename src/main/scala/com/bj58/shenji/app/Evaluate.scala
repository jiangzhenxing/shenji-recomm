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
    
    var scorelr = 0.5
    var scoredt = 0.5
    var score = 0.5
    
    if (values.length > 15) {
      val p = position(values)
      scorelr = lrEvaluate(sc, cookieid, p)
      scoredt = dtEvaluate(sc, cookieid, p)
      score = scorelr + scoredt * 0.4
    }
    
    (values(0), values(1), scorelr, scoredt, score)
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
  
  def clickEvaluate(sc: SparkContext) =
  {
    val testData = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
                     .map(_.split("\001"))
                     .map(values => (values(1), values(0))) // infoid, cookieid
                     
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")  // infoid, avg_click
                     .map(_.split("\001"))
                     .map(values => (values(0), values(1)))
    testData.leftOuterJoin(avgClick)
            .map { case (infoid, (cookieid, score)) => Array(cookieid, infoid, if (score == None) "-" else score.get).mkString("\t") }
            .repartition(1)
            .saveAsTextFile("/home/team016/resultdata/result1") // 961957
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
    val out = new File("/home/team016/shenji/result/result_jiangzhenxing_20161113_v3.txt")
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out)))
    val test_data = sc.textFile("/home/team016/middata/test_user_position/")
                      .sortBy(_.split(sep, 2)(0))
                      .toLocalIterator
                      .map(record => evaluate(sc, record))
                      .map { case (cookieid,infoid,scorelr, scoredt, score) => cookieid + sep + infoid + sep + scorelr + sep + scoredt + sep + score }
                      .foreach(r => writer.write(r + "\n"))
    writer.close()
    sc.stop()
  }
}