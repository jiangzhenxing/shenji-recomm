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
import com.bj58.shenji.util
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner

/**
 * 评分
 */
object Evaluate 
{
  
//  var lrModel: LogisticRegressionModel = null;
//  var dtModel: DecisionTreeModel = null
//  var preCookie: String = ""
  
//  def evaluate(sc: SparkContext, record: String, 
//                click_score: scala.collection.Map[String, Double], 
//                lrModels: scala.collection.Map[String, LogisticRegressionModel],
//                dtModels: scala.collection.Map[String, DecisionTreeModel]) =
//  {
//    val sep = "\t"
//    val values = record.split(sep)
//    val cookieid = values(0)
//    
//    var scorelr = 0.5
//    var scoredt = 0.5
//    var scoreclick = 0d
//    var score = 0.5
//    
//    try {
//      scoreclick = click_score(cookieid + sep + values(1))
//      if (values.length > 15) {
//        val p = position(values)
//        val lrModel = lrModels(cookieid)
//        val dtModel = dtModels(cookieid)
//        scorelr = logistic(vecdot(lrModel.weights.toArray, p.lrFeatures))
//        scoredt = dtModel.predict(Vectors.dense(p.lrFeatures))
//        score = scorelr + scoredt * 0.4 + scoreclick * 0.1
//      }
//    } catch {
//      case t: Throwable => t.printStackTrace()
//    }
//    (values(0), values(1), scorelr, scoredt, scoreclick, score)
//  }
  
  def lrdtEvaluateTraindata(sc: SparkContext) =
  {
//    val sep = "\001"
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect.toSet
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = Map[String, String]() // util.cmcLocal(sc)
    
    testCookies.map { cookieid =>
        println(cookieid)
        val bjobcates = sc.broadcast(userJobCates.getOrElse(cookieid, Array()))
        val blocals = sc.broadcast(userLocals.getOrElse(cookieid, Array()))
        
        try {
          val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
          val lrmodel = LogisticRegressionModel.load(sc, "/home/team016/middata/model5/lr/" + cookieid)
          val dtmodel = DecisionTreeModel.load(sc, "/home/team016/middata/model5/dt/" + cookieid)
          
          val result = train_data.map(_.split("\001"))
                                   .map { values => 
                                           val action = values(3)
                                           val position = Position(values.slice(5,28))
                                           val enterprice = Enterprise(values.slice(28, 49))
                                           position.enterprise = enterprice
                                           (position.infoid, label(action), position.lrFeatures(blocals.value, bjobcates.value, cmcLocal)) }
                                    .toLocalIterator
                                    .map { case (infoid, label, features) => (infoid, label, util.logistic(util.vecdot(lrmodel.weights.toArray, features)), dtmodel.predict(Vectors.dense(features))) }
                                    .map { case (infoid, label, lrscore, dtscore) => Array(cookieid, infoid, lrscore, dtscore, label).mkString("\t") }
          sc.parallelize(result.toSeq).saveAsTextFile("/home/team016/middata/result10/lrdt/" + cookieid)
        } finally {
            bjobcates.destroy()
            blocals.destroy()
        }
    }
  }
  
  def label(action: String) =
  {
    action match {
      case "seetel" => 1    // 100
      case "message" => 1
      case "apply" => 1
      case "1" => 1
      case "0" => 0
      case _ => 0
    }
  }
  
  def lrEvaluate(sc: SparkContext) =
  {
//    val sep = "\001"
    var preCookieid = ""
    var model: LogisticRegressionModel = null
    
    val test_data = sc.textFile("/home/team016/middata/testdata/")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = Map[String, String]() // util.cmcLocal(sc)
                           
    val scores = test_data.repartition(4)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                            val values = record.split("\001")
                            val cookieid = values(0)
                            val infoid = values(1)
                            if (cookieid != preCookieid) {
                              model = LogisticRegressionModel.load(sc, "/home/team016/middata/model5/lr/" + cookieid)
                              preCookieid = cookieid
                            }
                            var score = 0.5
                            if (values.length > 15) {
                              val position = Position(values.slice(2, 25))
                              val enterprise = Enterprise(values.slice(25, 46))
                              position.enterprise = enterprise 
                              val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()), cmcLocal)
                              score = util.logistic(util.vecdot(model.weights.toArray, feature))
                            }
                            Array(cookieid, infoid, score).mkString("\t")
                          }
                          
        
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/result5/lr/")
  }
  
  def dtEvaluate(sc: SparkContext) =
  {
//    val sep = "\t"
    var preCookieid = ""
    var model: DecisionTreeModel = null
    
    val test_data = sc.textFile("/home/team016/middata/testdata/")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
                     
    val scores = test_data.repartition(4)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             val infoid = values(1)
                             if (cookieid != preCookieid) {
                                model = DecisionTreeModel.load(sc, "/home/team016/middata/model5/dt/" + cookieid)
                                preCookieid = cookieid
                              }
                             var score = 0.5
                             if (values.length > 15) {
                                val position = Position(values.slice(2, 25))
                                val enterprise = Enterprise(values.slice(25, 46))
                                position.enterprise = enterprise 
                                val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()), cmcLocal)
                                score = model.predict(Vectors.dense(feature))
                              }
                             Array(cookieid, infoid, score).mkString("\t")
                          }
                          
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/result5/dt/")
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
    val testData = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/") // 961957
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
  
  
  def clickEvaluateTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect.toSet
    
    val testInfo = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/") // 961957
                     .map(_.split("\001")(1))
                     .collect
                     .toSet   // infoid
                     
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")
                     .map(_.split("\001"))
                     .filter(values => testInfo.contains(values(0)))
                     .map(values => (values(0), values(1)))
                     .collectAsMap()  // infoid, avg_click
                     
   testCookies.map { cookieid =>
        println(cookieid)
        val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
          
        val result = train_data.map(_.split("\001"))
                  .map { values => 
                         val action = values(3)
                         val position = Position(values.slice(5,28))
                         (position.infoid, label(action)) }
                  .toLocalIterator
                  .map { case (infoid, label) =>
                         Array(infoid, avgClick.getOrElse(infoid, "0"), label).mkString("\t") }
                  
         sc.parallelize(result.toSeq).saveAsTextFile("/home/team016/middata/result10/click/" + cookieid)
    }
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
  
  def cfEvaluateTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    val model = MatrixFactorizationModel.load(sc, "home/team016/middata/model/cf/")
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect.toSet
    
    val testInfo = sc.textFile("/home/team016/middata/traindatabyuser2/") // 961957
                     .map(_.split("\001"))
                     .map(values => Position(values.slice(5,28)).infoid)
                     .distinct
                     .collect
                     .toSet   // infoid
                     
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .filter(values => testCookies.contains(values(0)))
                          .map(values => (values(0), values(1).toInt)) // cookieid, index
                          .collectAsMap()
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .filter(values => testInfo.contains(values(0)))
                        .map(values => (values(0), values(1).toInt)) // infoid, index
                        .collectAsMap()
                        
    testCookies.map { cookieid =>
        val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
        
        val rawdata = train_data.map(_.split("\001"))
                                .flatMap { values => 
                                     val action = values(3)
                                     val position = Position(values.slice(5,28))
                                     val infoid = position.infoid
                                     
                                     val user = cookieid_code.getOrElse(cookieid, -1)
                                     val info = infoid_code.getOrElse(infoid, -1)
                                     
                                     if (user < 0 || info < 0)
                                       None
                                     else
                                       Some((cookieid, infoid, user, info, label(action))) }
                                 .cache()
                                 
        val userInfos = rawdata.map { case (cookieid, infoid, user, info, label) => (user, info) }
                               .repartition(32)
        val alias = rawdata.map { case (cookieid, infoid, user, info, label) => (user + ":" + info, (cookieid, infoid, label)) }
                           .repartition(32)
        model.predict(userInfos)
             .map(rating => (rating.user + ":" + rating.product, rating.rating))
             .join(alias)
             .mapValues { case (rating, (cookieid, infoid, label)) => Array(cookieid, infoid, rating, label).mkString("\t") }
             .map(_._2)
             .saveAsTextFile("/home/team016/middata/result10/cf/" + cookieid)
             
       rawdata.unpersist(false)
    }
  }
  
  
  def cfEvaluateTrainDataCon(sc: SparkContext) =
  {
    val sep = "\t"
    val model = MatrixFactorizationModel.load(sc, "home/team016/middata/model/cf/")
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect.toSet
    
    val testInfo = sc.textFile("/home/team016/middata/traindatabyuser2/") // 961957
                     .map(_.split("\001"))
                     .map(values => Position(values.slice(5,28)).infoid)
                     .distinct
                     .collect
                     .toSet   // infoid
                     
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .filter(values => testCookies.contains(values(0)))
                          .map(values => (values(0), values(1).toInt)) // cookieid, index
                          .collectAsMap()
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .filter(values => testInfo.contains(values(0)))
                        .map(values => (values(0), values(1).toInt)) // infoid, index
                        .collectAsMap()
                        
    testCookies.map { cookieid =>
        val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
        
        val rawdata = train_data.map(_.split("\001"))
                                .flatMap { values => 
                                     val action = values(3)
                                     val position = Position(values.slice(5,28))
                                     val infoid = position.infoid
                                     
                                     val user = cookieid_code.getOrElse(cookieid, -1)
                                     val info = infoid_code.getOrElse(infoid, -1)
                                     
                                     if (user < 0 || info < 0)
                                       None
                                     else
                                       Some((cookieid, infoid, user, info, label(action))) }
                                 .cache()
                                 
        val userInfos = rawdata.map { case (cookieid, infoid, user, info, label) => (user, info) }
                               .repartition(32)
        val alias = rawdata.map { case (cookieid, infoid, user, info, label) => (user + ":" + info, (cookieid, infoid, label)) }
                           .repartition(32)
        model.predict(userInfos)
             .map(rating => (rating.user + ":" + rating.product, rating.rating))
             .join(alias)
             .mapValues { case (rating, (cookieid, infoid, label)) => Array(cookieid, infoid, rating, label).mkString("\t") }
             .map(_._2)
             .saveAsTextFile("/home/team016/middata/result10/cf/" + cookieid)
             
       rawdata.unpersist(false)
    }
  }
  
  def evaluate(sc: SparkContext) =
  {
    val sep = "\t"

    // (count: 949258, mean: 0.420419, stdev: 0.226644, max: 0.999548, min: 0.000039)
    val lrscore = sc.textFile("/home/team016/middata/result5/lr") // 961957 - 12699 = 949258
                    .map(_.split(sep))
                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    // (count: 949258, mean: 0.241173, stdev: 0.339714, max: 1.000000, min: 0.000000)
    val dtscore = sc.textFile("/home/team016/middata/result/dt2") // 961957 - 12699 = 949258
                    .map(_.split(sep))
                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) } 
    // (count: 865381, mean: 0.058398, stdev: 0.130565, max: 4.127115, min: -0.468521)
    val cfscore = sc.textFile("/home/team016/middata/result/cf") // 865381
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    // (count: 961957, mean: 0.129696, stdev: 0.180484, max: 1.000000, min: 0.000000)
    // 0.1
    val clickscore = sc.textFile("/home/team016/middata/click_evaluate/") // 961957
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    
    lrscore.leftOuterJoin(dtscore)
           .map { case (cookie_info, (lr_score, op_cl_score)) => 
                     val cf_score = if (op_cl_score == None) 0d else op_cl_score.get.toDouble
                     cookie_info + sep + Range(1, 10).map(i => lr_score.toDouble + cf_score * i / 10).mkString(sep) }
           .repartition(1)
           .saveAsTextFile("/home/team016/middata/result5/lr_dt")
           
    lrscore.leftOuterJoin(clickscore)
           .map { case (cookie_info, (lr_score, op_cl_score)) => 
                     val cf_score = if (op_cl_score == None) 0d else op_cl_score.get.toDouble
                     cookie_info + sep + Range(1, 10).map(i => lr_score.toDouble + cf_score * i / 10).mkString(sep) }
           .repartition(1)
           .saveAsTextFile("/home/team016/middata/result5/lr_cl")
           
    lrscore.leftOuterJoin(cfscore)
           .map { case (cookie_info, (lr_score, op_cf_score)) => 
                     val cf_score = if (op_cf_score == None) 0d else op_cf_score.get.toDouble
                     cookie_info + sep + Range(1, 21).map(i => lr_score.toDouble + cf_score * i / 10).mkString(sep) }
           .repartition(1)
           .saveAsTextFile("/home/team016/middata/result/lr_cf")
           
    lrscore.join(dtscore)
           .leftOuterJoin(cfscore)
           .join(clickscore)
           .map { case (cookie_info, (((lr_score, dtscore), cf_score), click_score)) => 
                     cookie_info + sep + Range(1, 21).map(i => lr_score.toDouble + dtscore.toDouble * i / 10).mkString(sep) }
           .repartition(1)
           .saveAsTextFile("/home/team016/middata/result5/lrdtcfcl")
           
    Range(1,10).foreach{
            i =>
            val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/team016/shenji/result/result_jiangzhenxing_20161118_v4lrdt" + i + ".txt")))
            sc.textFile("/home/team016/middata/result5/lr_dt/")
            .map(_.split("\t"))
            .map(values => Array(values(0), values(1), values(1 + i)).mkString("\t"))
            .toLocalIterator
            .foreach(line => writer.write(line + "\n"))
            writer.close()
           }
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
             highlights = "",
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
      
    if (args(0).toUpperCase() == "LRDT_TRAIN")
      lrdtEvaluateTraindata(sc)
      
    if (args(0).toUpperCase() == "CLICK_TRAIN")
      clickEvaluateTrainData(sc)
      
    if (args(0).toUpperCase() == "CF_TRAIN")
      cfEvaluateTrainData(sc)
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