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
import org.apache.spark.mllib.classification.SVMModel
import java.util.concurrent.Executors
import java.util.concurrent.Callable

/**
 * 评分
 * @author jiangzhenxing
 */
object Evaluate 
{

  def lrdtsvmEvaluateTraindataPart(sc: SparkContext, partCookiePath: String, scorePath: String) =
  {
//    val sep = "\001"
    val testCookies = sc.textFile(partCookiePath).collect.toSet
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = Map[String, String]() // util.cmcLocal(sc)
//    val log = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/team016/shenji/spark/lrdtsvmEvaluateTraindataPart.error")))
    val some = "XyPGPhF_u-GyRgFBwHPzuNRzPZ6DNE"
    testCookies.map { cookieid =>
        println(cookieid)
        val bjobcates = sc.broadcast(userJobCates.getOrElse(cookieid, Array()))
        val blocals = sc.broadcast(userLocals.getOrElse(cookieid, Array()))
        
        try {
          val train_data = sc.textFile("/home/team016/middata/stage2/traindatabyuser_split/train20/" + cookieid)
          val lrmodel = LogisticRegressionModel.load(sc, "/home/team016/middata/stage2/model80/lr/all/" + cookieid)
          val dtmodel = if (cookieid != some) DecisionTreeModel.load(sc, "/home/team016/middata/stage2/model80/dt_laste/all/" + cookieid) else null
          val svmmodel = SVMModel.load(sc, "/home/team016/middata/stage2/model80/svm/all/" + cookieid)
          
          val result = train_data.map(_.split("\001"))
                                   .map { values => 
                                           val action = values(3)
                                           val position = Position(values.slice(5,28))
                                           val enterprice = Enterprise(values.slice(28, 49))
                                           position.enterprise = enterprice
                                           (position.infoid, label(action), position.lrFeatures(blocals.value, bjobcates.value)) }
                                    .repartition(10)
                                    .toLocalIterator
                                    .map { case (infoid, label, features) => 
                                      val lrscore = util.logistic(util.vecdot(lrmodel.weights.toArray, features))
                                      val dtscore = if (dtmodel != null) dtmodel.predict(Vectors.dense(features)) else 0
                                      val svmscore = util.vecdot(svmmodel.weights.toArray, features)
                                      Array(cookieid, infoid, lrscore, dtscore, svmscore, label).mkString("\t") }
                                    
          sc.parallelize(result.toSeq).saveAsTextFile(scorePath + "/" + cookieid)
        } catch {
          case t: Throwable => t.printStackTrace()
                               print(cookieid + " in: " + partCookiePath)
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
  
  /**
   * 使用逻辑归模型对测试数据进行打分
   * (count: 2034327, mean: 0.369164, stdev: 0.228072, max: 0.997375, min: 0.000012)
   */
  def lrEvaluate(sc: SparkContext) =
  {
//    val sep = "\001"
    var preCookieid = ""
    var model: LogisticRegressionModel = null
     
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata/") // 2034327
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = Map[String, String]() // util.cmcLocal(sc)
    var numRecord = 0
    var numUser = 0
    
    val scores = test_data.repartition(1000)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                            numRecord += 1
                            println("eval " + numRecord)
                            val values = record.split("\001")
                            val cookieid = values(0)
                            val infoid = values(1)
                            if (cookieid != preCookieid) {
                              numUser += 1
                              println("load " + numUser + " :" + cookieid)
                              model = LogisticRegressionModel.load(sc, "/home/team016/middata/stage2/model/lr_raw/all/" + cookieid)
                              preCookieid = cookieid
                            }
                            var score = 0.5
                            if (values.length > 15) {
                              val position = Position(values.slice(2, 25))
                              val enterprise = Enterprise(values.slice(25, 46))
                              position.enterprise = enterprise 
                              val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()))
                              score = util.logistic(util.vecdot(model.weights.toArray, feature))
                            }
                            Array(cookieid, infoid, score).mkString("\t")
                          }
                          
        
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/stage2/result/lr2/")
  }
  
  
  def dtEvaluate(sc: SparkContext) =
  {
//    val sep = "\t"
    var preCookieid = ""
    var model: DecisionTreeModel = null
    
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata/")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
    val some = "XyPGPhF_u-GyRgFBwHPzuNRzPZ6DNE"
    var n = 0
    val scores = test_data.repartition(1000)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             val infoid = values(1)
                             n += 1
                             println("complete " + n + " ==============")
                             if (cookieid == some) {
                               Array(cookieid, infoid, 0).mkString("\t")
                             } else {
                               if (cookieid != preCookieid) {
                                model = DecisionTreeModel.load(sc, "/home/team016/middata/stage2/model80/dt_laste/all/" + cookieid)
                                preCookieid = cookieid
                                }
                               var score = 0.5
                               if (values.length > 15) {
                                  val position = Position(values.slice(2, 25))
                                  val enterprise = Enterprise(values.slice(25, 46))
                                  position.enterprise = enterprise 
                                  val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()))
                                  score = model.predict(Vectors.dense(feature))
                                }
                               Array(cookieid, infoid, score).mkString("\t")
                             }
                          }
                          
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/stage2/result/dt80_2/")
  }
  
  /**
   * (count: 2034327, mean: 0.221116, stdev: 0.354659, max: 2.000000, min: 0.000000)
   */
  def dtEvaluatePart(sc: SparkContext, part: Int) =
  {
//    val sep = "\t"
    var preCookieid = ""
    var model: DecisionTreeModel = null
    
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata8/part-0000" + part + ".lzo")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
//    val some = "XyPGPhF_u-GyRgFBwHPzuNRzPZ6DNE"
    var n = 0
    val scores = test_data.repartition(1000)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             val infoid = values(1)
                             n += 1
                             println("complete " + n + " ==============")
                             
                               if (cookieid != preCookieid) {
                                model = DecisionTreeModel.load(sc, "/home/team016/middata/stage2/model/dt_last/all/" + cookieid)
                                preCookieid = cookieid
                                }
                               var score = 0.5
                               if (values.length > 15) {
                                  val position = Position(values.slice(2, 25))
                                  val enterprise = Enterprise(values.slice(25, 46))
                                  position.enterprise = enterprise 
                                  val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()))
                                  score = model.predict(Vectors.dense(feature))
                                }
                               Array(cookieid, infoid, score).mkString("\t")
                             }
                          
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/stage2/result/dtpart/part" + part)
  }
  
  /**
   * 使用决策树回归对测试数据打分
   * (count: 2034327, mean: 0.016249, stdev: 0.125015, max: 2.000000, min: 0.000000)
   
  def dtEvaluate(sc: SparkContext) =
  {
//    val sep = "\t"
    var preCookieid = ""
    var model: DecisionTreeModel = null
    
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata/")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
    val some = "XyPGPhF_u-GyRgFBwHPzuNRzPZ6DNE"
    val scores = test_data.repartition(1000)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             val infoid = values(1)
                             
                             if (cookieid == some) {
                               Array(cookieid, infoid, 0).mkString("\t")
                             } else {
                               if (cookieid != preCookieid) {
                                model = DecisionTreeModel.load(sc, "/home/team016/middata/stage2/model80/dt_laste/all/" + cookieid)
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
                          }
                          
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/stage2/result/dt/")
  }
  */
  
  /**
   * (count: 2034327, mean: -0.489476, stdev: 0.979158, max: 3.914118, min: -9.493875)
   */
  def svmEvaluate(sc: SparkContext) =
  {
//    val sep = "\t"
    var preCookieid = ""
    var model: SVMModel = null
    
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata/")
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
                     
    val scores = test_data.repartition(1000)
                          .sortBy(_.split("\001", 2)(0))
                          .toLocalIterator
                          .map { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             val infoid = values(1)
                             if (cookieid != preCookieid) {
                                model = SVMModel.load(sc, "/home/team016/middata/stage2/model3/svm/all/" + cookieid)
                                preCookieid = cookieid
                              }
                             var score = 0.5
                             if (values.length > 15) {
                                val position = Position(values.slice(2, 25))
                                val enterprise = Enterprise(values.slice(25, 46))
                                position.enterprise = enterprise 
                                val feature = position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()))
                                score = util.vecdot(model.weights.toArray, feature) // model.predict(Vectors.dense(feature))
                              }
                             Array(cookieid, infoid, score).mkString("\t")
                          }
                          
    sc.parallelize(scores.toSeq, 1)
      .saveAsTextFile("/home/team016/middata/stage2/result/svm/")
  }
  
  /**
   * (count: 504310, mean: 1.411102, stdev: 0.130589, max: 1.882736, min: 0.100000)
   */
  def resumeEvaluate(sc: SparkContext) =
  {
    // cookieid + \t + resume
    val users = sc.textFile("/home/team016/middata/stage2/test_user_resume/", 100)
                     .map(_.split("\t"))
                     .map { case Array(cookieid, resume) => (cookieid, resume) }
                     .groupByKey
                     .map { case (cookieid, resumeIter) => (cookieid, User(cookieid=cookieid, resumes=resumeIter.map(Resume(_)).toSeq)) }
//                     .collectAsMap
                     
    val test_data = sc.textFile("/home/team016/middata/stage2/testdata/", 100)
     
    val cmcLocals = util.cmcLocal(sc)
    val cmcCates = util.cmcCates(sc)
    
    val positions = test_data.flatMap { record =>
                             val values = record.split("\001")
                             val cookieid = values(0)
                             
                             if (values.length > 15) {
                                val position = Position(values.slice(2, 25))
                                val enterprise = Enterprise(values.slice(25, 46))
                                position.enterprise = enterprise 
                                Some(cookieid, position)
                              } else {
                                None
                              }
                          }
                          
    positions.leftOuterJoin(users)
             .map { case (cookieid, (position, user)) => cookieid + "\t" + position.infoid + "\t" + (if (user == None) 0 else 2-user.get.matches(position, cmcCates, cmcLocals)) }
             .saveAsTextFile("/home/team016/middata/stage2/result/resume2/")
  }
  
//  def dtEvaluate(sc: SparkContext, cookieid: String, p: Position) =
//  {
//    if (cookieid != preCookie) {
//      dtModel = DecisionTreeModel.load(sc, "/home/team016/middata/stage2/model/dt2/" + cookieid)
//      preCookie = cookieid
//    }
//    dtModel.predict(Vectors.dense(p.lrFeatures))
//  }
  
  /**
   * (count: 2034327, mean: 2.406893, stdev: 2.272690, max: 33.921478, min: 0.000000)
   */
  def clickEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    val testData = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage2/testdata/") // 961957
                     .map(_.split("\001"))
                     .map(values => (values(1), values(0))) // infoid, cookieid
                     
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")  // infoid, avg_click (count: 9699254, mean: 2.176162, stdev: 5.021383, max: 1546.000000, min: 1.000000)
                     .map(_.split("\001"))
                     .map(values => (values(0), values(1)))
                     
    testData.leftOuterJoin(avgClick)
//            .map { case (infoid, (cookieid, score)) => Array(cookieid, infoid, if (score == None) "-" else score.get).mkString("\t") }
            .map { case (infoid, (cookieid, score)) => Array(cookieid, infoid, if (score == None) 0 else math.sqrt(score.get.toDouble)).mkString("\t") /* 开平方做一下缩放 */ }
            .repartition(1)
            .saveAsTextFile("/home/team016/middata/stage2/result/click_evaluate") // (count: 2034327, mean: 2.406893, stdev: 2.272690, max: 33.921478, min: 0.000000)
    /*
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
            .saveAsTextFile("/home/team016/middata/stage2/result/click_evaluate") // 961957
            */
  }
  
  def clickEvaluateTrainDataAll(sc: SparkContext) =
  {
    val sep = "\t"
    
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")
                     .map(_.split("\001"))
                     .map(values => (values(0), values(1).toDouble))
                     .collectAsMap()  // infoid, avg_click
                     
   val bavgClick = sc.broadcast(avgClick)
   
   sc.textFile("/home/team016/middata/stage2/traindatabyuser_split/train20/*")
          .repartition(1000)
          .mapPartitions(iter => iter.map { record => 
                     val values = record.split("\001")
                     val cookieid = values(0)
                     val action = values(3)
                     val position = Position(values.slice(5,28))
                     Array(cookieid, position.infoid, math.sqrt(bavgClick.value.getOrElse(position.infoid, 0)), label(action)).mkString("\t")  
                })
          .saveAsTextFile("/home/team016/middata/stage2/train_result/click/")
  }
  
  def clickEvaluateTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    
    val testCookies = sc.textFile("/home/team016/middata/stage2/test_cookies").collect.toSet
    
//    val trainInfo = sc.textFile("/home/team016/middata/stage2/traindatabyuser/") // 961957
//                     .map(_.split("\001")(2))
//                     .distinct
//                     .collect
//                     .toSet   // infoid
                     
    val avgClick = sc.textFile("/home/team016/middata/avg_position_click/")
                     .map(_.split("\001"))
//                     .filter(values => trainInfo.contains(values(0)))
                     .map(values => (values(0), values(1).toDouble))
                     .collectAsMap()  // infoid, avg_click
                     
   val bavgClick = sc.broadcast(avgClick)
   testCookies.map { cookieid =>
        println(cookieid)
        sc.textFile("/home/team016/middata/stage2/traindatabyuser_split/train20/" + cookieid)
          .repartition(8)
          .mapPartitions(iter => iter.map { record => 
                     val values = record.split("\001")
                     val action = values(3)
                     val position = Position(values.slice(5,28))
                     Array(cookieid, position.infoid, math.sqrt(bavgClick.value.getOrElse(position.infoid, 0)), label(action)).mkString("\t")  
                })
//          .map(_.split("\001"))
//          .map { values => 
//                   val action = values(3)
//                   val position = Position(values.slice(5,28))
//                   Array(position.infoid, math.sqrt(bavgClick.value.getOrElse(position.infoid, 0)), label(action)).mkString("\t") }
          .saveAsTextFile("/home/team016/middata/stage2/train_result/clickbyuser2/" + cookieid)
    }
  }
  
  /**
   * (count: 1828996, mean: 0.058850, stdev: 0.130629, max: 3.922073, min: -0.674005)
   */
  def cfEvaluate(sc: SparkContext) =
  {
    val sep = "\t"
    val model = MatrixFactorizationModel.load(sc, "home/team016/middata/model/cf/")
    
//    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
//                     .map(_.split("\001"))  // cookieid,infoid
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .map(values => (values(0), values(1).toInt)) // cookieid, index
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .map(values => (values(0), values(1).toInt)) // infoid, index
                        
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage2/testdata/")
                     .map(_.split("\001")) .map(values => (values(0), values(1)))   // cookieid, infoid
                     .join(cookieid_code)
                     .map { case (cookieid, (infoid, cookieid_index)) => (infoid, cookieid_index) }
                     .join(infoid_code)
                     .map { case (infoid, (cookieid_index, infoid_index)) => (cookieid_index, infoid_index) }
                     .repartition(100)
//    val testdata = sc.textFile("/home/team016/middata/stage2/testdata_with_code/")
//                     .map(_.split(sep).map(_.toInt))
//                     .map(values => (values(0), values(1)))
    model.predict(testdata)
         .map(rating => Array(rating.user, rating.product, rating.rating).mkString(sep))
         .saveAsTextFile("/home/team016/middata/stage2/result/cf")  // (count: 865381, mean: 0.058398, stdev: 0.130565, max: 4.127115, min: -0.468521)
  }
  
  def cfEvaluateTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    val model = MatrixFactorizationModel.load(sc, "home/team016/middata/model/cf/")
    
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .map(values => (values(0), values(1).toInt)) // cookieid, index
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .map(values => (values(0), values(1).toInt)) // infoid, index
                        
    val train_data = sc.textFile("/home/team016/middata/stage2/traindatabyuser_split/train20/*") // r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime,position, enterprise
                       .map(_.split("\001"))
                       .map(values => (values(0),values(2)))  // cookieid, infoid
                       .join(cookieid_code)
                       .map(_._2)    // infoid, cookiecode
                       .join(infoid_code)
                       .map(_._2)  // cookie_code, info_code
                       .repartition(100)
                       
    val code_cookieid = cookieid_code.map { case (cookieid, code) => (code, cookieid) }
    val code_infoid = infoid_code.map { case (infoid, code) => (code, infoid) }
    
    model.predict(train_data)
         .map(rating => (rating.user, (rating.product, rating.rating)))
         .join(code_cookieid)
         .map { case (user, ((info, rating), cookieid)) => (info, (cookieid, rating)) }
         .join(code_infoid)
         .map { case (info, ((cookieid, rating), infoid)) => Array(cookieid, infoid, rating).mkString("\t") }
         .saveAsTextFile("/home/team016/middata/stage2/train_result/cf/")
  }
  
  /**
   * 对train20的分数进行关联，并按用户保存
   * 如果存在重复数据，标签取最大值
   */
  def evaluateTrainDataByUser(sc: SparkContext) =
  {
    
  }
  
  /**
   *  (count: 2034327, mean: -2.449442, stdev: 2.523254, max: 21.599605, min: -112.980436)
   */
  def evaluateComprehensive(sc: SparkContext) =
  {
    // cookieid weights
    val weightsMap = sc.textFile("/home/team016/middata/stage2/model3/comprehensive_svm/part/*")
                    .map(_.split("\t"))
                    .map { case Array(cookieid, weights) => (cookieid, weights.split(",").map(_.toDouble)) }
                    .collectAsMap
    val bweightsMap = sc.broadcast(weightsMap)
    
    // cookieid, infoid, lrscore, dtscore, svmscore, cfscore, clickscore
    val allScore = sc.textFile("/home/team016/middata/stage2/result/allscore/")
    allScore.repartition(20)
            .mapPartitions(iter => 
                            iter.map(_.split("\t"))
                                .map { case Array(cookieid, infoid, lrscore, dtscore, svmscore, cfscore, clickscore) => 
                                            cookieid + "\t" + infoid + "\t" + 
                                            util.vecdot(weightsMap.getOrElse(cookieid, Array(0,0,0,0,0)), Array(lrscore, dtscore, svmscore, cfscore, clickscore).map(_.toDouble)) })
            .saveAsTextFile("/home/team016/middata/stage2/result/comprehensive_svm")
  }
  
  def evaluateLRClick(sc: SparkContext) =
  {
    val sep = "\t"

    // (count: 949258, mean: 0.420419, stdev: 0.226644, max: 0.999548, min: 0.000039)
    val lrscore = sc.textFile("/home/team016/middata/stage2/result/lr") // 961957 - 12699 = 949258
                    .map(_.split(sep))
//                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    // (count: 961957, mean: 0.129696, stdev: 0.180484, max: 1.000000, min: 0.000000)
    // 0.1
    val clickscore = sc.textFile("/home/team016/middata/stage2/result/click_evaluate/") // 961957
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    lrscore.leftOuterJoin(clickscore)
           .map { case (cookie_info, (lr_score, op_cl_score)) => 
                     val cl_score = if (op_cl_score == None) 0d else op_cl_score.get.toDouble
                     cookie_info + sep + Range(1, 2).map(i => lr_score.toDouble + cl_score * i / 100).mkString(sep) }
           .repartition(1)
//           .toLocalIterator
//           .foreach(line => writer.write(line + "\n"))
           .saveAsTextFile("/home/team016/middata/stage2/result/lr_cl")
  }
  
  def evaluate(sc: SparkContext) =
  {
    val sep = "\t"

    // (count: 949258, mean: 0.420419, stdev: 0.226644, max: 0.999548, min: 0.000039)
    val lrscore = sc.textFile("/home/team016/middata/stage2/result/lr2") // 961957 - 12699 = 949258
                    .map(_.split(sep))
//                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    // (count: 949258, mean: 0.241173, stdev: 0.339714, max: 1.000000, min: 0.000000)
    val dtscore = sc.textFile("/home/team016/middata/stage2/result/dt80part/*") // 961957 - 12699 = 949258
                    .map(_.split(sep))
                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) } 
    val svmscore = sc.textFile("/home/team016/middata/stage2/result/svm/") // 961957 - 12699 = 949258
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) } 
    // (count: 865381, mean: 0.058398, stdev: 0.130565, max: 4.127115, min: -0.468521)
    val cfscore = sc.textFile("/home/team016/middata/stage2/result/cf") // 865381
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    // (count: 961957, mean: 0.129696, stdev: 0.180484, max: 1.000000, min: 0.000000)
    // 0.1
    val clickscore = sc.textFile("/home/team016/middata/stage2/result/click_evaluate/") // 961957
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    
    val resumescore = sc.textFile("/home/team016/middata/stage2/result/resume/") // 961957
                    .map(_.split(sep))
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) }
    
    val lrclscore = sc.textFile("/home/team016/middata/stage2/result/lr_cl/") // 961957 - 12699 = 949258
                    .map(_.split(sep))
                    .filter(_(1) != "--")
                    .map { case Array(cookieid,infoid,score) => (cookieid + sep + infoid, score) } 
    
    lrscore.leftOuterJoin(resumescore)
           .map { case (cookie_info, (lr_score, op_resume_score)) => 
                     val resume_score = if (op_resume_score == None) 0d else op_resume_score.get.toDouble
                     cookie_info + sep + (lr_score.toDouble + 2 - resume_score * 0.1) }
           .repartition(10)
           .saveAsTextFile("/home/team016/middata/stage2/result/lr_resume")
           
    lrscore.leftOuterJoin(dtscore)
           .map { case (cookie_info, (lr_score, op_cl_score)) => 
                     val cf_score = if (op_cl_score == None) 0d else op_cl_score.get.toDouble
                     cookie_info + sep + Range(1, 10).map(i => lr_score.toDouble + cf_score * i / 10).mkString(sep) }
           .repartition(1)
           .saveAsTextFile("/home/team016/middata/stage2/result5/lr_dt")
    
    lrscore.leftOuterJoin(clickscore)
           .map { case (cookie_info, (lr_score, op_cl_score)) => 
                     val cl_score = if (op_cl_score == None) 0d else op_cl_score.get.toDouble
                     cookie_info + sep + Range(1, 2).map(i => lr_score.toDouble + (if (cl_score < 1) 0 else math.log(cl_score)) * i / 100).mkString(sep) }
           .repartition(10)
//           .toLocalIterator
//           .foreach(line => writer.write(line + "\n"))
           .saveAsTextFile("/home/team016/middata/stage2/result/lr_cl")
           
    lrscore.leftOuterJoin(cfscore)
           .map { case (cookie_info, (lr_score, op_cf_score)) => 
                     val cf_score = if (op_cf_score == None) 0d else op_cf_score.get.toDouble
                     cookie_info + sep + Range(1, 2).map(i => lr_score.toDouble + cf_score * i / 50).mkString(sep) }
           .repartition(10)
           .saveAsTextFile("/home/team016/middata/stage2/result/lr_cf")
           
    lrscore.join(dtscore)
           .leftOuterJoin(cfscore)
           .join(clickscore)
           .map { case (cookie_info, (((lr_score, dtscore), cf_score), click_score)) => 
                     cookie_info + sep + Range(1, 21).map(i => lr_score.toDouble + dtscore.toDouble * i / 10).mkString(sep) }
           .repartition(10)
           .saveAsTextFile("/home/team016/middata/stage2/result5/lrdtcfcl")
           
    Range(1,10).foreach{
            i =>
            val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/team016/shenji/result/result_jiangzhenxing_20161118_v4lrdt" + i + ".txt")))
            sc.textFile("/home/team016/middata/stage2/result5/lr_dt/")
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
    sc.setLogLevel("WARN")
    
    val sep = "\t"
    
    if (args(0).toUpperCase() == "CF")
      cfEvaluate(sc)
      
    if (args(0).toUpperCase() == "LR")
      lrEvaluate(sc)
    
    if (args(0).toUpperCase() == "DT")
      dtEvaluate(sc)
      
    if (args(0).toUpperCase() == "DTPART")
      dtEvaluatePart(sc, args(1).toInt)
      
    if (args(0).toUpperCase() == "SVM")
      svmEvaluate(sc)
      
    if (args(0).toUpperCase() == "LRDTSVM_TRAIN_PART")
      lrdtsvmEvaluateTraindataPart(sc, 
          "/home/team016/middata/stage2/test_cookies_split10/part" + args(1), 
          "/home/team016/middata/stage2/train_result/lrdtsvmbyuser/part" + args(1))
    
    if (args(0).toUpperCase() == "CLICK_TRAIN")
      clickEvaluateTrainDataAll(sc)
      
    if (args(0).toUpperCase() == "CF_TRAIN")
      cfEvaluateTrainData(sc)
      
    if (args(0) == "Comprehensive") {
      evaluateComprehensive(sc)
    }
    
    if (args(0) == "clickEvaluate")
      clickEvaluate(sc)
      
    if (args(0) == "evaluateLRClick")
      evaluateLRClick(sc)
      
    if (args(0) == "resume")
      resumeEvaluate(sc)
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