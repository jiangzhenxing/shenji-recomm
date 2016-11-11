package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.tree.DecisionTree

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Callable

import com.bj58.shenji.data.Position
import org.slf4j.LoggerFactory

/**
 * 决策权回归模型
 */
object DTModel extends Serializable 
{
  val logger = LoggerFactory.getLogger("DTModel")
  
  def train(sc: SparkContext) = 
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
//    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
//    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
//    val unionRecords = list_position.union(action_position)
    
    val executor = Executors.newFixedThreadPool(32)
    
    testCookies.map { cookieid =>
                        executor.submit(new Callable[String]() {
                          def call = {
                            try {
                              val rawdatas = sc.textFile("/home/team016/middata/traindatabyuser/" + cookieid + "-*")
                                               .map(_.split("\t")).map(values => (values(3), position(values)))
                              val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
                              val bactionCount = sc.broadcast(actionCount)
                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
                              DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity="variance", maxDepth=9, maxBins=32)
                                                        .save(sc, "/home/team016/middata/model/dt/" + cookieid) // 9:0.6448457311761356
                              cookieid
                            } catch {
                               case t: Throwable => throw new RuntimeException(cookieid)
                            }
                          }
                    })
                  }
            .foreach(f => try { logger.info(f.get + " has complete in dtmodel") } catch { case e: Exception => logger.error(e.getMessage + " in dtmodel", e) } )
    
//    val result = testCookies.map(cookieid => (cookieid, train_data.filter(record => record.substring(0, record.indexOf("\t")) == cookieid)))
//                            .map { case (cookieid, records) => 
//                                      executor.submit(new Callable[String]() {
//                                        def call = {
//                                          try {
//                                            val rawdatas = records.map(_.split("\t")).map(values => (values(3), position(values)))
//                                            val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
//                                            val bactionCount = sc.broadcast(actionCount)
//                                            val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
//                                            DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity="variance", maxDepth=9, maxBins=32)
//                                                        .save(sc, "/home/team016/middata/model/dt/" + cookieid) // 9:0.6448457311761356
//                                            cookieid
//                                          } catch {
//                                            case t: Throwable => throw new RuntimeException(cookieid)
//                                          }
//                                        }
//                                      }) }
//                             .foreach(f => try { logger.info(f.get + " has complete") } catch { case e: Exception => logger.error(e.getMessage, e) } )
      executor.shutdown()
  }
  
  def position(values: Array[String]) = 
  {
    Position(infoid = values(2),
						 scate1 = values(6),
						 scate2 = values(7),
						 scate3 = values(8),
						 title = values(9),
						 userid = values(5),
						 local = values(10),
						 salary = values(11),
						 education = values(12),
						 experience = values(13),
						 trade = values(14),
						 enttype = values(15),
						 fresh = values(17),
						 fuli = values(16),
						 additional = values(18)
						 )
  }
  
  def labeledPoints(action: String, position: Position, actionCount: scala.collection.Map[String, Int]) =
  {
    val features = Vectors.dense(position.lrFeatures)
    
    val seetelCount = actionCount.getOrElse("seetel", 0).doubleValue
    val messageCount = actionCount.getOrElse("message", 0).doubleValue
    val applyCount = actionCount.getOrElse("apply", 0).doubleValue
    
    val deliveryCount = (seetelCount + messageCount + applyCount).doubleValue // 这部分相当于投递数量
    val clickCount = actionCount.getOrElse("1", 0).doubleValue  // 点击数量
    val noCount = actionCount.getOrElse("0", 0).doubleValue     // 末点击数量
    
    val actionTotal = (deliveryCount + clickCount)  // 正样本数量 = 投递数量 + 点击数量
    val needNum = noCount - actionTotal             // 需要过分抽样的数量
    
    // 过分抽样，让正负样本的数量相当
    val needBase = (if (deliveryCount == 0) math.round(needNum / actionTotal).intValue else math.floor(needNum / actionTotal).intValue) + 1
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    action match {
      case "seetel" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray    // 100
      case "message" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray
      case "apply" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray
      case "1" => Range(0,needBase).map(x => LabeledPoint(1, features)).toArray        //40
      case "0" => Array(LabeledPoint(0, features))
      case _ => Array(LabeledPoint(0, features))
    }
  }
}