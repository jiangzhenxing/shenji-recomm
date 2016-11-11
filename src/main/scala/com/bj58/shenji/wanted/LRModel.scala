package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.SparkConf

import com.bj58.shenji.data.Position
import org.apache.spark.SparkConf
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

/**
 * 逻辑回归模型
 */
object LRModel extends Serializable
{
  /**
   * (HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)
   */
  val logger = LoggerFactory.getLogger("LRModel")
  
  def train(sc: SparkContext) = 
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
//    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
//    // 87895
//    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    val train_data = sc.textFile("/home/team016/middata/test_train_data/")
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
//    val unionRecords = list_position.union(action_position).sortBy(record => { val values = record.split(sep); values(0) + sep + values(4) }, true, 15)
    
    val executor = Executors.newFixedThreadPool(32)
    
    testCookies.map { cookieid =>
                        executor.submit(new Callable[String]() {
                          def call = {
                            try {
                              val train_data = sc.textFile("/home/team016/middata/traindatabyuser/" + cookieid + "-*")
                              val rawdatas = train_data.map(_.split("\t")).map(values => (values(3), position(values)))
                              val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
                              val bactionCount = sc.broadcast(actionCount)
                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
                              LogisticRegressionWithSGD.train(datas, 250, 2).save(sc, "/home/team016/middata/model/lr/" +cookieid)
                              cookieid
                            } catch {
                               case t: Throwable => throw new RuntimeException(cookieid)
                            }
                          }
                    })
                  }
            .foreach(f => try { logger.info(f.get + " has complete in lrmodel") } catch { case e: Exception => logger.error(e.getMessage + "  in lrmodel", e) } )
    
//    cookieid => (cookieid, train_data.filter(record => record.substring(0, record.indexOf("\t")) == cookieid)))
//                            .map { case (cookieid, records) => 
//                                      executor.submit(new Callable[String]() {
//                                        def call = {
//                                          try {
//                                            val rawdatas = records.map(_.split("\t")).map(values => (values(3), position(values)))
//                                            val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
//                                            val bactionCount = sc.broadcast(actionCount)
//                                            val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
//                                            LogisticRegressionWithSGD.train(datas, 250, 2).save(sc, "/home/team016/middata/model/lr/" +cookieid)
//  //                                          cookieid + sep + model.weights.toArray.mkString(",")
//                                            cookieid
//                                          } catch {
//                                            case t: Throwable => throw new RuntimeException(cookieid)
//                                          }
//                                        }
//                                      }) }
//                              .foreach(f => try { logger.info(f.get + " has complete") } catch { case e: Exception => logger.error(e.getMessage, e) } )
    executor.shutdown()
//    sc.parallelize(result).map(_.get()).saveAsTextFile("/home/team016/middata/model/lr/")
    
//    list_position.union(action_position)
//                 .groupBy(record => record.substring(0, record.indexOf("\t")))
//                 .map { case (cookieid, records) => 
//                          val config = new SparkConf().setMaster("local").setAppName("Train LR Model")
//                          val sc2 = new SparkContext(config)
//                          val rawdatas = sc2.parallelize(records.toSeq).map(_.split("\t")).map(values => (values(3), position(values)))
//                          val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
//                          val bactionCount = sc2.broadcast(actionCount)
//                          val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache
////                          .map(labeledPoints).cache //.sortBy(_(4).toLong)
////                          datas.cache()
//                          val weights = LogisticRegressionWithSGD.train(datas, 250, 2).weights
//                          sc2.stop()
//                          (cookieid, weights) }
//                 .map { case ((cookieid, weights)) => cookieid + sep + weights.toArray.mkString(",") }
//                 .saveAsTextFile("/home/team016/middata/model/LR")
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
  
  def labeledPoint(values: Array[String]) =
  {
    val p = position(values)
    val features = Vectors.dense(p.lrFeatures)
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    val label = values(3) match {
      case "seetel" => 1.0
      case "message" => 1.0
      case "apply" => 1.0
      case "1" => 1.0
      case "0" => 0.0
      case _ => 0.0
    }
    
    LabeledPoint(label, features)
  }
  
  def labelFetures(values: Array[String]) =
  {
    val p = position(values)
    val features = p.lrFeatures
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    val label = values(3) match {
      case "seetel" => 1.0
      case "message" => 1.0
      case "apply" => 1.0
      case "1" => 1.0
      case "0" => 0.0
      case _ => 0.0
    }
    
    (label, features)
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
  
  def main(args: Array[String]): Unit = 
  {
    
    println("cookieid,userid,infoid,clicktag,clicktime,userid,scate1,scate2,scate3,title,local,salary,education,experience,trade,enttype,fuli,fresh,additional".split(",").zipWithIndex.mkString(","))
  }
}