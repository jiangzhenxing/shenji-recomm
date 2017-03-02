package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.tree.DecisionTree

import org.apache.spark.SparkConf
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import org.dmg.pmml.True

import com.bj58.shenji.data.Position
import com.bj58.shenji.util
import com.bj58.shenji.data.Enterprise
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.jboss.netty.util.internal.ConcurrentHashMap
import java.util.Map.Entry
import org.apache.spark.mllib.classification.SVMWithSGD
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

/**
 * 逻辑回归模型训练
 * @author jiangzhenxing
 */
object LRModel extends Serializable
{
  val logger = LoggerFactory.getLogger("LRModel")
  val minPartitions = 24  // 最小分区数，对于计算密集型任务，可设为集群CPU核数
  
  def train(sc: SparkContext, cookiePath: String, lroutput: String) =
  {
      val sep = "\t"
      val testCookies = sc.textFile(cookiePath).collect
      val userJobCates = util.userJobcates(sc)
      val userLocals = util.userLocals(sc)
      // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
      // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
      val accum = sc.accumulator(0, "SUCESS_COUNT")
      var n = 0
      testCookies.foreach { cookieid =>
        n += 1
        println(cookieid + ": " + n + "********************************")
        val jobCates = userJobCates.getOrElse(cookieid, Array())
        val locals = userLocals.getOrElse(cookieid, Array())
        trainUser(sc, cookieid, jobCates, locals, lroutput)
        println(cookieid + " has complete in lrmodel @" + new Date)
      }
  }
  
  /**
   * 在一个任务中使用多个线程同时进行训练，每个线程训练一个用户的模型
   * 但在实际运行中好像会出现异常，可能是spark不支持多线程并发执行
   */
  def trainConcurrent(sc: SparkContext, cookiePath: String, lroutput: String) =
  {
    val executor = Executors.newFixedThreadPool(16)
    val sep = "\t"
    val testCookies = sc.textFile(cookiePath).collect.toSet
    val userJobCates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    
    testCookies.map { cookieid =>
      executor.submit(new Runnable() {
                          def run = {
                            val jobCates = userJobCates.getOrElse(cookieid, Array())
                            val locals = userLocals.getOrElse(cookieid, Array())
                            trainUser(sc, cookieid, jobCates, locals, lroutput)
                          }
                    })
    }
    
    executor.awaitTermination(100, TimeUnit.DAYS)
  }
  
  /**
   * 对一个用户进行训练
   */
  def trainUser(sc: SparkContext, cookieid: String, jobcates: Array[String], locals: Array[String], lroutput: String) =
  {
    try {
          val train_data = sc.textFile("/home/team016/middata/stage2/traindatabyuser_split/train80/" + cookieid, minPartitions)
          // 5 + 23 + 21 detail:0-5; position:5-28; enterprise:28-49
          val rawdatas = train_data.map(_.split("\001"))
                                   .map { values => 
                                           val action = values(3)
                                           val position = Position(values.slice(5,28))
                                           val enterprice = Enterprise(values.slice(28, 49))
                                           position.enterprise = enterprice
                                           (action, position) }
                                   .cache()
//          println("rawdatas count is " + rawdatas.count())
          val notclick = rawdatas.filter(_._1 == "0").map { case (action, position) => (position.infoid, (action, position)) }
          val clicks = rawdatas.filter(_._1 != "0").map { case (action, position) => (position.infoid, (action, position)) }
          // 从未点击记录中的帖子把已点击帖子清除掉，但效果好像并不明显
          val cleandatas = notclick.subtractByKey(clicks).union(clicks).map(_._2)
          println("cleandatas count is " + cleandatas.count())
          
          val actionCount = cleandatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
          val bjobcates = sc.broadcast(jobcates)
          val blocals = sc.broadcast(locals)
          
          try {
            val needBase = needBaseCount(actionCount)
            println("needBase is " + needBase)
            
            val lrdatas = cleandatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value, needBase) }
                                    .cache // .sortBy(_(4).toLong)
            println(lrdatas.count())
            
            LogisticRegressionWithSGD.train(input=lrdatas, numIterations=250, stepSize=2)
                                     .save(sc, lroutput + "/" + cookieid)
              
            lrdatas.unpersist(false)
          } finally {
            bjobcates.destroy()
            blocals.destroy()
          }
        } catch {
          case t: Throwable => logger.error(cookieid, t)
        }
  }
  
  /**
   * 产生数据的LabelPoint，同时进行过分抽样
   */
  def labeledPoints(action: String, position: Position, 
                    locals: Array[String], jobcates: Array[String], 
                    needBase: Int) =
  {
    val features = Vectors.dense(position.lrFeatures(locals, jobcates))
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    action match {
      case "seetel" => Range(0,needBase * 2).map(x => LabeledPoint(1, features)).toArray    // 100
      case "message" => Range(0,needBase * 2).map(x => LabeledPoint(1, features)).toArray
      case "apply" => Range(0,needBase * 2).map(x => LabeledPoint(1, features)).toArray
      case "1" => Range(0,needBase).map(x => LabeledPoint(1, features)).toArray        //40
      case "0" => Array(LabeledPoint(0, features))
      case _ => Array(LabeledPoint(0, features))
    }
  }
  
  /**
   * 需要过分抽样的倍数
   */
  def needBaseCount(actionCount: scala.collection.Map[String, Int]) =
  {
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
    needBase
  }
}