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
 * 逻辑回归模型
 */
object LRModel extends Serializable
{
  val logger = LoggerFactory.getLogger("LRModel")
   
  def trainAll(sc: SparkContext) =
  {
    // /home/team016/middata/model5/
//    train2(sc, "/home/team016/middata/stage2/test_cookies",
//              "/home/team016/middata/stage2/model/lr",
//              "/home/team016/middata/stage2/model/svm",
//              "/home/team016/middata/stage2/model/dt")
//    trainAndEval(sc, "/home/team016/middata/test_cookies")
    trainConcurrent(sc, "/home/team016/middata/stage2/test_cookies", 
                        "/home/team016/middata/stage2/model/lr_raw",
                        "/home/team016/middata/stage2/model/dt")
  }
  
  def trainPart(sc: SparkContext, part: Int) =
  {
//    trainConcurrent(sc, "/home/team016/middata/test_cookies_split4/part" + part, 
//                        "/home/team016/middata/model3/lr/part" + part,
//                        "/home/team016/middata/model3/dt/part" + part)
    train2(sc, "/home/team016/middata/stage2/test_cookies_split10/part" + part,
              "/home/team016/middata/stage2/model/lr_clean/part" + part,
              "/home/team016/middata/stage2/model/svm/part" + part,
              "/home/team016/middata/stage2/model/dt/part" + part)
  }
    
  def train2(sc: SparkContext, cookiePath: String, lroutput: String, svmoutput: String, dtoutput: String) =
  {
      val sep = "\t"
      val testCookies = sc.textFile(cookiePath).collect
      val userJobcates = util.userJobcates(sc)
      val locals = util.userLocals(sc)
      val cmcLocal = Map[String, String]()
      val bcmcLocal = sc.broadcast(cmcLocal)
      // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
      // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
      val accum = sc.accumulator(0, "SUCESS_COUNT")
      var n = 0
      testCookies.map { cookieid =>
        n += 1
        println(cookieid + ": " + n + "********************************")
        try {
          val train_data = sc.textFile("/home/team016/middata/stage2/traindatabyuser/" + cookieid + "-*")
          /*
           * // 5 + 23 + 21 detail:0-5; position:5-25; enterprise:29-49
             .map { values => val p = Position(values.slice(5,29))
           */
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
          
          val cleandatas = notclick.subtractByKey(clicks).union(clicks).map(_._2)
          println("cleandatas count is " + cleandatas.count())
          
          val actionCount = cleandatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
          val bjobcates = sc.broadcast(userJobcates.getOrElse(cookieid, Array()))
          val blocals = sc.broadcast(locals.getOrElse(cookieid, Array()))
          
          try {
            val needBase = needBaseCount(actionCount)
            println("needBase is " + needBase)
            val lrdatas = cleandatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value,bcmcLocal.value, needBase) }.cache // .sortBy(_(4).toLong)
            rawdatas.unpersist(false)
            println(lrdatas.count())
            
            LogisticRegressionWithSGD.train(input=lrdatas, numIterations=250, stepSize=2)
                                     .save(sc, lroutput + "/" + cookieid)
              
//            SVMWithSGD.train(input=lrdatas, numIterations=200)
//              .save(sc, svmoutput + "/" + cookieid)
//              
            lrdatas.unpersist(false)
//            
//            val (base1, base2) = needBaseCountDT(actionCount)
//            val dtdatas = rawdatas.flatMap { case (action, position) => 
//                                              labeledPointsDT(action, position, blocals.value, bjobcates.value,bcmcLocal.value, base1, base2) }.cache // .sortBy(_(4).toLong)
//            DecisionTree.trainRegressor(input=dtdatas, categoricalFeaturesInfo=Map[Int, Int](), impurity="variance", maxDepth=10, maxBins=36)
//              .save(sc, dtoutput + "/" + cookieid) // 9:0.6448457311761356
//              
//            dtdatas.unpersist(false)
            accum += 1
            cookieid
          } finally {
            bjobcates.destroy()
            blocals.destroy()
          }
        } catch {
          case t: Throwable => logger.error(cookieid, t)
        }
      }
        .foreach(c => println(c + " has complete in lrmodel @" + new Date))
  }
  
  def trainConcurrent(sc: SparkContext, cookiePath: String, lroutput: String, dtoutput: String) =
  {
    val executor = Executors.newFixedThreadPool(16)
    val sep = "\t"
    val testCookies = sc.textFile(cookiePath).collect.toSet
    val userJobcates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
    val bcmcLocal = sc.broadcast(cmcLocal)
//    val testData = testDatas(sc, testCookies, userLocals, userJobcates, cmcLocal)
    
//    val lrscores = new ConcurrentHashMap[String,Array[(String,Double)]]()
//    val dtscores = new ConcurrentHashMap[String,Array[(String,Double)]]()
    val count = new AtomicInteger(0)
    var complete = 0
    
    testCookies.map { cookieid =>
      executor.submit(new Callable[String]() with Serializable {
                          def call = {
                            val num = count.incrementAndGet()
                            println("train " + cookieid + " 第 " + num + " ****************************")
                            try {
                              val train_data = sc.textFile("/home/team016/middata/stage2/traindatabyuser/" + cookieid + "-*")
                              /*
                               * // 5 + 23 + 21 detail:0-5; position:5-25; enterprise:29-49
                                 .map { values => val p = Position(values.slice(5,29))
                               */
                              val rawdatas = train_data.map(_.split("\001"))
                                                       .map { values => 
                                                               val action = values(3)
                                                               val position = Position(values.slice(5,28))
                                                               val enterprice = Enterprise(values.slice(28, 49))
                                                               position.enterprise = enterprice
                                                               (action, position) }
//                                                        .cache()
                                                        
//                              val notclick = rawdatas.filter(_._1 == "0").map { case (action, position) => (position.infoid, (action, position)) }
//                              val clicks = rawdatas.filter(_._1 != "0").map { case (action, position) => (position.infoid, (action, position)) }
                              
//                              val cleandatas = notclick.subtractByKey(clicks).union(clicks).map(_._2)
                              
                              val actionCount = rawdatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
                              val needBase = needBaseCount(actionCount)
                              
                              val jobcates = userJobcates.getOrElse(cookieid, Array())
                              val locals = userLocals.getOrElse(cookieid, Array())
                              
                              val bjobcates = sc.broadcast(jobcates)
                              val blocals = sc.broadcast(locals)
                              
                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value,bcmcLocal.value, needBase) }
                                                  .cache // .sortBy(_(4).toLong)
                              try {
                                LogisticRegressionWithSGD.train(datas, 250, 2)
                                                         .save(sc, lroutput + "/" + cookieid)
//                                val lrscore = testData(cookieid).map(infoposition => (infoposition._1, util.logistic(util.vecdot(lrmodel.weights.toArray,infoposition._2))))
//                                lrscores.put(cookieid, lrscore)
                                // 
//                                DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity = "variance", maxDepth = 10, maxBins = 36)
//                                            .save(sc, dtoutput + "/" + cookieid) // 9:0.6448457311761356
//                                val dtscore = testData(cookieid).map(position => (position._1, dtmodel.predict(Vectors.dense(position._2))))
//                                dtscores.put(cookieid, dtscore)
                                cookieid + " complete!"
                              } finally {
                                datas.unpersist(true)
                                bjobcates.destroy()
                                blocals.destroy()
                              }
                            } catch {
                              case t: Throwable => { t.printStackTrace(); cookieid + " error: " + t.getMessage } // logger.error("", t)
                            }
                          }
                    })
    }.foreach { f => { complete += 1; println("complete " + f.get + " 第 " + complete + " ##################") } }
    executor.shutdown()
    
//    sc.parallelize(lrscores.entrySet().toArray.toSeq)
//      .flatMap { entry => val e = entry.asInstanceOf[Entry[String,Array[(String,Double)]]]
//                  e.getValue.map { case (infoid, score) => e.getKey + "\t" + infoid + "\t" + score } }
//      .saveAsTextFile("/home/team016/middata/result6/lr")
//      
//    sc.parallelize(dtscores.entrySet().toArray)
//      .flatMap { entry => val e = entry.asInstanceOf[Entry[String,Array[(String,Double)]]]
//                  e.getValue.map { case (infoid, score) => e.getKey + "\t" + infoid + "\t" + score } }
//      .saveAsTextFile("/home/team016/middata/result6/dt")
  }
  
  def labeledPoints(action: String, position: Position, 
                    locals: Array[String], jobcates: Array[String], 
                    cmcLocal: scala.collection.Map[String, String], needBase: Int) =
  {
    val features = Vectors.dense(position.lrFeatures(locals, jobcates, cmcLocal))
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
  
  def labeledPointsDT(action: String, position: Position, 
                    locals: Array[String], jobcates: Array[String], 
                    cmcLocal: scala.collection.Map[String, String], 
                    base1: Int, base2: Int) =
  {
    val features = Vectors.dense(position.lrFeatures(locals, jobcates, cmcLocal))
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    action match {
      case "seetel" => Range(0, base2).map(x => LabeledPoint(2, features)).toArray    // 100
      case "message" => Range(0, base2).map(x => LabeledPoint(2, features)).toArray
      case "apply" => Range(0, base2).map(x => LabeledPoint(2, features)).toArray
      case "1" => Range(0, base1).map(x => LabeledPoint(1, features)).toArray        //40
      case "0" => Array(LabeledPoint(0, features))
      case _ => Array(LabeledPoint(0, features))
    }
  }
  
  
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
  
  def needBaseCountDT(actionCount: scala.collection.Map[String, Int]) =
  {
    val seetelCount = actionCount.getOrElse("seetel", 0).doubleValue
    val messageCount = actionCount.getOrElse("message", 0).doubleValue
    val applyCount = actionCount.getOrElse("apply", 0).doubleValue
    
    val deliveryCount = (seetelCount + messageCount + applyCount).doubleValue // 这部分相当于投递数量
    val clickCount = actionCount.getOrElse("1", 0).doubleValue  // 点击数量
    val notClickCount = actionCount.getOrElse("0", 0).doubleValue     // 末点击数量
    
    val base1 = if (notClickCount > 0) math.round(notClickCount / clickCount).intValue() else 1
    val base2 = if (deliveryCount > 0) math.round(notClickCount / deliveryCount).intValue() else 1
    
    (base1, base2)
//    val actionTotal = (deliveryCount + clickCount)  // 正样本数量 = 投递数量 + 点击数量
//    val needNum = noCount - actionTotal             // 需要过分抽样的数量
    
    // 过分抽样，让正负样本的数量相当
//    val needBase = (if (deliveryCount == 0) math.round(needNum / actionTotal).intValue else math.floor(needNum / actionTotal).intValue) + 1
//    needBase
  }
  
}