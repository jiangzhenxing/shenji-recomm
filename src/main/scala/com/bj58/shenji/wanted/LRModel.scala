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
    val conf = new SparkConf().setAppName("Train").set("spark.driver.allowMultipleContexts", "true")
    
    val localsc = new ThreadLocal[SparkContext]() {
      
      override def initialValue() = {  
           new SparkContext(conf) 
        } 
    }
    val sc1 = new SparkContext(conf)
    // (0,19 155 442) (1,637829)
//    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
//    // 87895
//    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
//    val train_data = sc.textFile("/home/team016/middata/test_train_data/")
    val testCookies = sc1.textFile("/home/team016/middata/test_cookies").collect
    
    sc1.stop()
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
//    val unionRecords = list_position.union(action_position).sortBy(record => { val values = record.split(sep); values(0) + sep + values(4) }, true, 15)
    
    val executor = Executors.newFixedThreadPool(12)
    
//    val accum = sc.accumulator(0, "SUCESS_COUNT")
    
//    testCookies.map { cookieid =>
//      println(cookieid)
//      try {
//                              val train_data = sc.textFile("/home/team016/middata/traindatabyuser/" + cookieid + "-*")
//                              val rawdatas = train_data.map(_.split("\t")).map(values => (values(3), position(values)))
//                              val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
//                              val bactionCount = sc.broadcast(actionCount)
//                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
//                              try {
//                                LogisticRegressionWithSGD.train(datas, 250, 2)
//                                                         .save(sc, "/home/team016/middata/model/lr2/" +cookieid)
//                                DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity="variance", maxDepth=10, maxBins=36)
//                                            .save(sc, "/home/team016/middata/model/dt2/" + cookieid) // 9:0.6448457311761356
//                                accum += 1
//                              } finally {
//                                datas.unpersist(true)
//                                bactionCount.destroy()
//                              }
//                      } catch {
//                               case t: Throwable => logger.error(cookieid, t)
//                     }
//                        executor.submit(new Callable[String]() {
//                          def call = {
//                            try {
//                              val sc = localsc.get
//                              val train_data = sc.textFile("/home/team016/middata/traindatabyuser/" + cookieid + "-*")
//                              val rawdatas = train_data.map(_.split("\t")).map(values => (values(3), position(values)))
//                              val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap
//                              val bactionCount = sc.broadcast(actionCount)
//                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, bactionCount.value) }.cache // .sortBy(_(4).toLong)
//                              try {
//                                LogisticRegressionWithSGD.train(datas, 250, 2)
//                                                         .save(sc, "/home/team016/middata/model/lr4/" +cookieid)
//                                DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity="variance", maxDepth=10, maxBins=36)
//                                          .save(sc, "/home/team016/middata/model/dt4/" + cookieid) // 9:0.6448457311761356
////                                accum += 1
//                                } finally {
//                                datas.unpersist(true)
//                                bactionCount.destroy()
//                              }
//                              cookieid
//                            } catch {
//                               case t: Throwable => throw new RuntimeException(cookieid, t)
//                            }
//                          }
//                    })
//                  }
//            .foreach(f => try { logger.info(f.get + " has complete in lrmodel") } catch { case e: Exception => logger.error(e.getMessage + "  in lrmodel", e) } )
    
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
//    executor.awaitTermination(10, TimeUnit.DAYS)
//    sc.stop()
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
  
  def testDatas(sc: SparkContext, testCookies: Set[String],
                userLocals: scala.collection.Map[String, Array[String]],
                userJobCates: scala.collection.Map[String, Array[String]],
                cmcLocal: scala.collection.Map[String, String]): scala.collection.Map[String, Array[(String,Array[Double])]] = //
  {
    val sep = "\001"
    var preCookieid = ""
    var model: LogisticRegressionModel = null
    val bcookies = sc.broadcast(testCookies)
    val test_data = sc.textFile("/home/team016/middata/testdata/")
                           
    test_data.map(_.split(sep))
             .filter(_.length > 2)
             .filter(values => bcookies.value.contains(values(0)))
             .groupBy(_(0)) // cookieid + sep + infoid +  (if (pe == None) "--" else position + sep + enterprise) 1 + 1 + 23 + 21
             .map { case (cookieid, iter) => 
                      val features = iter.map { values =>
                                     val position = Position(values.slice(2, 25))
                                     val enterprise = Enterprise(values.slice(25, 46))
                                     position.enterprise = enterprise 
                                     (position.infoid, position.lrFeatures(userLocals.getOrElse(cookieid, Array()), userJobCates.getOrElse(cookieid, Array()), cmcLocal)) }
                              .toArray 
                      (cookieid, features) }
             .collectAsMap
  }
  
  def lrEval(model: LogisticRegressionModel, datas: Array[Position],
            locals: Array[String], jobcates: Array[String], 
            cmcLocal: scala.collection.Map[String, String], actionCount: scala.collection.Map[String, Int]) =
  {
    datas.map(position => (position.infoid, util.logistic(util.vecdot(model.weights.toArray,position.lrFeatures(locals, jobcates, cmcLocal)))))
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
    
    testCookies.map { cookieid =>
      executor.submit(new Callable[String]() with Serializable {
                          def call = {
                            println(cookieid)
                            try {
                              val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
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
                              
                              val actionCount = rawdatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
                              val jobcates = userJobcates.getOrElse(cookieid, Array())
                              val locals = userLocals.getOrElse(cookieid, Array())
                              
                              val bactionCount = sc.broadcast(actionCount)
                              val bjobcates = sc.broadcast(jobcates)
                              val blocals = sc.broadcast(locals)
                              
                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value,bcmcLocal.value, bactionCount.value) }.cache // .sortBy(_(4).toLong)
                              try {
                                LogisticRegressionWithSGD.train(datas, 250, 2)
                                                         .save(sc, lroutput + "/" + cookieid)
//                                val lrscore = testData(cookieid).map(infoposition => (infoposition._1, util.logistic(util.vecdot(lrmodel.weights.toArray,infoposition._2))))
//                                lrscores.put(cookieid, lrscore)
                                //                                  
                                DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity = "variance", maxDepth = 10, maxBins = 36)
                                            .save(sc, dtoutput + "/" + cookieid) // 9:0.6448457311761356
//                                val dtscore = testData(cookieid).map(position => (position._1, dtmodel.predict(Vectors.dense(position._2))))
//                                dtscores.put(cookieid, dtscore)
                                cookieid + " complete!"
                              } finally {
                                datas.unpersist(true)
                                bactionCount.destroy()
                                bjobcates.destroy()
                                blocals.destroy()
                              }
                            } catch {
                              case t: Throwable => { t.printStackTrace(); cookieid + " error: " + t.getMessage } // logger.error("", t)
                            }
                          }
                    })
    }.foreach { f => println(f.get) }
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
  
  
  def trainAndEval(sc: SparkContext, cookiePath: String) =
  {
    val sep = "\t"
    val testCookies = sc.textFile(cookiePath).collect.toSet
    val userJobcates = util.userJobcates(sc)
    val userLocals = util.userLocals(sc)
    val cmcLocal = util.cmcLocal(sc)
    val bcmcLocal = sc.broadcast(cmcLocal)
    val testData = testDatas(sc, testCookies, userLocals, userJobcates, cmcLocal)
    
    val lrscores = new ConcurrentHashMap[String,Array[(String,Double)]]()
    val dtscores = new ConcurrentHashMap[String,Array[(String,Double)]]()
    
    testCookies.map { cookieid =>
                            println(cookieid)
                            try {
                              val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
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
                              
                              val actionCount = rawdatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
                              val jobcates = userJobcates.getOrElse(cookieid, Array())
                              val locals = userLocals.getOrElse(cookieid, Array())
                              
                              val bactionCount = sc.broadcast(actionCount)
                              val bjobcates = sc.broadcast(jobcates)
                              val blocals = sc.broadcast(locals)
                              
                              val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value,bcmcLocal.value, bactionCount.value) }.cache // .sortBy(_(4).toLong)
                              try {
                                val lrmodel = LogisticRegressionWithSGD.train(datas, 250, 2)
                                val lrscore = testData(cookieid).map(infoposition => (infoposition._1, util.logistic(util.vecdot(lrmodel.weights.toArray,infoposition._2))))
                                lrscores.put(cookieid, lrscore)
                                //                                  .save(sc, lroutput + "/" + cookieid)
                                val dtmodel = DecisionTree.trainRegressor(datas, Map[Int, Int](), impurity = "variance", maxDepth = 10, maxBins = 36)
//                                  .save(sc, dtoutput + "/" + cookieid) // 9:0.6448457311761356
                                val dtscore = testData(cookieid).map(position => (position._1, dtmodel.predict(Vectors.dense(position._2))))
                                dtscores.put(cookieid, dtscore)
                                cookieid + " complete!"
                              } finally {
                                datas.unpersist(true)
                                bactionCount.destroy()
                                bjobcates.destroy()
                                blocals.destroy()
                              }
                            } catch {
                              case t: Throwable => { t.printStackTrace(); cookieid + " error: " + t.getMessage } // logger.error("", t)
                            }
    }
    
    sc.parallelize(lrscores.entrySet().toArray.toSeq)
      .flatMap { entry => val e = entry.asInstanceOf[Entry[String,Array[(String,Double)]]]
                  e.getValue.map { case (infoid, score) => e.getKey + "\t" + infoid + "\t" + score } }
      .saveAsTextFile("/home/team016/middata/result7/lr")
      
    sc.parallelize(dtscores.entrySet().toArray)
      .flatMap { entry => val e = entry.asInstanceOf[Entry[String,Array[(String,Double)]]]
                  e.getValue.map { case (infoid, score) => e.getKey + "\t" + infoid + "\t" + score } }
      .saveAsTextFile("/home/team016/middata/result7/dt")
  }
  
  def trainAll(sc: SparkContext) =
  {
    // /home/team016/middata/model5/
//    train2(sc, "/home/team016/middata/test_cookies",
//              "/home/team016/middata/model5/lr",
//              "/home/team016/middata/model5/dt")
//    trainAndEval(sc, "/home/team016/middata/test_cookies")
    trainConcurrent(sc, "/home/team016/middata/test_cookies", 
                        "/home/team016/middata/model9/lr",
                        "/home/team016/middata/model9/dt")
  }
  
  def trainPart(sc: SparkContext, part: Int) =
  {
    trainConcurrent(sc, "/home/team016/middata/test_cookies_split4/part" + part, 
                        "/home/team016/middata/model3/lr/part" + part,
                        "/home/team016/middata/model3/dt/part" + part)
//    train2(sc, "/home/team016/middata/test_cookies_split4/part" + part,
//              "/home/team016/middata/model/lr6/part" + part,
//              "/home/team016/middata/model/dt6/part" + part)
  }
  
  def train2(sc: SparkContext, cookiePath: String, lroutput: String, dtoutput: String) =
  {
      val sep = "\t"
      val testCookies = sc.textFile(cookiePath).collect
      val userJobcates = util.userJobcates(sc)
      val locals = util.userLocals(sc)
      val cmcLocal = util.cmcLocal(sc)
      val bcmcLocal = sc.broadcast(cmcLocal)
      // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
      // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
      val accum = sc.accumulator(0, "SUCESS_COUNT")

      testCookies.map { cookieid =>
        println(cookieid)
        try {
          val train_data = sc.textFile("/home/team016/middata/traindatabyuser2/" + cookieid + "-*")
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
                                      
          val actionCount = rawdatas.map { case (action, position) => (action, 1) }.reduceByKey(_ + _).collectAsMap
          val bactionCount = sc.broadcast(actionCount)
          val bjobcates = sc.broadcast(userJobcates.getOrElse(cookieid, Array()))
          val blocals = sc.broadcast(locals.getOrElse(cookieid, Array()))
          
          val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, blocals.value, bjobcates.value,bcmcLocal.value, bactionCount.value) }.cache // .sortBy(_(4).toLong)
          try {
            LogisticRegressionWithSGD.train(input=datas, numIterations=250, stepSize=2)
              .save(sc, lroutput + "/" + cookieid)
            DecisionTree.trainRegressor(input=datas, categoricalFeaturesInfo=Map[Int, Int](), impurity="variance", maxDepth=10, maxBins=36)
              .save(sc, dtoutput + "/" + cookieid) // 9:0.6448457311761356
            accum += 1
          } finally {
            datas.unpersist(true)
            bactionCount.destroy()
            bjobcates.destroy()
            blocals.destroy()
          }
        } catch {
          case t: Throwable => logger.error(cookieid, t)
        }
      }
        .foreach(c => println(c + " has complete in lrmodel"))
      sc.stop()
  }
  
  def labeledPoints(action: String, position: Position, 
                    locals: Array[String], jobcates: Array[String], 
                    cmcLocal: scala.collection.Map[String, String], actionCount: scala.collection.Map[String, Int]) =
  {
    val features = Vectors.dense(position.lrFeatures(locals, jobcates, cmcLocal))
    
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
  
//  def labeledPoint(values: Array[String]) =
//  {
//    val p = position(values)
//    val features = Vectors.dense(p.lrFeatures)
//    
//    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
//    val label = values(3) match {
//      case "seetel" => 1.0
//      case "message" => 1.0
//      case "apply" => 1.0
//      case "1" => 1.0
//      case "0" => 0.0
//      case _ => 0.0
//    }
//    
//    LabeledPoint(label, features)
//  }
  
//  def labelFetures(values: Array[String]) =
//  {
//    val p = position(values)
//    val features = p.lrFeatures
//    
//    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
//    val label = values(3) match {
//      case "seetel" => 1.0
//      case "message" => 1.0
//      case "apply" => 1.0
//      case "1" => 1.0
//      case "0" => 0.0
//      case _ => 0.0
//    }
//    
//    (label, features)
//  }
  
//  def position(values: Array[String]) = 
//  {
//    Position(infoid = values(2),
//						 scate1 = values(6),
//						 scate2 = values(7),
//						 scate3 = values(8),
//						 title = values(9),
//						 userid = values(5),
//						 local = values(10),
//						 salary = values(11),
//						 education = values(12),
//						 experience = values(13),
//						 trade = values(14),
//						 enttype = values(15),
//						 fresh = values(17),
//						 fuli = values(16),
//						 highlights = "",
//						 additional = values(18)
//						 )
//  }
  
  def main(args: Array[String]): Unit = 
  {
    
    println("cookieid,userid,infoid,clicktag,clicktime,userid,scate1,scate2,scate3,title,local,salary,education,experience,trade,enttype,fuli,fresh,additional".split(",").zipWithIndex.mkString(","))
  }
}