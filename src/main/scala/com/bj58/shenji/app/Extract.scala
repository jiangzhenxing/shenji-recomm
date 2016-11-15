package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.HashPartitioner

import scala.collection.mutable.Map
import scala.collection.Set

import java.io._

import com.bj58.shenji.data._
import com.bj58.shenji.util._
import org.apache.hadoop.io.NullWritable
import org.apache.spark.Partitioner

/**
 * 数据抽取
 * @author jiangzhenxing
 * @create 2016-11-06
 */
object Extract 
{
  val field_split = "\t"

  /**
   * 提取测试集中的用户的展示职位数据
   */
  def extractTestDetail(sc: SparkContext, dt: Int) = 
  {
    val testCookies = testCookieSet(sc)
    val bcookies = sc.broadcast(testCookies)
    val sep = "\t"
    val detail = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/detail/dt=" + dt)
                   .map(JobListRecord(_))
                   .filter(r => bcookies.value.contains(r.cookieid))
                   .map(r => (r.infoid,Array(r.cookieid,r.userid,r.infoid,r.clicktag,if (r.clicktag == "1") r.clicktime else r.stime).mkString(sep)))
    
    val position = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=" + dt)
                     .map(Position(_))
                     .map(p => (p.infoid, Array(p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep)))
    
    detail.join(position)
          .map { case (infoid: String, (d: String, p: String)) => d + sep + p }
          .saveAsTextFile("/home/team016/middata/test_list_position/dt" + dt)
  }
  
  /**
   * 提取用户的展示职位数据
   */
  def extractDetail(sc: SparkContext) = 
  {
    val sep = "\t"
    val locals = sc.textFile("/home/team016/middata/area_city").map(_.split("\001")).map(values => (values(0),values(1))).collect.toMap
    val blocals = sc.broadcast(locals)
    
    val detail = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/detail/*")
                   .map(JobListRecord(_))
                   .map(r => (r.infoid,Array(r.cookieid,r.userid,r.infoid,blocals.value.getOrElse(r.sloc1,-1),r.clicktag,if (r.clicktag == "1") r.clicktime else r.stime).mkString(sep)))
                   
    val position = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/*")
                     .map(Position(_))
                     .map(p => (p.infoid, Array(p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep)))
    
    detail.join(position)
          .map { case (infoid: String, (d: String, p: String)) => {
                    val detail = d.split(sep)
                    val position = p.split(sep)
                    (Array(detail(3),position(1), position(2)).mkString(sep), d + sep + p)
                  }}
          .repartitionAndSortWithinPartitions(new HashPartitioner(100))
          .saveAsTextFile("/home/team016/middata/list_position/all")
  }
  
  /**
   * 提取测试集中的用户的点击事件关联职位信息
   */
  def extractAction(sc: SparkContext, dt: Int) = 
  {
    val sep = "\t"
    val testCookies = testCookieSet(sc)
    val bcookies = sc.broadcast(testCookies)
    
    val action = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/useraction/dt=" + dt)
                   .map(UserActionRecord(_))
                   .filter(r => bcookies.value.contains(r.cookieid))
                   .map(r => (r.infoid, Array(r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime).mkString("\t")))
                   
    val position = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=" + dt)
                     .map(Position(_))
                     .map(p => (p.infoid, Array(p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep)))
                     
    action.join(position)
          .map { case (infoid: String, (d: String, p: String)) => d + sep + p }
          .saveAsTextFile("/home/team016/middata/test_action_position/dt" + dt)
  }
  
  /**
   * 提取测试集中的用户的点击事件关联职位信息
   */
  def extractDelivery(sc: SparkContext, dt: Int) = 
  {
    val sep = "\t"
    val testCookies = testCookieSet(sc)
    val bcookies = sc.broadcast(testCookies)
    
    val delivery = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/delivery/dt=" + dt)
                   .map(DeliveryRecord(_))
                   .filter(r => bcookies.value.contains(r.cookieid))
                   .map(r => (r.infoid, Array(r.cookieid, r.resumeuserid, r.infoid, "apply", r.deliverytime).mkString("\t")))
                   
    val position = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=" + dt)
                     .map(Position(_))
                     .map(p => (p.infoid, Array(p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep)))
                     
    delivery.join(position)
          .map { case (infoid: String, (d: String, p: String)) => d + sep + p }
          .saveAsTextFile("/home/team016/middata/test_delivery_position/dt" + dt)
  }
  
  /**
   * 抽取测试数据中的职位信息
   */
  def extractTestPosition(sc: SparkContext) =
  {
    val sep = "\t"
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
                     .map(_.split("\001"))
                     .map(values => (values(1).trim, values(0))) // infoid, cookieid
                     
    val positions = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=16")
                      .map(p => (p.substring(0, p.indexOf("\001")).trim, p))  // infoid, position
                      
    testdata.leftOuterJoin(positions)
            .map { case (infoid, (cookieid,position)) => (cookieid, if (position == None) None else Some(Position(position.get))) }
            .map { case (cookieid, option) =>  if (option == None) cookieid + sep + "--" else { val p = option.get; Array(cookieid,p.infoid,p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep) } }
            .repartition(1)
            .saveAsTextFile("/home/team016/middata/test_user_position/") // 949258  961957 12699
  }
  
  /**
   * 抽取测试数据中的简历信息
   */
  def extractTestResume(sc: SparkContext) =
  {
    val sep = "\t"
    // 找到用户id
    val listUser = (sc.textFile("/home/team016/middata/test_list_position/*")
                     .map(_.split("\t",5))
                     .map(values => (values(1),values(0))) // userid, cookieid
                     .distinct())
                     
    val resume = (sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/resume/*")
                      .map(p => (p.substring(0, p.indexOf("\001")), p))) // userid, resume
    listUser.join(resume)
            .mapValues { case (cookieid, resume) => cookieid + sep + resume }
            .map(_._1)
            .saveAsTextFile("/home/team016/middata/test_user_resume")
//    testdata.join(resume)
//            .map { case (infoid, (cookieid,position)) => (cookieid, Position(position)) }
//            .map { case (cookieid, p) =>  Array(cookieid,p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep) }
//            .saveAsTextFile("/home/team016/middata/test_user_position/")
  }
  
  /**
   * 从测试数据中抽取训练数据(90%)测试数据(10%)
   */
  def extratTestTrain(sc: SparkContext) =
  {
    val testUser = "(HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)"
    val count = testUser.split("), (")
            .map(_.replace("(", "").replace(")",""))
            .map(_.split(","))
            .map(values => (values(0), values(1).toInt))
            
    val userCount = Map[String,Int]() ++= count
    
    val bUserCount = sc.broadcast(userCount)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    
    list_position.union(action_position)
                 
//    test_user_position.filter()
  }
  
  
  def test_all_actionSorted(sc: SparkContext) {
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    list_position.union(action_position)
  }
  
  /**
   * 从测试数据按用户分开保存
   */
  def extratPartUser(sc: SparkContext) =
  {
    val sep = "\t"
    val testUser = "(HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)"
    val count = testUser.split("\\), \\(")
                        .map(_.replace("(", "").replace(")",""))
                        .map(_.split(","))
                        .map(values => (values(0), values(1).toInt))
    val userCount = Map[String,Int]() ++= count
    val testCookies = userCount.keySet
    
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
//    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
//    val testCookies = testdata.map(_.split("\001")(0)).distinct.collect
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    val unionRecords = list_position.union(action_position)
    
    testCookies.map(cookieid => (cookieid,unionRecords.filter(record => record.substring(0, record.indexOf("\t")) == cookieid)))
               .map {case (cookieid, records) => records.cache()
                                                        .sortBy(_.split("\t",7)(4).toLong)
                                                        .saveAsTextFile("/home/team016/middata/test_all_action_by_user/" + cookieid) }
  }
  
  def exactResult(sc: SparkContext) =
  {
    val out = new File("/home/team016/shenji/result/result_jiangzhenxing_20161113_v2.txt")
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out)))
    sc.textFile("/home/team016/resultdata/result1")
      .map(_.split("\t"))
      .map(values => { if (values(2) == "-") values(2) = "3"; values.mkString("\t") } )
      .toLocalIterator
      .foreach(r => writer.write(r + "\n"))
    writer.close()
  }
  
  def copyToLocal(sc: SparkContext) =
  {
    val testUser = "m1NfUhbQujboiZKAEM0zNY7OUYVKuk, m1NfUh3QuhR2NWNduDqWi7uWmdFKuk, m1NfUhbQubPhUbG5yWKpPYFn07FKuk, yb0Qwj7_uRRC2YIREycfRM-jm17ZIk, HZGNrH7_u-FHn7I2rytdEhQsnNOaIk, w-RDugRAubGPNLFWmYNoNgPJnAqvNE, uvVYENdyubQVuRw8pHwuEN65PLKOIk, njRWwDuARMmo0A6amNqCuDwiibRKuk, RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk, m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk, pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE, m1NfUh3QuhcYwNuzyAt30duwXMPKuk, UvqNu7K_uyIgyWR60gDvw7GjPA6GNE, NDwwyBqyugRvuDOOE1EosdR3ERRdNE, m1NfUh3QuA_oIR73N-E30DPlRh6Kuk, RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE, m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk, m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk, uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE"
    testUser.split(", ")
            .map(_.trim)
            .map { user => 
                      val out = new File("/home/team016/shenji/data/" + user)
                      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out)))
                      sc.textFile("/home/team016/middata/test_all_action_by_user/" + user)
                        .toLocalIterator
                        .foreach(r => writer.write(r + "\n"))
                      writer.close() }
  }
  
  def testTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    list_position.union(action_position)
                 .sortBy(record => { val values = record.split(sep); values(0) + sep + values(4) }, true, 5)
                 .saveAsTextFile("/home/team016/middata/test_train_data/")
  }
  
  def splitUser(sc: SparkContext)
  {
    val testCookie = sc.textFile("/home/team016/middata/test_cookies").collect
    val size = 6000
    Range(0, testCookie.size, size).map(begin => testCookie.slice(begin, begin + size))
                                   .zipWithIndex
                                   .foreach { case (values, index) => sc.parallelize(values).saveAsTextFile("/home/team016/middata/test_cookies_split/part" + index) }
  }
  
  /**
   * 测试数据中不同地区和职位
   */
  def testLocalJobCates(sc: SparkContext) = 
  {
    val test_data = sc.textFile("/home/team016/middata/test_train_data/")
    val locals = sc.textFile("/home/team016/middata/area_city").map(_.split("\001")).map(values => (values(0),values(1))).collect.toMap
//    val test_local = test_data.map(_.split("\t")).flatMap(values => values(10).split(",").map(locals.getOrElse(_,"-1"))).distinct.collect
    test_data.map(_.split("\t"))
             .flatMap(values => values(10).split(",").map(lc => Array(locals.getOrElse(lc,"-1"),values(6),values(7)).mkString("\t"))) // city,cate1,cate2,cate3
             .distinct
             .saveAsTextFile("/home/team016/middata/test_local_job2/")
  }
  
  /**
   * 为cookieid和infoid进行规范编码
   */
  def idcode(sc: SparkContext)
  {
    val click_count = sc.textFile("/home/team016/middata/click_count/")
                        .map(_.split("\001"))

    val cookieids = click_count.map(_(0)) // 9276870,9699254
    val infoids = click_count.map(_(1))
    
    cookieids
      .distinct
      .zipWithIndex
      .map { case (cookieid, index) => cookieid + "\t" + index }
      .repartition(10)
      .saveAsTextFile("/home/team016/middata/cookieid_code") // 9276870
      
    infoids
      .distinct
      .zipWithIndex
      .map { case (infoid, index) => infoid + "\t" + index }
      .repartition(10)
      .saveAsTextFile("/home/team016/middata/infoid_code") // 9699254
  }
  
  /**
   * 为click_count中的数据的id转换成编码
   */
  def click_count_with_code(sc: SparkContext)
  {
    val sep = "\t"
    val click_count = sc.textFile("/home/team016/middata/click_count/")
                        .map(_.split("\001")) // cookieid,infoid,click_count
                        
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .map(values => (values(0), values(1))) // cookieid, index
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .map(values => (values(0), values(1))) // infoid, index
                        
    click_count.map(values => (values(0), (values(1), values(2)))) // cookieid, (infoid,count)
               .join(cookieid_code)
               .map { case (cookieid, ((infoid,count), cookieid_index)) => (infoid, (cookieid_index,count)) }
               .join(infoid_code)
               .map { case (infoid, ((cookieid_index,count), infoid_index)) => Array(cookieid_index, infoid_index, count).mkString(sep) }
               .saveAsTextFile("/home/team016/middata/click_count_with_code/")
  }
  
  /**
   * 为测试集中的数据的id转换成编码
   */
  def testdata_with_code(sc: SparkContext)
  {
    val sep = "\t"
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
                     .map(_.split("\001"))  // cookieid,infoid
                     
    val cookieid_code = sc.textFile("/home/team016/middata/cookieid_code/")
                          .map(_.split(sep))
                          .map(values => (values(0), values(1))) // cookieid, index
                          
    val infoid_code = sc.textFile("/home/team016/middata/infoid_code")
                        .map(_.split(sep))
                        .map(values => (values(0), values(1))) // infoid, index
                        
    testdata.map(values => (values(0), values(1)))   // cookieid, infoid
            .join(cookieid_code)
               .map { case (cookieid, (infoid, cookieid_index)) => (infoid, cookieid_index) }
               .join(infoid_code)
               .map { case (infoid, (cookieid_index, infoid_index)) => Array(cookieid_index, infoid_index).mkString(sep) }
               .saveAsTextFile("/home/team016/middata/testdata_with_code/")
  }
  
  
  
  /**
   * 测试数据集中的cookieid
   */
  def testCookieSet(sc: SparkContext) = 
  {
    sc.textFile("/home/team016/middata/test_cookies").collect.toSet
  }
  
//  def sparkContext = new SparkContext(new SparkConf())
  
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Extract " + args(0))
    val sc = new SparkContext(conf)
    
    if (args(0).toLowerCase() == "testdetail")
      Range(1,16).foreach(dt => extractTestDetail(sc, dt))
      
    if (args(0).toLowerCase() == "detail")
      extractDetail(sc)
      
    if (args(0) == "action")
      Range(1,16).foreach(dt => extractAction(sc, dt))
      
    if (args(0) == "delivery")
      Range(1,16).foreach(dt => extractDelivery(sc, dt))
      
    if (args(0) == "position")
      extractTestPosition(sc)
      
    if (args(0) == "resume") {
      extractTestResume(sc)
    }
      
    if (args(0) == "partUser") {
      extratPartUser(sc)
    }
    
    if (args(0) == "copy") {
      copyToLocal(sc)
    }
    
    if (args(0) == "TrainData") {
      testTrainData(sc)
    }
    
    if (args(0) == "idcode") {
      idcode(sc)
    }
    
    if (args(0) == "click_count_with_code") {
      click_count_with_code(sc)
    }
    
    sc.stop()
  }
}