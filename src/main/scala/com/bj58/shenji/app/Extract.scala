package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Map

import com.bj58.shenji.data._

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
  def extractDetail(sc: SparkContext, bcookies: Broadcast[Set[String]], dt: Int) = 
  {
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
   * 提取点击事件关联职位信息
   */
  def extractAction(sc: SparkContext, bcookies: Broadcast[Set[String]], dt: Int) = 
  {
    val sep = "\t"
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
   * 抽取测试数据中的职位信息
   */
  def extractTestPosition(sc: SparkContext) =
  {
    val sep = "\t"
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
                     .map(_.split("\001"))
                     .map(values => (values(1), values(0)))
                     
    val positions = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=16")
                      .map(p => (p.substring(0, p.indexOf("\001")), p))
                      
    testdata.join(positions)
            .map { case (infoid, (cookieid,position)) => (cookieid, Position(position)) }
            .map { case (cookieid, p) =>  Array(cookieid,p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep) }
            .saveAsTextFile("/home/team016/middata/test_user_position/")
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
    
//    val test_user_position = sc.textFile("/home/team016/middata/test_user_position/")
//    test_user_position.filter()
  }
  
  /**
   * 从测试数据按用户分开保存
   */
  def extratActionByUser(sc: SparkContext, testCookies: Set[String]) =
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
//    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
//    val testCookies = testdata.map(_.split("\001")(0)).distinct.collect
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    val unionRecords = list_position.union(action_position)
    
    testCookies.map(cookieid => unionRecords.filter(record => record.substring(0, record.indexOf("\t")) == cookieid)
                                            .sortBy(_.split("\t",7)(4).toLong)
                                            .saveAsTextFile("/home/team016/middata/test_allaction_by_user/" + cookieid))
  }
  
  def main(args: Array[String]): Unit = 
  {
    val conf = new SparkConf().setAppName("Extract " + args(0))
    val sc = new SparkContext(conf)
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
    val testCookies = testdata.map(_.split("\001")(0)).distinct.collect.toSet
    val bcookies = sc.broadcast(testCookies)
    
    if (args(0) == "detail")
      Range(1,16).foreach(dt => extractDetail(sc, bcookies, dt))
      
    if (args(0) == "action")
      Range(1,16).foreach(dt => extractAction(sc, bcookies, dt))
      
    if (args(0) == "position")
      extractTestPosition(sc)
      
    if (args(0) == "actionByUser")
      extratActionByUser(sc, testCookies)
      
    sc.stop()
  }
}