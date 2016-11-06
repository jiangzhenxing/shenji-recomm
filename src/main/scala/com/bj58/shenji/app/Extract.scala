package com.bj58.shenji.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.bj58.shenji.data._
import org.apache.spark.broadcast.Broadcast

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
    sc.stop()
  }
}