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
import org.dmg.pmml.False

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
   * 提取测试集中的用户投递职位信息
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
   * 抽取训练数据
   */
  def extractTrainData(sc: SparkContext) = 
  {
    val testCookies = testCookieSet(sc)
    val bcookies = sc.broadcast(testCookies)
    val sep = "\001"
    
    Range(1,16).foreach { dt => 
      println("Extract " + dt)
      val position = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=" + dt)
                       .map(line => (Position(line).userid, line))
                       
      val enterprise_user = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/enterprise_re_user/dt=" + dt)
                              .map(EnterpriseUser(_))
                              .map(user => (user.userid, user.enterpriseid)) // userid, enterpriseid
                              
      val enterprise = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/enterprise/dt=" + dt)
                         .map(line => (Enterprise(line).id, line))
                         
      val positionEnterprise = position.leftOuterJoin(enterprise_user)
                                      .map { case (userid, (position, enterpriseid)) => (if (enterpriseid == None) "-" else enterpriseid.get, position) }
                                      .leftOuterJoin(enterprise)
                                      .map { case (enterpriseid, (position, enterprise)) => (Position(position).infoid, (position, if (enterprise == None) "" else enterprise.get)) }
              
      val detail = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/detail/dt=" + dt)
                     .map(JobListRecord(_))
                     .filter(record => bcookies.value.contains(record.cookieid))
                     .map(r => (r.infoid,Array(r.cookieid,r.userid,r.infoid,r.clicktag,if (r.clicktag == "1") r.clicktime else r.stime).mkString(sep)))
      
      val action = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/useraction/dt=" + dt)
                     .map(UserActionRecord(_))
                     .filter(r => bcookies.value.contains(r.cookieid))
                     .map(r => (r.infoid, Array(r.cookieid, r.userid, r.infoid, r.clicktag, r.clicktime).mkString(sep)))
                     
      val delivery = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/delivery/dt=" + dt)
                     .map(DeliveryRecord(_))
                     .filter(r => bcookies.value.contains(r.cookieid))
                     .map(r => (r.infoid, Array(r.cookieid, r.resumeuserid, r.infoid, "apply", r.deliverytime).mkString(sep)))
                     
      detail.union(action)
            .union(delivery)
            .join(positionEnterprise)
            .map { case (infoid, (detail, (position, enterprise))) => detail + sep + position + sep + enterprise } // 5 + 23 + 21
            .repartition(10)
            .saveAsTextFile("/home/team016/middata/stage2/traindata/dt=" + dt)
    }
  }
  
  /**
   * 提取用户点击的工作类别
   */
  def extractUserJobCates(sc: SparkContext) =
  {
    import com.bj58.shenji.data.Position
    val sep = "\001"
    val traindata = sc.textFile("/home/team016/middata/stage2/traindata/*")
    traindata.map(_.split(sep)) // detail:0-5; position:5-29; enterprise:29-51
             .filter(_(3) != "0")
             .map { values => val p = Position(values.slice(5,29))
                              (values(0), Seq(p.scate1,p.scate2,p.scate3).mkString(",")) }
             .distinct
             .groupBy(_._1)
             .map { case (cookieid, iter) => cookieid + sep + iter.map(_._2).mkString(";") }
             .repartition(1)
             .saveAsTextFile("/home/team016/middata/stage2/user_job_cates/")
  }
  
  /**
   * 提取用户点击工作的地域
   */
  def extractUserLocals(sc: SparkContext) =
  {
    import com.bj58.shenji.data.Position
    val sep = "\001"
//    val locals = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/ds_dict_cmc_local")
//                   .map(_.split(sep))
//                   .map(values => (values(0), values(3).split("\002").slice(0, 2).mkString(",")))
//                   .collectAsMap
//    val blocals = sc.broadcast(locals)
    
    val traindata = sc.textFile("/home/team016/middata/stage2/traindata/*")
    traindata.map(_.split(sep)) // detail:0-5; position:5-29; enterprise:29-51
             .filter(_(3) != "0")
             .flatMap { values => val p = Position(values.slice(5,29))
                              p.local.split(",").map(local => (values(0), local)) }
             .distinct
             .groupBy(_._1)
             .map { case (cookieid, iter) => cookieid + sep + iter.map(_._2).mkString(";") }
             .repartition(1)
             .saveAsTextFile("/home/team016/middata/stage2/user_locals/")
  }
  
  /**
   * 提取用户点击的二级地域
   */
  def extractUserLocals2(sc: SparkContext) =
  {
    import com.bj58.shenji.data.Position
    val sep = "\001"
    val locals = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/ds_dict_cmc_local")
                   .map(_.split(sep))
                   .map(values => (values(0), values(3).split("\002").slice(0, 2).mkString(",")))
                   .collectAsMap
    val blocals = sc.broadcast(locals)
    
    val traindata = sc.textFile("/home/team016/middata/traindata/*")
    traindata.map(_.split(sep)) // detail:0-5; position:5-29; enterprise:29-51
             .filter(_(3) != "0")
             .flatMap { values => val p = Position(values.slice(5,29))
                              p.local.split(",").map(local => (values(0), blocals.value.getOrElse(local,"-"))) }
             .distinct
             .groupBy(_._1)
             .map { case (cookieid, iter) => cookieid + sep + iter.map(_._2).mkString(";") }
             .repartition(1)
             .saveAsTextFile("/home/team016/middata/user_locals2/")
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
            .map { case (infoid, (cookieid,position)) => (cookieid, infoid, if (position == None) None else Some(Position(position.get))) }
            .map { case (cookieid, infoid, option) =>  if (option == None) cookieid + sep + infoid + sep + "--" else { val p = option.get; Array(cookieid,p.infoid,p.userid,p.scate1,p.scate2,p.scate3,p.title,p.local,p.salary,p.education,p.experience,p.trade,p.enttype,p.fuli,p.fresh,p.additional).mkString(sep) } }
            .repartition(1)
            .saveAsTextFile("/home/team016/middata/test_user_position/") // 949258  961957 12699
  }
  
  /**
   * 抽取测试数据
   */
  def extractTestData(sc: SparkContext) =
  {
    val sep = "\001"
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage2/testdata/")
                     .map(_.split("\001"))
                     .map(values => (values(1).trim, values(0))) // infoid, cookieid
                     
    val positions = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/position/dt=16")
                      .map(line => (Position(line).userid, line))  // userid, position
                      
    val enterprise_user = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/enterprise_re_user/dt=16")
                            .map(EnterpriseUser(_))
                            .map(user => (user.userid, user.enterpriseid)) // userid, enterpriseid
                            
    val enterprise = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/enterprise/dt=16")
                       .map(line => (Enterprise(line).id, line))
                       
    val positionEnterprise = positions.leftOuterJoin(enterprise_user)
                                      .map { case (userid, (position, enterpriseid)) => (if (enterpriseid == None) "-" else enterpriseid.get, position) }
                                      .leftOuterJoin(enterprise)
                                      .map { case (enterpriseid, (position, enterprise)) => (Position(position).infoid, (position, if (enterprise == None) "" else enterprise.get)) }
                                      // infoid,(position,enterprise)
              
    testdata.leftOuterJoin(positionEnterprise)
            .map { case (infoid, (cookieid,pe)) => cookieid + sep + infoid + sep +  (if (pe == None) "--" else pe.get._1 + sep + pe.get._2) }
            .repartition(32)
            .saveAsTextFile("/home/team016/middata/stage2/testdata/") // 949258  961957 12699
  }
  
  /**
   * 抽取测试数据中的简历信息
   */
  def extractTestResume(sc: SparkContext) =
  {
    val sep = "\t"
    // 找到用户id
    val listUser = (sc.textFile("/home/team016/middata/stage2/traindatabyuser/")
                      .repartition(100)
                     .map(_.split("\001"))
                     .map(values => (values(1),values(0))) // userid, cookieid
                     .distinct()
                     ) // /home/team016/middata/cookie_user
//    listUser.map { case (userid, cookieid) => cookieid + "\t" + userid }
//            .saveAsTextFile("/home/team016/middata/test_cookie_user")
            
    val resume = (sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/traindata/resume/*")
                    .repartition(100)
                      .map(p => (p.split("\001", 3)(1), p))) // userid, resume
    listUser.join(resume)
            .mapValues { case (cookieid, resume) => cookieid + sep + resume }
            .map(_._2)
            .saveAsTextFile("/home/team016/middata/stage2/test_user_resume")
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
  
  /**
   * 抽取训练数据
   */
  def testTrainData(sc: SparkContext) =
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    val delivery_position = sc.textFile("/home/team016/middata/test_delivery_position/*")
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    list_position.union(action_position)
                 .union(delivery_position)
                 .sortBy(record => { val values = record.split(sep); values(0) + sep + values(4) }, true, 5)
                 .saveAsTextFile("/home/team016/middata/test_train_data_all/")
  }
  
  def splitUser(sc: SparkContext)
  {
    val testCookie = sc.textFile("/home/team016/middata/stage2/test_cookies").collect // 23497
    val size = 2500
    Range(0, testCookie.size, size).map(begin => testCookie.slice(begin, begin + size))
                                   .zipWithIndex
                                   .foreach { case (values, index) => sc.parallelize(values.toSeq).saveAsTextFile("/home/team016/middata/stage2/test_cookies_split10/part" + index) } // 23497
  }
  
  def splitTrainData(sc: SparkContext, part: Int) =
  {
//    val userDataCount = sc.textFile("/home/team016/middata/stage2/userDataCount/")
//                          .map(_.split("\t"))
//                          .map { case Array(cookieid, count) => (cookieid, count.toInt) }
//                          .collectAsMap
                          
    val cookies = sc.textFile("/home/team016/middata/stage2/test_cookies_split10/part" + part).collect()
    
    cookies.foreach { cookieid => 
      val userdata = sc.textFile("/home/team016/middata/stage2/traindatabyuser/" + cookieid + "-*")
//      val datacount = userDataCount(cookieid)
//      val train80 = (datacount * 0.8).intValue()
      val Array(train80, train20) = userdata.randomSplit(Array(0.8, 0.2), 97)
      train80.saveAsTextFile("/home/team016/middata/stage2/traindatabyuser_split/train80/" + cookieid)
      train80.saveAsTextFile("/home/team016/middata/stage2/traindatabyuser_split/train20/" + cookieid)
    }
  }
  
  def userDataCount(sc: SparkContext) = 
  {
    sc.textFile("/home/team016/middata/stage2/traindatabyuser/")
      .map(line => (line.split("\001")(0), 1))
      .reduceByKey(_ + _)
      .map(kv => kv._1 + "\t" + kv._2)
      .saveAsTextFile("/home/team016/middata/stage2/userDataCount")
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
   * 为训练集中的数据的id转换成编码
   */
  def traindata_with_code(sc: SparkContext)
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
  
  def extractAllScore(sc: SparkContext) =
  {
    val testCookies = sc.textFile("/home/team016/middata/test_cookies").collect
    testCookies.foreach { cookieid =>
        val lrdtScore = sc.textFile("/home/team016/middata/result10/lrdt/" + cookieid)    // cookieid, infoid, lrscore, dtscore, label
                          .map(_.split("\t"))
                          .map{ case Array(cookieid, infoid, lrscore, dtscore, label) => (cookieid + "\t" + infoid, (lrscore, dtscore, label)) }
                          .cache
//        val cfScore = sc.textFile("/home/team016/middata/result10/cf/" + cookieid)        // cookieid, infoid, rating, label
//                        .map(_.split("\t"))
//                        .map { case Array(cookieid, infoid, rating, label) => (cookieid + "\t" + infoid, rating) }
        
        val clickScore = sc.textFile("/home/team016/middata/result10/click/" + cookieid)  // infoid, avgClick, label
                           .map(_.split("\t"))
                           .map { case Array(infoid, avgClick, label) => (cookieid + "\t" + infoid, avgClick) }
                           .cache
        lrdtScore.join(clickScore)
                 .map { case (cookie_info, ((lrscore, dtscore, label), avgClick)) => Array(cookie_info, lrscore, dtscore, avgClick, label).mkString("\t") }
                 .saveAsTextFile("/home/team016/middata/result10/all/" + cookieid)
                 
        lrdtScore.unpersist(false)
        clickScore.unpersist(false)
    }
  }
  
  def extracTrainScoreByUser(sc: SparkContext) =
  {
    // cookieid, infoid, lrscore, dtscore, svmscore, label
    val lrdtsvmscore = sc.textFile("/home/team016/middata/stage2/train_result/lrdtsvmbyuser/all/*")
                                .map(_.split("\t")) 
                                .map { case Array(cookieid, infoid, lrscore, dtscore, svmscore, label) => 
                                  (cookieid + "\t" + infoid, (lrscore, dtscore, svmscore, label))
                                }
                                .repartition(100)
    val cfScores = sc.textFile("/home/team016/middata/stage2/train_result/cf/")  // 32 203 297 * 100
                     .map(_.split("\t"))
                     .map { case Array(cookieid, infoid, score) => (cookieid + "\t" + infoid, score) }
                     .distinct()
                     .repartition(100)
    // cookieid, infoid, clickscore, label(action)
    val clickScores = sc.textFile("/home/team016/middata/stage2/train_result/click/")
                     .map(_.split("\t"))
                     .map { case Array(cookieid, infoid, score, label) => (cookieid + "\t" + infoid, score) }
                     .distinct
                     .repartition(100)
                     
    lrdtsvmscore.leftOuterJoin(cfScores)
                .map { case (cookie_info, ((lrscore, dtscore, svmscore, label), cfscore)) => 
                        (cookie_info, (lrscore, dtscore, svmscore, if (cfscore == None) "0" else cfscore.get, label)) }
                .leftOuterJoin(clickScores)
                .map { case (cookie_info, ((lrscore, dtscore, svmscore, cfscore, label), clickscore)) => 
                        Array(cookie_info, lrscore, dtscore, svmscore, cfscore, if (clickscore == None) "0" else clickscore.get, label).mkString("\t") }
                .saveAsTextFile("/home/team016/middata/stage2/train_result/allscore2/")
  }
  
  def extractTestScore(sc: SparkContext) =
  {
    // cookieid, infoid, score
    val lrScores = sc.textFile("/home/team016/middata/stage2/result/lr") // 2 034 327
                                .map(_.split("\t"))
                                .map { case Array(cookieid, infoid, score) => 
                                  (cookieid + "\t" + infoid, score)
                                }
    
    val dtScores = sc.textFile("/home/team016/middata/stage2/result/dtpart/*") // 2034327
                                .map(_.split("\t")) 
                                .map { case Array(cookieid, infoid, score) => 
                                  (cookieid + "\t" + infoid, score)
                                }
    
    val svmScores = sc.textFile("/home/team016/middata/stage2/result/svm") // 2 034 327
                                .map(_.split("\t")) 
                                .map { case Array(cookieid, infoid, score) => 
                                  (cookieid + "\t" + infoid, score)
                                }
    
    val cfScores = sc.textFile("/home/team016/middata/stage2/result/cf/")  // 1 828 996
                     .map(_.split("\t"))
                     .map { case Array(cookieid, infoid, score) => (cookieid + "\t" + infoid, score) }
    
    // cookieid, infoid, clickscore, label(action)
    val clickScores = sc.textFile("/home/team016/middata/stage2/result/click_evaluate/") // 2034327
                     .map(_.split("\t"))
                     .map { case Array(cookieid, infoid, score) => (cookieid + "\t" + infoid, score) }
    // lrscore, dtscore, svmscore, cfscore, clickscore
    lrScores.leftOuterJoin(dtScores)
                .map { case (cookie_info, (lrscore, dtscore)) => 
                        (cookie_info, lrscore + "\t" + (if (dtscore == None) "0" else dtscore.get)) }
                .leftOuterJoin(svmScores)
                .map { case (cookie_info, (lrdtscore, svmscore)) => 
                        (cookie_info, (lrdtscore + "\t" + (if (svmscore == None) "0" else svmscore.get))) }
                .leftOuterJoin(cfScores)
                .map { case (cookie_info, (lrdtsvmscore, cfscore)) => 
                        (cookie_info, (lrdtsvmscore + "\t" + (if (cfscore == None) "0" else cfscore.get))) }
                .leftOuterJoin(clickScores)
                .map { case (cookie_info, (lrdtsvmcfscore, clickscore)) => 
                        cookie_info + "\t" + lrdtsvmcfscore + "\t" + (if (clickscore == None) "0" else clickscore.get) }
                .repartition(10)
                .saveAsTextFile("/home/team016/middata/stage2/result/allscore/")  // 2034327
  }
  
  def splitTestData(sc: SparkContext) =
  {
    sc.textFile("/home/team016/middata/stage2/testdata/")
      .map(line => (line.split("\001", 3)(0), line))
      .partitionBy(new HashPartitioner(8))
      .map(_._2)
      .saveAsTextFile("/home/team016/middata/stage2/testdata8/")
  }
  
  /**
   * 测试数据集中的cookieid
   */
  def testCookieSet(sc: SparkContext) = 
  {
    sc.textFile("/home/team016/middata/stage2/test_cookies").collect.toSet // 23497
  }
  
  
  def extractTestCookie(sc: SparkContext) =
  {
    sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage2/testdata/")
      .map(_.split("\001")(0))
      .distinct()
      .saveAsTextFile("/home/team016/middata/stage2/test_cookies/")
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
    
    if (args(0) == "extractUserLocals") {
      extractUserLocals(sc)
    }
    
    if (args(0) == "extractUserJobCates") {
      extractUserJobCates(sc)
    }
    
    if (args(0) == "extractTrainData") {
      println("************** extractTrainData begin ********************")
      extractTrainData(sc)
    }
    
    if (args(0) == "extractTestData") {
      println("************** extractTestData begin ********************")
      extractTestData(sc)
    }
    
    if (args(0) == "extractAllScore") {
      extractAllScore(sc)
    }
    
    if (args(0) == "splitTrainData") {
      splitTrainData(sc, args(1).toInt)
    }
    
    if (args(0) == "extracTrainScoreByUser") {
      extracTrainScoreByUser(sc)
    }
    
    if (args(0) == "extractTestScore") {
      extractTestScore(sc)
    }
    
    if (args(0) == "splitTestData") {
      splitTestData(sc)
    }
    
    if (args(0) == "splitUser") {
      splitUser(sc)
    }
    
    
    sc.stop()
  }
}