package com.bj58.shenji.wanted

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.bj58.shenji.data.Position
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

/**
 * 逻辑回归模型
 */
object LRModel extends Serializable
{
  /**
   * (HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)
   */
  
  
  
  def train(sc: SparkContext) = 
  {
    val sep = "\t"
    // (0,19 155 442) (1,637829)
    val list_position = sc.textFile("/home/team016/middata/test_list_position/*")
    // 87895
    val action_position = sc.textFile("/home/team016/middata/test_action_position/*")
    
    val testdata = sc.textFile("/home/hdp_hrg_game/shenjigame/data/stage1/testdata/")
    val testCookies = testdata.map(_.split("\001")(0)).distinct.collect
    
    // (cookieid,0),(userid,1),(infoid,2),(clicktag,3),(clicktime,4),(userid,5),(scate1,6),(scate2,7),(scate3,8),(title,9),(local,10),
    // (salary,11),(education,12),(experience,13),(trade,14),(enttype,15),(fuli,16),(fresh,17),(additional,18)
    val unionRecords = list_position.union(action_position)
    
    testCookies.map(cookieid => (cookieid, unionRecords.filter(record => record.substring(0, record.indexOf("\t")) == cookieid)))
               .map {
                      case (cookieid, records) => 
                        val datas = records.map(_.split("\t")).sortBy(_(4).toLong).map(labeledPoint)
                        LogisticRegressionWithSGD.train(datas, 10, .05)
                                                 .save(sc, "/home/team016/middata/model/lr/" + cookieid)
                    }
    
//    list_position.union(action_position)
//                 .groupBy(record => record.substring(0, record.indexOf("\t")))
//                 .map { case (cookieid, records) => 
//                          val sc2 = new SparkContext(bconfig.value)
//                          val datas = sc2.parallelize(records.toSeq).map(_.split("\t").sortBy(_(4).toLong)).map(labeledPoint)
//                          datas.cache()
//                          LogisticRegressionWithSGD.train(datas, 20, .05)
//                                                   .save(sc2, "/home/team016/middata/model/lr/" + cookieid)
//                          sc2.stop() }
//                 .count
  }
  
  def labeledPoint(values: Array[String]) =
  {
    val p = Position(infoid = values(2),
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
  
  def main(args: Array[String]): Unit = 
  {
    
    println("cookieid,userid,infoid,clicktag,clicktime,userid,scate1,scate2,scate3,title,local,salary,education,experience,trade,enttype,fuli,fresh,additional".split(",").zipWithIndex.mkString(","))
  }
}