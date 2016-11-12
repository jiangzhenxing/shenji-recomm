package com.bj58.shenji.test

import com.bj58.shenji.data._
import scala.collection.mutable._

object Test1 {
  def main(args: Array[String]): Unit = {
//    println(DeliveryRecord("111\001222\001333\001444\001555\001666\001111\001111\001"))
//    val s = Set(1,2,3)
//    s ++= Array(4,5,3,2,2,9)
//    println(s)
//    
//    println("Hello World!".substring(3).!=("Hello World!".substring(3)));
//    
//    println("123" != "123")
//    println("123" == "123")
//    
//    var dis = 1.0
//    println(dis + (if ("111" == "-") 1.5 else 2))
//    println(dis + (if ("-" == "-") 1.5 else 2))
//    println("Hello!")
//    val s1 = Array(1,2,3)
//    val s2 = Array(4,5,6)
//    
//    (s1 ++: s2 ++: s2).foreach(println)
//    
//    println("aa|bb|cc".split("\\|").mkString(","))
//    
//    for {
//      a ← s1
//      b ← s2
//    } {
//      println(a,b)
//    }
    
//    "aaa,bbb,ccc,ddd".split(",", 2).foreach { println }
    
//    Range(1,16).foreach(dt => println(dt))
//    
//    val a = Array(1,2,3,4)
//    a(0) = 0
//    println(a.mkString(","))
//    
//    Range(10,30,1).foreach(x => println(x/10d))
    
    println(math.exp(1))
    
    val m = Map("0" -> 10000, "1" -> 3000)
    println(m.getOrElse("see", 0).doubleValue())
    
    println(math.ceil(1.123456))
    
    val s = """244:互联网/电子商务 245:计算机软件 246:计算机硬件 247:IT服务/系统集成 248:通信/电信 
                                             249:电子技术/半导体/集成电路 250:仪器仪表/工业自动化 251:财务/审计 252:金融/银行 
                                             253:保险 254:贸易/进出口 255:批发/零售 256:快速消费品(食品/饮料等) 257:耐用消费品（家具/家电等） 
                                             258:服装/纺织/皮革 259:办公用品及设备 260:钢铁/机械/设备/重工 261:汽车/摩托车 262:医疗/保健/卫生/美容 
                                             263:生物/制药/医疗器械 264:广告/创意 265:公关/市场推广/会展 266:文体/影视/艺术 267:媒体传播 
                                             268:出版/印刷/造纸 269:房地产/物业管理 270:建筑/建材 271:家居/室内设计/装潢 272:中介/专业服务 
                                             273:检测/认证 274:法律/法务 275:教育/科研/培训 276:旅游/酒店 277:娱乐休闲/餐饮/服务 
                                             278:交通/运输/物流 279:航天/航空 280:化工/采掘/冶炼 281:能源（电力/水利/矿产） 
                                             282:原材料和加工 283:政府/非盈利机构 284:环保 285:农林牧渔 286:多元化集团 3527:人力资源服务 
                                             287:其他行业 288:陶瓷卫浴 289:家具灯饰 290:纺织服饰  291:游戏 294:咨询/顾问 295:信托/拍卖 296:租赁服务"""
    println(s.split("\\s+").map(_.split(":")(0)).map("\"" + _ + "\"").size)
    
    
    
  }
}