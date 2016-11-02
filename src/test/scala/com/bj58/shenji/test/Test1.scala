package com.bj58.shenji.test

import com.bj58.shenji.data._
import scala.collection.mutable._

object Test1 {
  def main(args: Array[String]): Unit = {
//    println(DeliveryRecord("111\001222\001333\001444\001555\001666\001111\001111\001"))
//    val s = Set(1,2,3)
//    s ++= Set(4,5,6)
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
    
    val s1 = Array(1,2,3)
    val s2 = Array(4,5,6)
    
    for {
      a ← s1
      b ← s2
    } {
      println(a,b)
    }
  }
}