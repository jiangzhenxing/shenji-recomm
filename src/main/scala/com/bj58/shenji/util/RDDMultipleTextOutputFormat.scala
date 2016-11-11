package com.bj58.shenji.util

import org.apache.spark._
import org.apache.spark.SparkContext._
 
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

/**
 * User: 过往记忆
 * Date: 15-03-11
 * Time: 上午08:24
 * bolg: https://www.iteblog.com
 * 本文地址：https://www.iteblog.com/archives/1281
 * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
 * 过往记忆博客微信公共帐号：iteblog_hadoop
 */
class MultipleTextOutputFormatWithoutKey extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
  
  override def generateActualKey(key: Any, value: Any) = NullWritable.get
}

object Split {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SplitTest")
    val sc = new SparkContext(conf)
    (sc.parallelize(List(("w", "www"), ("b", "blog"), ("c", "com"), ("w", "bt")))
      .map(value => (value._1, value._2 + "Test"))
      .repartition(2)
      .saveAsHadoopFile("iteblog", classOf[NullWritable], classOf[String],
        classOf[MultipleTextOutputFormatWithoutKey]))
    sc.stop()
  }
}