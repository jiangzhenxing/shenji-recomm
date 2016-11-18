package com.bj58.shenji.data

/**
 * 各个模型的得分
 */
case class Scores(cookieid: String,
                  infoid: String,
                  lrScore: Double,
                  dtScore: Double,
                  cfScore: Double,
                  clicks: Double,
                  label: Double) 
                  extends Serializable
{
  def features: Array[Double] = Array(lrScore, dtScore, cfScore, clicks)
}

object Scores
{
  def apply(line: String): Scores =
  {
    val values = line.split("\t")  // cookieid, infoid, lrscore, dtscore, avgClick, label
    Scores(cookieid = values(0),
           infoid = values(1),
           lrScore = values(2).toDouble,
           dtScore = values(3).toDouble,
           cfScore = 0,         // 暂时没打出来
           clicks = values(4).toDouble,
           label = values(5).toDouble)
  }
}