package com.bj58.shenji

package object util 
{
  /**
   * 计算向量的模长
   */
  def vecmodel(v: Array[Double]) =
  {
    math.sqrt(v.map(d ⇒ d * d).sum)
  }

  /**
   * 向量的点积
   */
  def vecdot(v1: Array[Double], v2: Array[Double]): Double = 
    v1.zip(v2).map { case (d1,d2) => d1 * d2 }.sum
  
  /**
   * logistic函数值
   */
  def logistic(x: Double): Double = 1 / (1 + math.exp(-x))
}