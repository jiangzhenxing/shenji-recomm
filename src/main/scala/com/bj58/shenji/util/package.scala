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

  def vecdot(v1: Array[Double], v2: Array[Double]): Double = v1.zip(v2).map { case (d1,d2) => d1 * d2 }.sum
}