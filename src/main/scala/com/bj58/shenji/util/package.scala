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

}