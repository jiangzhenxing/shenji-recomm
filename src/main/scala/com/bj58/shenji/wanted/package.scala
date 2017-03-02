package com.bj58.shenji

/**
 * 根据用户行为来进行推荐
 * 主要方法有：逻辑回归，决策树，SVM, 协同过滤（矩阵分解）
 * @author jiangzhenxing
 * @date 2016-10-31
 */
package object wanted 
{
  def trainLR = LRModel.train _
  def trainDT = DTModel.train _
  def trainSVM = SVMModel.train _
  def trainCF = CFModel.train _
  def trainComprehensive = Comprehensive.train _
}