package com.bj58.shenji


import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import com.bj58.shenji.data._
import java.util.concurrent.Executors
import java.util.concurrent.Callable


/**
 * 根据用户行为来进行推荐
 * 主要方法有：逻辑回归，决策树，SVM, 协同过滤（矩阵分解）
 * @author jiangzhenxing
 * @date 2016-10-31
 */
package object wanted 
{
  def trainLR = LRModel.train _
  def trainLRPart = LRModel.trainPart _
  def trainDT = DTModel.train _
  def trainDTPart = DTModel.trainPart _
  def trainSVM = SVMModel.train _
  def trainSVMPart =SVMModel.trainPart _
  def trainCF = CFModel.train _
  def trainComprehensive = Comprehensive.train _
}