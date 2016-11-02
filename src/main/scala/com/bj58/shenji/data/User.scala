package com.bj58.shenji.data

import scala.collection.mutable.Set

/**
 * 用户信息
 * @author jiangzhenxing
 * @date 2016-11-01
 */
case class User(
    cookieid: String,
    userid: String,
    resumes: Seq[Resume] = Seq())
{
  /**
   * 用户信息和职位信息进行匹配
   * 如有多个简历，取差最小的那个
   * 匹配方法如下：
   * targetCateid: 目标职位类别ID，加上实际投递的职位类别，匹配时包含为0，不包含为1
   * targetAreaid: 目标城市，加上实际投递地域，匹配时包含为0，不包含为1
   * education: 教育程度，1:不限 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
   * salary: 		薪资，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
   * rdoidentity: 个人身份，1:在校学生 0:社会人才 | 与职位要求中的［fresh: 是否接受应届生，0：不接受 1：接受］相匹配
   * workedyears: 	工作年限，0:不限 1:1-3年 2:3-5年 3:5-10年 4:10年以上 5:应届生 6:1年以下
   * 							如果职位要求是不限，则差计为0，否则两者相减
   */
  def matches(job: Job): Double = 
  {
    for (resume ← resumes) {
      // 职位类别,目标城市,教育程度,薪资，个人身份，工作年限
      val cate, aread, education, salary, rdoidentity, experience = 0.0
      
      resume.deliveryCateid
		
    }
    0
  }
  
  /**
   * 工作年限距离
   * 简历: 0:不限 5:应届生 6:1年以下 1:1-3年 2:3-5年 3:5-10年 4:10年以上
   * 职位: 1:不限 4:1年以下 5:1-2年 6:3-5年 7:6-7年 8:8-10年 9:10年以上
   */
  def workedyearsDis(workedyears: Int, experience: Int): Double =
  {
    
    0
  }
  
  /**
   * 身份距离
   * 如果是在校生，则与fresh相减(即接受应届生为1-1＝0，不接受为1－0＝1)，否则是社会人才,差计为0
   * 简历: 1:在校学生 0:社会人才
   * 职位: 是否接受应届生，0：不接受 1：接受
   */
  def identityDis(identity: Int, fresh: Int): Double =
  {
    if (identity == 1)
      fresh - identity
    else
      0d
  }
  
  /**
   * 薪资距离
   * 如果简历或职位是面议，则差置为1，否则相减
   * 简历: 1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
   * 职位: 1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
   */
  def salaryDis(resumSalary: Int, jobSalary: Int): Double =
  {
    var dis = 0d
    if (resumSalary == 1 || jobSalary == 1)
      dis = 1d
    else
      dis = resumSalary - jobSalary
    dis / 9d
  }
  
  /**
   * 学历距离
   * 简历: 1:高中以下 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
   * 职位: 1:不限 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
   */
  def educationDis(redu: Int, jedu: Int): Double =
  {
    (redu - jedu) / 7d
  }
  
  /**
   * 职位类别距离
   * targetcate 目标职位类别+实际投递的职位类别,数据格式为1,2,3;1,2,4;1,2,5
   * jobcate1,2,3　职位中的1,2,3级职位类别
   * 如果简历中的期望职位类别+投递职位类别 包含 招聘职位的类别，则距离为0，否则为1
   * 多个目标取差距最小的
   */
  def catedDis(targetcate: String, jobcate1: String, jobcate2: String, jobcate3: String): Double =
  {
    var mindis = 3D;
    for (tcate ← targetcate.split(";")) {
      val tcates = tcate.split(",")
      val jcates = Array(jobcate1,jobcate2,jobcate3)
      val dis = arrdis(tcates, jcates)
      mindis = math.min(mindis, dis)
    }
    mindis / 3d
  }
  
  /**
   * 地域距离
   * targetArea 目标地域，加上实际投递地域, 数据格式为1,2,3;1,2,4;1,2,5，'-'表示所有
   * jobarea 职位所在地域，多个，数据格式为1,2,3;1,2,4;1,-,-，'-'表示所有
   */
  def areaDis(targetArea: String, jobarea: String): Double =
  {
    var mindis = 3d;
    for {
          tcate ← targetArea.split(";")
          jcate ← jobarea.split(";")
        }
    {
      val tcates = tcate.split(",")
      val jcates = jcate.split(";")
      val dis = arrdis(tcates, jcates)
      mindis = math.min(mindis, dis)
    }
    mindis / 3d
  }
  
  /**
   * 计算两个数组之间的距离，全部为'-'，数组大小为3
   */
  def arrdis(a1: Array[String], a2: Array[String]): Double =
  {
    var dis = 0D
    if (a1(0) == "-") {
      return 0
    }
    if (a1(0) != a2(0)) {
      return if (a2(0) == "-") 2.5 else 3
    }
    if (a1(1) == "-") {
      return 0
    }
    if (a1(1) != a2(1)) {
      return if (a2(1) == "-") 1.5 else 2
    } 
    if (a1(2) == "-") {
      return 0 
    }
    if (a1(2) != a2(2)) {
      return if (a2(2) == "-") 0.5 else 1
    }
    0
  }
}

object User 
{
  def apply() = 
  {
    
  }
  
  def main(args: Array[String]): Unit = {
    
  }
}