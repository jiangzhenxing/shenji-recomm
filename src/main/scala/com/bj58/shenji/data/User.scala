package com.bj58.shenji.data

import scala.collection.mutable.Set
import com.bj58.shenji.util._

/**
 * 用户信息
 * @author jiangzhenxing
 * @date 2016-11-01
 */
case class User(
    cookieid: String,
    userid: String,
    resumes: Seq[Resume] = Seq(),
    cmcCates: Map[String,String],  // cmc_cate的映射，key为cateid, value为fullcatepaths，以','分隔
    cmcLocals: Map[String,String]  // cmc_local的映射，key为areaid, value为fullpath，以','分隔
    )
{
  /**
   * 用户信息和职位信息进行匹配
   * 如有多个简历，取差最小的那个
   * 匹配方法如下：
   * targetCate: 目标职位类别，加上实际投递的职位类别
   * targetArea: 目标城市，加上实际投递地域
   * education: 教育程度，1:不限 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
   * salary: 		薪资，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
   * rdoidentity: 个人身份，1:在校学生 0:社会人才 | 与职位要求中的［fresh: 是否接受应届生，0：不接受 1：接受］相匹配
   * workedyears: 	工作年限，0:不限 1:1-3年 2:3-5年 3:5-10年 4:10年以上 5:应届生 6:1年以下
   * 							如果职位要求是不限，则差计为0，否则两者相减
   */
  def matches(position: Position): Double = 
  {
    var mindis = 10000000d
    
    for (resume ← resumes) {
      // 职位类别距离,地域距离,教育程度,薪资，个人身份，工作年限
      val disarr = Array(catedDis(targetCates(resume), position.scate1, position.scate2, position.scate3),
                         areaDis(targetAreas(resume), positionAreas(position)),
                         educationDis(resume.education, position.education),
                         salaryDis(resume.salary, position.salary),
                         identityDis(resume.rdoidentity, position.fresh),
                         workedyearDis(resume.workedyears, position.experience))
		  val dis = vecmodel(disarr)
		  mindis = math.min(dis, mindis)
    }
    
    mindis
  }
  
  /**
   * 职位中的地域，可能有多个，以','分隔
   */
  def positionAreas(position: Position) =
  {
    position.local.split(",").filter(_ != "-").map(local => cmcLocals(local)).mkString(";")
  }
  
  /**
   * 简历中的目标职位类别，加上实际投递的职位类别
   */
  def targetCates(resume: Resume): String =
  {
    val cates = Set[String]()
    cates ++= resume.targetCateid.split(",")
    if (resume.deliveryCateid != "-")
      cates ++= resume.deliveryCateid.split(",")
    
    val allCates = Set[String]()
    cates.foreach(cate => allCates add cmcCates(cate))
    
    allCates.mkString(";")
  }
  
  /**
   * 简历中的目标职位类别，加上实际投递的职位类别
   */
  def targetAreas(resume: Resume): String =
  {
    val areas = Set[String]()
    areas ++= resume.targetAreaid.split(",")
    if (resume.deliveryAreaid != "-")
      areas ++= resume.deliveryAreaid.split(",")
    
    val allAreas = Set[String]()
    areas.foreach(area => allAreas add cmcLocals(area))
    
    allAreas.mkString(";")
  }
  
  /**
   * 工作年限距离
   * 简历: 0:不限 5:应届生 6:1年以下 1:1-3年 2:3-5年 3:5-10年 4:10年以上
   * 职位: 1:不限 4:1年以下 5:1-2年 6:3-5年 7:6-7年 8:8-10年 9:10年以上
   */
  def workedyearDis(workedyears: Int, experience: Int): Double =
  {
    val workyear = workedyears match {
                      case 0 => -1
                      case 5 => 0
                      case 6 => 0.5
                      case 1 => 1
                      case 2 => 3
                      case 3 => 7
                      case 4 => 10
                      case _ => 5
                    }
    val expectyear = experience match {
                      case 1 => -1
                      case 4 => 0.5
                      case 5 => 1
                      case 6 => 3
                      case 7 => 6
                      case 8 => 8
                      case 9 => 10
                      case _ => 5
                    }
    if (expectyear == -1) 0 else (expectyear - workyear) / 10
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
      val dis = treedis(tcates, jcates)
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
      val dis = treedis(tcates, jcates)
      mindis = math.min(mindis, dis)
    }
    mindis / 3d
  }
  
  /**
   * 计算两个类别之间的距离，前面的为大类，后面的为子类，全部为'-'或没有相应的值，a,b的长度至少为1
   * 大类不同距离比子类不同距离远
   */
  def treedis(a: Array[String], b: Array[String]): Double =
  {
    val a1 = if (a.length > 0) a(0) else "-"
    val b1 = if (b.length > 0) b(0) else "-"
    
    if (a1 == "-") 
      return 0
    
    if (a1 != b1) 
      return if (b1 == "-") 2.5 else 3
    
    
    val a2 = if (a.length > 1) a(1) else "-"
    val b2 = if (b.length > 1) b(1) else "-"
    
    if (a2 == "-") 
      return 0
    
    if (a2 != b2) 
      return if (b2 == "-") 1.5 else 2
    
    
    val a3 = if (a.length > 2) a(2) else "-"
    val b3 = if (b.length > 2) b(2) else "-"
    
    if (a3 == "-") 
      return 0 
    
    if (a3 != b3) 
      return if (b3 == "-") 0.5 else 1
    
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