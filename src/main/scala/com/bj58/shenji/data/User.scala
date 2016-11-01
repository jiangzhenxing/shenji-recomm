package com.bj58.shenji.data

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
   * 				 		如果简历或职位是面议，则差置为1，否则相减
   * rdoidentity: 个人身份，1:在校学生 0:社会人才 | 与职位要求中的［fresh: 是否接受应届生，0：不接受 1：接受］相匹配
   * 							如果是在校生，则与fresh相减(即接受应届生为1-1＝0，不接受为1－0＝1)，否则差计为0
   * experience: 	工作年限，1:不限 4:1年以下 5:1-2年 6:3-5年 7:6-7年 8:8-10年 9:10年以上  （好像没有提供）
   * 							如果职位要求是不限，则差计为0，否则两者相减
   */
  def matches(job: Job): Double = 
  {
    // TODO
    0
  }
}

object User 
{
  def apply() = 
  {
    
  }
}