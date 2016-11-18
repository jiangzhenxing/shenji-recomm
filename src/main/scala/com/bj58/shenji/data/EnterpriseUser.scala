package com.bj58.shenji.data

/**
 * 企业职位关联数据
 * @author jiangzhenxing
 * @date 2016-10-31
 */
case class EnterpriseUser( val id: String,		      // 主键id
													 val userid: String,		// 用户id
													 val enterpriseid: String		// 企业id
                          ) extends Serializable

object EnterpriseUser 
{
  
  def apply(line: String): EnterpriseUser = 
  {
    apply(line.split("\t"))
  }
  
  def apply(values: Array[String]): EnterpriseUser =
  {
    EnterpriseUser(id = values(0),
                   userid = values(1),
                   enterpriseid = values(2)
                   )
  }
  
}