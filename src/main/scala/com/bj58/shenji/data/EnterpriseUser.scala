package com.bj58.shenji.data

/**
 * 企业职位关联数据
 * @author jiangzhenxing
 * @date 2016-10-31
 */
class EnterpriseUser(val id: String,		      // 主键id
													 val userid: String,		// 用户id
													 val enterpriseid: String,		// 企业id
													 val dt: String		      // 分区字段，如1、2、3…
                          ) extends Serializable
{
  
}

object EnterpriseUser 
{
  
  def apply() = 
  {
    
  }
  
}