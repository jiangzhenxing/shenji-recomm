package com.bj58.shenji.data

/**
 * 用户行为记录
 * @author jiangzhenxing
 * @date 2016-10-31
 */
class UserActionRecord(val cookieid: String,    // 用户CookieID
											 val clicktag: String,		// 点击事件标示：查看电话seetel、在线交谈message、立即申请apply
											 val clicktime: String,		// 点击事件时间戳
											 val userid: String,		  // 用户ID，未登录状态记录为‘-’
											 val infoid: String,		  // 职位ID
											 val dt: String		        // 分区字段
                      )
{
  
}

object UserActionRecord 
{
  def apply()
  {
    
  }
}