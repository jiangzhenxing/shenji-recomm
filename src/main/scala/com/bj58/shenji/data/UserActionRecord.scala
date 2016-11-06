package com.bj58.shenji.data

/**
 * 用户行为记录
 * @author jiangzhenxing
 * @date 2016-10-31
 */
case class UserActionRecord(cookieid: String,   // 用户CookieID
											      clicktag: String,		// 点击事件标示：查看电话seetel、在线交谈message、立即申请apply
											      clicktime: String,	// 点击事件时间戳
											      userid: String,		  // 用户ID，未登录状态记录为‘-’
											      infoid: String		  // 职位ID
                           ) 
extends Serializable

object UserActionRecord 
{
  def apply(line: String): UserActionRecord =
  {
    val values = line.split("\001")
    UserActionRecord( cookieid = values(0),
                      clicktag = values(1),
                      clicktime = values(2),
                      userid = values(3),
                      infoid = values(4) 
                    )
  }
}