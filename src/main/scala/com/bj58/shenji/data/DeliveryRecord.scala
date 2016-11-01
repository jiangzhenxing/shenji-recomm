package com.bj58.shenji.data

/**
 * 简历投递记录
 * @author jiangzhenxing
 * @date 2016-10-31
 */
case class DeliveryRecord( val cookieid: String,		// 用户CookieID
										       val infoid: String,		  // 职位ID
										       val entuserid: String,		// 发布职位用户ID
										       val resumeid: String,		// 简历ID
										       val resumeuserid: String,	// 简历用户ID
										       val deliverytime: String,	// 投递时间戳
										       val slot: String,		    // 推荐位标示，不是来自推荐位的记为‘-’
										       val dt: String		        // 分区字段
                          )
{
  
}

object DeliveryRecord 
{
  val field_delim = "\001"
  
  def apply(record: String): DeliveryRecord =
  {
    val values = record.split(field_delim)
    DeliveryRecord(values(0),values(1),values(2),values(3),values(4),values(5),values(6),values(7))
  }
}