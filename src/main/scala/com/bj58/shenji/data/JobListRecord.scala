package com.bj58.shenji.data

/**
 * 职位展示数据
 * @author jiangzhenxing
 * @date 2016-10-31
 */
class JobListRecord(val cookieid: String,   // 用户CookieID
                   val userid:   String,   // 用户ID，未登录状态记录为‘-’
                   val ip:       String,   // 用户IP
                   val sid:      String,   // 检索ID，唯一地标识一次检索
                   val stime:    String,   // 访问时间戳
                   val xforward: String,   // http_x_forward_for
                   val ua:       String,   // http_user_agent
                   val sloc1:    String,   // 一级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                   val sloc2:    String,   // 二级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                   val sloc3:    String,   // 三级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                   val sloc4:    String,   // 四级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                   val scate1:   String,   // 一级检索类别，可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                   val scate2:   String,   // 二级检索类别，可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                   val scate3:   String,   // 三级检索类别，可与可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                   val scate4:   String,   // 四级检索类别，可与可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                   val url:      String,   // 当前访问页面URL
                   val keyword:  String,   // 搜索关键字
                   val params:   String,   // 单元参数
                   val slot:     String,   // 推荐位标示，大列表页数据记为‘-’
                   val infoid:   String,   // 职位ID
                   val infouserid: String, // 发布职位用户ID
                   val pageno:   String,   // 职位所在页码
                   val position: String,   // 职位所在位置
                   val clicktag: String,   // 点击标示，点击为1，未点击为0
                   val clicktime: String  // 点击行为时间戳
                   ) extends Serializable

object JobListRecord
{
  val field_delim = "\001"
  def apply(line: String): JobListRecord =
  {
    val values = line.split(field_delim)
    new JobListRecord(cookieid = values(0),   // 用户CookieID
                			userid = values(1),     // 用户ID，未登录状态记录为‘-’
                			ip = values(2),         // 用户IP
                			sid = values(3),        // 检索ID，唯一地标识一次检索
                			stime = values(4),      // 访问时间戳
                			xforward = values(5),   // http_x_forward_for
                			ua = values(6),         // http_user_agent
                			sloc1 = values(7),      // 一级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                			sloc2 = values(8),      // 二级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                			sloc3 = values(9),      // 三级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                			sloc4 = values(10),     // 四级检索地域，可与hdp_58_common_defaultdb.ds_dict_cmc_displocal关联
                			scate1 = values(11),    // 一级检索类别，可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                			scate2 = values(12),    // 二级检索类别，可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                			scate3 = values(13),    // 三级检索类别，可与可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                			scate4 = values(14),    // 四级检索类别，可与可与hdp_58_common_defaultdb.ds_dict_cmc_dispcategory关联
                			url = values(15),       // 当前访问页面URL
                			keyword = values(16),   // 搜索关键字
                			params = values(17),    // 单元参数
                			slot = values(18),      // 推荐位标示，大列表页数据记为‘-’
                			infoid = values(19),    // 职位ID
                			infouserid = values(20), // 发布职位用户ID
                			pageno = values(21),     // 职位所在页码
                			position = values(22),   // 职位所在位置
                			clicktag = values(23),   // 点击标示，点击为1，未点击为0
                			clicktime = values(24)  // 点击行为时间戳
                )
  }
  
  def main(args: Array[String]): Unit = 
  {
    val Array(id: String, name: String) = "111,Tom".split(",")
    println(id, name)
  }
}