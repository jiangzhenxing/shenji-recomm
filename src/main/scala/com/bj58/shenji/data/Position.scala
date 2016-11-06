package com.bj58.shenji.data

/**
 * 职位详情
 * @author jiangzhenxing
 * @created 2016-10-31
 */
case class Position( infoid: String,		// 职位ID
										 scate1: String,		// 一级归属类别，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联
										 scate2: String,		// 二级归属类别，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联
										 scate3: String,		// 三级归属类别，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联
//										 adddate: String,		// 发布时间戳
//										 effectivedate: String,		// 职位有效截止时间戳
//										 postdate: String,	// 最后修改时间戳
										 title: String,		  // 职位标题
										 userid: String,		// 用户ID
										 local: String,		  // 职位展现地域，可与hdp_58_common_defaultdb.ds_dict_cmc_local.localid关联,可能有多个值
										 salary: Int,		    // 最低薪资标示，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
										 education: Int,		// 学历要求，1:不限 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
										 experience: Int,	  // 工作年限，1:不限 4:1年以下 5:1-2年 6:3-5年 7:6-7年 8:8-10年 9:10年以上
										 trade: String,		  // 发布职位公司对应行业，244:互联网/电子商务 245:计算机软件 246:计算机硬件 247:IT服务/系统集成 248:通信/电信 
                                            // 249:电子技术/半导体/集成电路 250:仪器仪表/工业自动化 251:财务/审计 252:金融/银行 
                                            // 253:保险 254:贸易/进出口 255:批发/零售 256:快速消费品(食品/饮料等) 257:耐用消费品（家具/家电等） 
                                            // 258:服装/纺织/皮革 259:办公用品及设备 260:钢铁/机械/设备/重工 261:汽车/摩托车 262:医疗/保健/卫生/美容 
                                            // 263:生物/制药/医疗器械 264:广告/创意 265:公关/市场推广/会展 266:文体/影视/艺术 267:媒体传播 
                                            // 268:出版/印刷/造纸 269:房地产/物业管理 270:建筑/建材 271:家居/室内设计/装潢 272:中介/专业服务 
                                            // 273:检测/认证 274:法律/法务 275:教育/科研/培训 276:旅游/酒店 277:娱乐休闲/餐饮/服务 
                                            // 278:交通/运输/物流 279:航天/航空 280:化工/采掘/冶炼 281:能源（电力/水利/矿产） 
                                            // 282:原材料和加工 283:政府/非盈利机构 284:环保 285:农林牧渔 286:多元化集团 3527:人力资源服务 
                                            // 287:其他行业 288:陶瓷卫浴 289:家具灯饰 290:纺织服饰  291：游戏 294:咨询/顾问 295:信托/拍卖 296: 租赁服务
										 enttype: String,		// 发布职位公司对应公司性质，1476:私营 1477:国有 1478:股份制 1479:外商独资/办事处 1480:中外合资/合作 1481:上市公司 1482:事业单位 1483:政府机关 1484:非营利机构 1485:个人企业
										 fresh: Int,		    // 是否接受应届生，0：不接受 1：接受
										 fuli: String,		  // 福利保障，1:五险一金,8:包住,10:包吃,9:年底双薪,6:周末双休,5:交通补助,7:加班补助,2:餐补,3:话补,4:房补
										                    // 数据格式为：1|8|9|4|3|5|7 或 -
										 additional: String   // 任职要求附加项，552496:会有加班 552497:需要出差 552498:需要管理团队 552499:异地派遣工作, 
										                       // 552496|552498 没有为‘-’
                  )  extends Serializable
{
  
  /**
   * 产生用于逻辑回归和决策树回归模型的特征
   * 企业评分，暂缺
   */
  def lrFeatures: Array[Double] = 
  {
    // 薪资, 学历要求, 工作年限，是否接受应届生，五险一金，包住，包吃，年底双薪,周末双休,交通补助,加班补助,餐补,话补,房补，会有加班，需要出差，需要管理团队，异地派遣工作，企业性质one-hot编码
    Array[Double](salary,education,experience,fresh) ++: fuliFeature ++: additionalFeature ++: enttypeOneHot
  }
  
  /**
   * 产生用于决策树回归模型的特征
   * 公司性质为类别型特征，不需one-hot编码
   */
  def dtFeatures: Array[Double] =
  {
    // 薪资, 学历要求, 工作年限，是否接受应届生,企业性质,五险一金，包住，包吃，年底双薪,周末双休,交通补助,加班补助,餐补,话补,房补，会有加班，需要出差，需要管理团队，异地派遣工作，
    Array[Double](salary,education,experience,fresh,enttype.toInt) ++: fuliFeature ++: additionalFeature
  }
  
  /**
   * 将公司性质进行one-Hot编码
   * 1476:私营 1477:国有 1478:股份制 1479:外商独资/办事处 1480:中外合资/合作 1481:上市公司 1482:事业单位 1483:政府机关 1484:非营利机构 1485:个人企业
   * 数据为：1478 或 -
   */
  def enttypeOneHot: Array[Double] = 
  {
    Array("1476","1477","1478","1479","1480","1481","1482","1483","1484","1485").map(t => if (t == enttype) 1d else 0d)
  }
  
  /**
   * 福利保障特征
   * 1:五险一金,8:包住,10:包吃,9:年底双薪,6:周末双休,5:交通补助,7:加班补助,2:餐补,3:话补,4:房补
   * 数据格式为：1|8|9|4|3|5|7 或 -
   */
  def fuliFeature: Array[Double] = 
  {
    val fulis = fuli.split("\\|").toSet
    Array("1","8","10","9","6","5","7","2","3","4").map(f => if (fulis.contains(f)) 1d else 0d)
  }
  
  /**
   * 任职要求附加项特征
   * 552496:会有加班 552497:需要出差 552498:需要管理团队 552499:异地派遣工作
   * 数据格式为：552496|552498 没有为‘-’
   */
  def additionalFeature: Array[Double] = 
  {
    val additionals = additional.split("\\|").toSet
    Array("552496","552497","552498","552499").map(a => if (additionals.contains(a)) 1d else 0d)
  }
}

object Position
{
  def apply(line: String): Position =
  {
    val field_delim = "\001"
    val values = line.split(field_delim)
    Position(infoid = values(0),
						 scate1 = values(2),
						 scate2 = values(3),
						 scate3 = values(4),
						 title = values(6),
						 userid = values(7),
						 local = values(13),
						 salary = if (values(14) == "-") 1 else values(14).toInt,
						 education = if (values(15) == "-") 1 else values(15).toInt,
						 experience = if (values(16) == "-") 1 else values(16).toInt,
						 trade = values(17),
						 enttype = values(18),
						 fresh = values(20).toInt,
						 fuli = values(19),
						 additional = values(22))
  }
}