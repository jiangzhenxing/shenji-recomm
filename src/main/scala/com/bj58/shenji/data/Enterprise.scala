package com.bj58.shenji.data

/**
 * 企业基本信息数据
 * @author jiangzhenxing
 * @date 2016-10-31
 */
case class Enterprise( val id: String,		    // 企业ID
											 val adminid: String,		// 企业管理员用户ID
											 val name: String,		  // 企业名
											 val address: String,		// 企业地址
											 val capitalregistered: String,	// 注册资金
											 val enterprisetype: String,		// 企业类型
											 val businessfield: String,		  // 企业经营范围（尽量使用trade）
											 val size: String,		  // 企业规模，1741：1-49人 1742:50-99人 1743:100-499人 1474:500-999人 1475：1000人以上
											 val cityid: String,		// 城市ID，归属地域，可与hdp_58_common_defaultdb.ds_dict_cmc_local关联
											 val homepage: String,		// 企业主页
											 val logo: String,		    // 企业logo位置
											 val introduction: String,	// 企业介绍
											 val authstate: String,		  // 验证状态：0待审核 1审核通过 2 审核拒绝
											 val updatedate: String,		// 最后更新时间戳
											 val createdate: String,		// 创建时间戳
											 val area: String,		// 区域商圈
											 val trade: String,		// 所属行业，244:互联网/电子商务 245:计算机软件 246:计算机硬件 247:IT服务/系统集成 248:通信/电信 
                                            // 249:电子技术/半导体/集成电路 250:仪器仪表/工业自动化 251:财务/审计 252:金融/银行 
                                            // 253:保险 254:贸易/进出口 255:批发/零售 256:快速消费品(食品/饮料等) 257:耐用消费品（家具/家电等） 
                                            // 258:服装/纺织/皮革 259:办公用品及设备 260:钢铁/机械/设备/重工 261:汽车/摩托车 262:医疗/保健/卫生/美容 
                                            // 263:生物/制药/医疗器械 264:广告/创意 265:公关/市场推广/会展 266:文体/影视/艺术 267:媒体传播 
                                            // 268:出版/印刷/造纸 269:房地产/物业管理 270:建筑/建材 271:家居/室内设计/装潢 272:中介/专业服务 
                                            // 273:检测/认证 274:法律/法务 275:教育/科研/培训 276:旅游/酒店 277:娱乐休闲/餐饮/服务 
                                            // 278:交通/运输/物流 279:航天/航空 280:化工/采掘/冶炼 281:能源（电力/水利/矿产） 
                                            // 282:原材料和加工 283:政府/非盈利机构 284:环保 285:农林牧渔 286:多元化集团 3527:人力资源服务 
                                            // 287:其他行业 288:陶瓷卫浴 289:家具灯饰 290:纺织服饰  291：游戏 294:咨询/顾问 295:信托/拍卖 296: 租赁服务
											 val en_type: String,		// 企业性质，1476:私营 1477:国有 1478:股份制 1479:外商独资/办事处 1480:中外合资/合作 1481:上市公司 1482:事业单位 1483:政府机关 1484:非营利机构 1485:个人企业
											 val category: String,	// 企业业务类型
											 val fuli: String,		  // 企业福利（固定，用于用户勾选），1:五险一金,8:包住,10:包吃,9:年底双薪,6:周末双休,5:交通补助,7:加班补助,2:餐补,3:话补,4:房补
											 val addfuli: String,		// 企业福利（不固定，用户自己添加） 
											 val dt: String		      // 分区字段
)
{
  
}

/**
 * @author jiangzx
 */
object Enterprise {
  def apply() =
  {
    
  }
}