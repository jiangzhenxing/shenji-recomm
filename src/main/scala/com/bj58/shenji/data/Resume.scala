package com.bj58.shenji.data

import java.util.Date

/**
 * 简历基本信息数据
 * @author jiangzhenxing
 * @created 2016-10-31
 */
case class Resume( id: String,				    // 简历ID
                   userid: String,		    // 简历用户ID
									 targetCateid: String,  // 目标职位类别ID，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联 eg:2620,2811,2414,2238,2701
									 targetAreaid: String,	// 目标城市
									 deliveryCateid: String,// 真实投递类别 没有为'-'
									 deliveryAreaid: String,  // 真实投递地域
									 salary: Int,		        // 期望薪资，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
									                        // 期望薪资，0：面议 1：1000以下 2：1000-2000 3：2000-3000 4: 3000-5000 5：5000-8000  6：8000-12000 7：12000-20000 8：20000-25000 9：25000以上
									 nowSalary: Int,		    // 现薪资段，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
									 education: Int,		    // 教育程度，1:高中以下 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
									 rdoidentity: Int,		// 个人身份，1:在校学生 0:社会人才
									 workedyears: Int,    // 工作年限，0:不限 1:1-3年 2:3-5年 3:5-10年 4:10年以上 5:应届生 6:1年以下
									 complete: Int,		    // 简历完整度,分数，如：70,80
									 updateDate: Date,		  // 最近更新时间戳
									 addDate: Date		      // 创建时间戳
                  ) extends Serializable
{
  
   
}

/**
 * @author jiangzx
 */
object Resume 
{
  def apply(record: String): Resume = 
  {
    val values = record.split("\001")
    Resume( id = values(0),
            userid = values(1),		    // 简历用户ID
						targetCateid = values(3),  // 目标职位类别ID，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联 eg:2620,2811,2414,2238,2701
						targetAreaid = values(21),	// 目标城市
						deliveryCateid = values(29),// 真实投递类别 没有为'-'
						deliveryAreaid = values(30),  // 真实投递地域
						salary = if (values(6) == "-") 1 else values(6).toInt,		        // 期望薪资，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
						                       // 期望薪资，0：面议 1：1000以下 2：1000-2000 3：2000-3000 4: 3000-5000 5：5000-8000  6：8000-12000 7：12000-20000 8：20000-25000 9：25000以上
						nowSalary = if (values(20) == "-") 1 else values(20).toInt,		    // 现薪资段，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
						education = if (values(7) == "-") 1 else values(7).toInt,		    // 教育程度，1:高中以下 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
						rdoidentity = values(23).toInt,		// 个人身份，1:在校学生 0:社会人才
						workedyears = if (values(5) == "-") 0 else values(5).toInt,    // 工作年限，0:不限 1:1-3年 2:3-5年 3:5-10年 4:10年以上 5:应届生 6:1年以下
						complete = values(17).toInt,		    // 简历完整度,分数，如：70,80
						updateDate = new Date(values(18).toLong),		  // 最近更新时间戳
						addDate = new Date(values(19).toLong) )		      // 创建时间戳)
  }
}

class ResumeInfo(  val id: String,				    // 简历ID
									 val userid: String,		    // 简历用户ID
									 val nowPosition: String,   // 当前职位
									 val targetCateid: String,  // 目标职位类别ID，可与hdp_58_common_defaultdb.ds_dict_cmc_category关联
									 val targetPosition: String,// 目标职位
									 val salary: Int,		        // 期望薪资，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
									 val education: Int,		    // 教育程度，1:不限 2:高中 3:技校 4:中专 5:大专 6:本科 7:硕士 8:博士
									 val openstate: String,		  // 简历开放状态，0:保密 1:公开 2:只对58认认证的公开
									 val letter: String,		    // 自我介绍
									 val birthday: String,		  // 出生日期
									 val gender: String,		    // 性别，0：男 1：女
									 val native: String,		    // 籍贯
									 val jobstate: String,		  // 求职状态
									 val marriage: String,		  // 婚姻情况
									 val userip: String,		    // 用户IP
									 val areaid: String,		    // 居住城市
									 val complete: String,		  // 简历完整度
									 val updateDate: Date,		// 最近更新时间戳
									 val addDate: Date,		    // 创建时间戳
									 val nowSalary: Int,		  // 现薪资段，1：面议 2：1000以下 3：1000-2000 4:2000-3000 5:3000-5000 6:5000-8000 7:8000-12000 8:12000-20000 9:20000-25000 10:25000以上
									 val targetAreaid: String,	// 目标城市
									 val source: String,		    // 发布来源，0:未知 1:简历 2:webappjl 3:mjl 4:wapjl 5:微简历 6:技能人才 7:技能人才无线 8线下导入简历
									 val rdoidentity: String,		// 个人身份，1:在校学生 0:社会人才
									 val certificates: String,	// 证书情况
									 val scholarship: String,		// 奖学金情况
									 val awards: String,		    // 获奖情况
									 val positionPerformance: String,		// 个人职责
									 val skills: String,		    // 技能
									 val deliveryCateid: String,// 真实投递类别
									 val deliveryAreaid: String,// 真实投递地域
									 val advantages1: String,		// 亮点1
									 val advantages2: String,		// 亮点2
									 val advantages3: String,		// 亮点3
									 val dt: String		          // 分区字段
                  )
{
  
}

/**
 * 简历-教育经历数据
 */
class Education( val id: String,				    // 教育经历ID
								 val resumeid: String,			// 简历ID
								 val startdate: String,			// 开始日期
								 val enddate: String,				// 结束日期
								 val school: String,				// 学校
								 val professionalid: String,// 专业ID
								 val professional: String,	// 专业名
								 val degree: String,				// 
								 val highlight: String,			// 亮点
								 val updatedate: String,		// 最近更新时间戳
								 val adddate: String,				// 创建时间戳
								 val dt: String				      // 分区字段
                 )
{
  
}

/**
 * 简历-工作经验数据
 */
class Experience( val id: String,				// 工作经验ID
								  val resumeid: String,				// 简历ID
								  val company: String,				// 公司名
								  val companytype: String,				// 公司类型
								  val companytrade: String,				// 公司行业
								  val startdate: String,				// 开始日期
								  val enddate: String,				// 结束日期
								  val categoryid: String,				// 
								  val positionname: String,				// 职位名称
								  val salary: String,				// 薪资段
								  val department: String,				// 
								  val description: String,				// 工作内容
								  val reportto: String,				// 
								  val performance: String,				// 
								  val updatedate: String,				// 最近更新时间戳
								  val adddate: String,				// 创建时间戳
								  val dt: String				// 分区字段
                  )
{
  
}

/**
 * 简历-项目经历数据
 */
class Project( val id: String,				  // 项目经验ID
							 val resumeid: String,		// 简历ID
							 val projectname: String,	// 项目名称
							 val startdate: String,		// 开始日期
							 val enddate: String,			// 结束日期
							 val introduction: String,// 项目介绍
							 val others: String,			// 
							 val updatedate: String,	// 最近更新日期
							 val adddate: String,			// 创建日期
							 val dt: String				    // 分区字段
               )
{
  
}