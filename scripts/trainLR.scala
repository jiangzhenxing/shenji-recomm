import scala.collection.Map
import com.bj58.shenji.data.Position
import com.bj58.shenji.util._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

  def position(values: Array[String]) = 
  {
    Position(infoid = values(2),
						 scate1 = values(6),
						 scate2 = values(7),
						 scate3 = values(8),
						 title = values(9),
						 userid = values(5),
						 local = values(10),
						 salary = values(11),
						 education = values(12),
						 experience = values(13),
						 trade = values(14),
						 enttype = values(15),
						 fresh = values(17),
						 fuli = values(16),
						 additional = values(18)
						 )
  }

  def labeledPoints(action: String, position: Position, actionCount: Map[String, Int]) =
  {
    val features = Vectors.dense(position.lrFeatures)
    
    val seetelCount = actionCount.getOrElse("seetel", 0).doubleValue
    val messageCount = actionCount.getOrElse("message", 0).doubleValue
    val applyCount = actionCount.getOrElse("apply", 0).doubleValue
    
    val deliveryCount = (seetelCount + messageCount + applyCount).doubleValue // 这部分相当于投递数量
    val clickCount = actionCount.getOrElse("1", 0).doubleValue  // 点击数量
    val noCount = actionCount.getOrElse("0", 0).doubleValue     // 末点击数量
    
    val actionTotal = (deliveryCount + clickCount)  // 正样本数量 = 投递数量 + 点击数量
    val needNum = noCount - actionTotal             // 需要过分抽样的数量
    
    // 过分抽样，让正负样本的数量相当
    val needBase = (if (deliveryCount == 0) math.round(needNum / actionTotal).intValue else math.floor(needNum / actionTotal).intValue) + 1
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    action match {
      case "seetel" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray    // 100
      case "message" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray
      case "apply" => Range(0,needBase * 10).map(x => LabeledPoint(1, features)).toArray
      case "1" => Range(0,needBase).map(x => LabeledPoint(1, features)).toArray        //40
      case "0" => Array(LabeledPoint(0, features))
      case _ => Array(LabeledPoint(0, features))
    }
  }
  
  def labeledPoints2(label: Double, features: Array[Double]) =
  {
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    label match {
      case 2 => Range(0,100).map(x => LabeledPoint(1, Vectors.dense(features))).toArray
      case 1 => Range(0,40).map(x => LabeledPoint(1, Vectors.dense(features))).toArray
      case 0 => Array(LabeledPoint(0, Vectors.dense(features)))
    }
  }

def labeledPoint(values: Array[String]) =
{
    val p = position(values)
    val features = Vectors.dense(p.lrFeatures)
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    val label = values(3) match {
      case "seetel" => 1.0
      case "message" => 1.0
      case "apply" => 1.0
      case "1" => 1.0
      case "0" => 0.0
      case _ => 0.0
    }
    
    LabeledPoint(label, features)
  }

 def labelFetures(values: Array[String]) =
  {
    val p = position(values)
    val features = p.lrFeatures
    
    // 查看电话seetel、在线交谈message、立即申请apply 点击记为1，展现记为0
    val label = values(3) match {
      case "seetel" => 2.0
      case "message" => 2.0
      case "apply" => 2.0
      case "1" => 1.0
      case "0" => 0.0
      case _ => 0.0
    }
    
    (label, features)
  }
  

var max_auc = 0d
var iter = 0

Range(250,300,10).foreach { iteration =>
var total_auc = 0d
var count = 0
val step = iteration / 10d
"m1NfUhbQujboiZKAEM0zNY7OUYVKuk, m1NfUh3QuhR2NWNduDqWi7uWmdFKuk, m1NfUhbQubPhUbG5yWKpPYFn07FKuk, yb0Qwj7_uRRC2YIREycfRM-jm17ZIk, HZGNrH7_u-FHn7I2rytdEhQsnNOaIk, w-RDugRAubGPNLFWmYNoNgPJnAqvNE, uvVYENdyubQVuRw8pHwuEN65PLKOIk, njRWwDuARMmo0A6amNqCuDwiibRKuk, RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk, m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk, pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE, m1NfUh3QuhcYwNuzyAt30duwXMPKuk, UvqNu7K_uyIgyWR60gDvw7GjPA6GNE, NDwwyBqyugRvuDOOE1EosdR3ERRdNE, m1NfUh3QuA_oIR73N-E30DPlRh6Kuk, RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE, m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk, m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk, uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE"
.split(", ")
.foreach { cookieid =>
//val cookieid = "m1NfUhbQujboiZKAEM0zNY7OUYVKuk"
val trainDatas = sc.textFile("data/userdata/train/" + cookieid)
val validData = sc.textFile("data/userdata/valid/" + cookieid)
val testData = sc.textFile("data/userdata/test/" + cookieid)

val rawdatas = trainDatas.map(_.split("\t")).map(values => (values(3), position(values)))
val actionCount = rawdatas.map { case (action, position) => (action,1) }.reduceByKey(_ + _).collectAsMap

//val firstCate3 = rawdatas.filter(values => values(3) == "1" && values(8) != "-").first()(8)
//val bcate3 = sc.broadcast(firstCate3)

val datas = rawdatas.flatMap { case (action, position) => labeledPoints(action, position, actionCount) }.cache

val model = LogisticRegressionWithSGD.train(datas, iteration, 2.0) // 250-2.0:0.6600676006045625 0.6652199055721625

val result = testData.map(_.split("\t")).map(labeledPoint).map(lp => (logistic(vecdot(model.weights.toArray, lp.features.toArray)), lp.label))
// (count: 2526, mean: 0.457514, stdev: 0.137072, max: 0.764279, min: 0.207129)
val auc = new BinaryClassificationMetrics(result).areaUnderROC

println("AUC " + cookieid + " is " + auc)
datas.map(lp => (lp.label,1)).reduceByKey(_+_).foreach(println)

total_auc = total_auc + auc
count = count + 1
}

val avg_auc = total_auc / count
println("avg auc is " + avg_auc)
println("iteration is " + iteration)
if (avg_auc > max_auc) {
  max_auc = avg_auc
  iter = iteration
}
}

println("max_auc is:" + max_auc)
println("the iteration is:" + iter)