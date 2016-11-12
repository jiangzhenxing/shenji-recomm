import scala.collection.Map
import scala.io.Source
import com.bj58.shenji.data.Position
import com.bj58.shenji.util._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

def label(values: Array[String]) = values(3) match {
      case "seetel" => 1.0
      case "message" => 1.0
      case "apply" => 1.0
      case "1" => 1.0
      case "0" => 0.0
      case _ => 0.0
    }

val click_map = Source.fromFile("data/test_position_click").getLines().map(_.split("\t")).map(values => (values(0), values(1).toDouble)).toMap


"m1NfUhbQujboiZKAEM0zNY7OUYVKuk, m1NfUh3QuhR2NWNduDqWi7uWmdFKuk, m1NfUhbQubPhUbG5yWKpPYFn07FKuk, yb0Qwj7_uRRC2YIREycfRM-jm17ZIk, HZGNrH7_u-FHn7I2rytdEhQsnNOaIk, w-RDugRAubGPNLFWmYNoNgPJnAqvNE, uvVYENdyubQVuRw8pHwuEN65PLKOIk, njRWwDuARMmo0A6amNqCuDwiibRKuk, RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk, m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk, pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE, m1NfUh3QuhcYwNuzyAt30duwXMPKuk, UvqNu7K_uyIgyWR60gDvw7GjPA6GNE, NDwwyBqyugRvuDOOE1EosdR3ERRdNE, m1NfUh3QuA_oIR73N-E30DPlRh6Kuk, RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE, m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk, m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk, uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE"
.split(", ")
.foreach { cookieid =>
val cookieid = "m1NfUhbQujboiZKAEM0zNY7OUYVKuk"
val trainDatas = sc.textFile("data/userdata/train/" + cookieid)
val validData = sc.textFile("data/userdata/valid/" + cookieid)
val testData = sc.textFile("data/userdata/test/" + cookieid)

val result = trainDatas.union(validData).union(testData).map(_.split("\t")).map(values => (label(values), click_map.getOrElse(values(2),10.0)))

val auc = new BinaryClassificationMetrics(result).areaUnderROC

println("AUC " + cookieid + " is " + auc)
// datas.map(lp => (lp.label,1)).reduceByKey(_+_).foreach(println)

}