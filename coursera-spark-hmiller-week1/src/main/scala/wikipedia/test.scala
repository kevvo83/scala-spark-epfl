package wikipedia

import org.apache.spark.{SparkContext, SparkConf}

object test {

  val sconf = new SparkConf().setMaster("local").setAppName("Local Test ")
  val sc = new SparkContext(sconf)

  val list1:List[String] = List("abc","def","abc","jkl","mno","pqr")
  val rdd1 = sc.parallelize(list1)

  def grouper(in: String): Int = in match {
    case "abc" => 1
    case _ => 2
  }

  val res = rdd1.groupBy(elem => grouper(elem)).groupByKey().collect()
  res.foreach(println(_))

  sc.stop()

  // Regular Scala Collections
  val st1 = list1.groupBy(elem => grouper(elem))

}

/*object main extends App {
  test3
}*/
