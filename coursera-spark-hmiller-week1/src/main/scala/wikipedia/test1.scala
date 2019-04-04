package wikipedia

import org.apache.spark.{SparkConf, SparkContext}

object test1 {

  val sconf = new SparkConf().setMaster("local").setAppName("Local Test ")
  val sc = new SparkContext(sconf)

  val list1 = List(("Vendor A",2000.0),("Vendor A",1233.2),("Vendor C",1200.1),("Vendor D",20.1),("Vendor A",345.1),("Vendor D",918.1))
  val rdd1 = sc.parallelize(list1)

  // Prints Pair of (Sum Budgets, # Events) by Vendor
  val res = rdd1.mapValues(budget => (budget,1)).reduceByKey((a:(Double, Int),b:(Double, Int)) => (a._1+b._1, a._2+b._2)).collect()
  res.foreach(println(_))

  // Print Average Budget by Vendor
  val res2 = rdd1.mapValues(budget => (budget,1)).reduceByKey((a:(Double, Int), b:(Double,Int)) => (a._1+b._1, a._2+b._2)).mapValues(budgettup => budgettup._1/budgettup._2).collect()
  res2.foreach(println(_))
  // Equivalent to res2 computation above in the 2nd mapValues - instead in this case, I've used a case to pattern match against the pairs
  val res3 = rdd1.mapValues(budget => (budget,1)).reduceByKey((a:(Double, Int), b:(Double,Int)) => (a._1+b._1, a._2+b._2)).mapValues(_ match {case (budget, numevents) => budget/numevents}).collect()

  sc.stop()

}
