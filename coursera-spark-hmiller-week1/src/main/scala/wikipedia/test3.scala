package wikipedia

import org.apache.spark.{SparkConf, SparkContext}

object test3 {

  val sconf = new SparkConf().setMaster("local").setAppName("Local Test ")
  val sc = new SparkContext(sconf)

  val testRdd = sc.parallelize(Seq(("title", "Java Jakarta")))

  val res = testRdd.filter(a=> a._2 contains ("Javaabc")).aggregate(0)((count:Int, elem: (String, String))=> count+1, (p1:Int, p2:Int)=>p1+p2)
  println("Result is: " + res)

}
