package timeusage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Lecture {

  val conf = new SparkConf().setMaster("local").setAppName("Kevin")
  val sc = new SparkContext(conf)

  // Minimum required to start new Spark Session - down the line it should replace Spark Context
  val spark = SparkSession.builder().appName("Kevin").getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.expressions.Aggregator


  case class Abo(val id: Int, val details: (String, String))
  case class Loc(val id: Int, val location: String)

  val as = List(
    Abo(100, ("Paris", "General")),
    Abo(101, ("Lyon", "Half")),
    Abo(102, ("Marseille", "General")),
    Abo(103, ("Lyon", "General"))
  )
  val asDF = sc.parallelize(as).toDF


  val lo = List(
    Loc(100, "Nantes"),
    Loc(102, "Paris"),
    Loc(109, "Geneve")
  )
  val loDF = sc.parallelize(lo).toDF


  val keyValues = List((3, "Me"), (1,"Thi"), (2,"Se"), (3,"ssa"), (1,"sIsA"), (3,"ge:"), (3,"-)"), (2,"cre"), (2,"t"))

  val keyValuesRDD = sc.parallelize(keyValues)
  keyValuesRDD.reduceByKey((x,y)=> x + y).sortBy(_._1).foreach(println(_))

  // Using DataSets
  val keyValuesDS = keyValues.toDS()

  // mapGroups function does not support partial aggregation, and as a result requires shuffling all the data in the Dataset.
  // This is obviously Not Good! - and needs and alternative
  keyValuesDS.groupByKey(a=> a._1).mapGroups((k,v)=> v.reduce((a,b)=> (a._1, a._2+b._2))).show() // Is there any other way where I can select by Column Name?????

  /*
  https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/expressions/Aggregator.html

  case class Data(i: Int)

  val customSummer =  new Aggregator[Data, Int, Int] {
    def zero: Int = 0
    def reduce(b: Int, a: Data): Int = b + a.i
    def merge(b1: Int, b2: Int): Int = b1 + b2
    def finish(r: Int): Int = r
  }.toColumn()

  val ds: Dataset[Data] = ...
  val aggregated = ds.select(customSummer)
  */

  val stringConcat = new Aggregator[(Int, String), String, String] {
    def zero: String = ""
    def reduce (b: String, a: (Int, String)): String = b + a._2
    def merge (b1: String, b2: String): String = b1 + b2
    def finish (r: String): String = r
    override def bufferEncoder: Encoder[String] = Encoders.STRING
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }.toColumn

  keyValuesDS.groupByKey(elem => elem._1).agg(stringConcat.as[String])


}
