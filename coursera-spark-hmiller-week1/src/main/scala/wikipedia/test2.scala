package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object test2 {

  val sconf = new SparkConf().setMaster("local").setAppName("Local Test ")
  val sc = new SparkContext(sconf)

  /*
  val fp:String = this.getClass().getClassLoader().getResource("wikipedia/visits.csv").getFile()

  val csvrdd = sc.textFile(fp)

  val patt: Regex = """^([a-zA-Z ]+);([a-zA-Z ]+);([a-zA-Z]+)$""".r

  val struc2 = List(("10",20))
  */

  val abos = sc.parallelize(List((101, ("Ruetli", "AG")), (102, ("Brelaz","DemiTarif")), (103, ("Gress","DemiTarifVisa")), (104, ("Schatten","DemiTarif"))))
  val locations = sc.parallelize(List((101,"Bern"), (101,"Thun"), (102,"Lausanne"), (102,"Geneve"), (102,"Nyon"), (103,"Zurich"), (103,"St-Gallen"),
                                        (103, "Chur")))

  val subscriptionsWithLocation = abos join locations
  subscriptionsWithLocation.collect().foreach(println(_))

  val abosWithLocationData = abos leftOuterJoin (locations)

  // Oh wow, the println would evaluate on collect() call, who knew? :-) I had my doubts about that to be honest
  abosWithLocationData.mapValues({
    case ((a,b),Some(c)) => println("Username: " + a + ", Tix Type: "+ b + ", Purchase Location: " + c)
    case ((a,b),None) => println("No Purchase Location Data available for Username: " + a + " of Tix Type: " + b)
  }).collect()

  val customersWithOptionalSubscriptions = abos rightOuterJoin (locations)

  customersWithOptionalSubscriptions.mapValues({
    case (Some((a,b)), c) => println ("Username " + a)
    case (None, c) => println("Purchases made in "+c+" were made by Cash or Paper Ticket")
  }).collect()

  val testRdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))




  /*val ab = csvrdd.map(elem => {
    for (pres <- patt.findAllMatchIn(elem)) yield (pres.group(1), pres.group(3))
  })*/



}
