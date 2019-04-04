import scala.util.matching.Regex

object session {

  val stringtest = "abcd;efgh;ijkl"

  val patt:Regex = """^([a-zA-Z ]+);([a-zA-Z ]+);([a-zA-Z]+)$""".r

  val res1 = patt.findAllMatchIn(stringtest).toList

  val response = for (a <- res1) yield (a.group(1), a.group(2), a.group(3))

  stringtest.split(";")



  //response
  //for (matchInst <- res1)
  //  println(matchInst.group(1) + ":")

  //this.getClass().getClassLoader().getResource("wikipedia/visits.csv").getFile()



}