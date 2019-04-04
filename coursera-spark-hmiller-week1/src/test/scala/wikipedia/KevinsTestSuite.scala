package wikipedia

import org.scalatest.FunSuite
import org.apache.spark.rdd.RDD

class KevinsTestSuite extends FunSuite {

  def categorizer(art: WikipediaArticle, langs:List[String]): List[String] = {
    val res: List[String] = for {
      lang <- langs
      if art.mentionsLanguage(lang)
    } yield lang
    res
  }

  test("Testing rankLangs") {
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    val res = ranked.head._1 === "Scala"
    assert(res)
  }

  test("Testing the make Index Function") {
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional")
    )
    val rdd = sc.parallelize(articles)

    //val index = rdd flatMap (a=> for (matchedlang <- categorizer(a, langs)) yield (matchedlang, a))

    val index1 = for {
      article <- rdd
      lang <- langs
      if (article.mentionsLanguage(lang))
    } yield (lang, article)

    /*val index: RDD[(String, WikipediaArticle)] = rdd.flatMap(a=> (
      langs map (lang => a.mentionsLanguage(lang) match {
        case true => (lang, a)
        case false => ()
      })
    )).filter(_.isInstanceOf[(String, WikipediaArticle)])*/

    /*val index: RDD[(String, WikipediaArticle)] = rdd.flatMap(article => (
      langs.map(lang => if (article.mentionsLanguage(lang)) (lang, article) else ())
    ))*/

    val index: RDD[Option[(String, WikipediaArticle)]] = rdd.flatMap(article =>
        langs.map(lang => article.mentionsLanguage(lang) match {
          case true => Some(lang, article)
          case false => None
        })).filter(_.isDefined)

    index.map(a=> a.get).groupByKey()

  }

}
