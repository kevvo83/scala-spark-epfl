package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.TraversableOnce
import scala.math.Ordering
import scala.util.Sorting

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf(true).setMaster("local[*]").setAppName("Kevin's Spark Master Node/Driver Program")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = {
    val intermed: RDD[String] = sc.textFile(WikipediaData.filePath)
    intermed.map[WikipediaArticle](line => WikipediaData.parse(line)).persist()
  }

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(wikiart => wikiart.mentionsLanguage(lang)).
      aggregate[Int](0)((count:Int, elem:WikipediaArticle)=> count + 1, ((p1:Int,p2:Int)=>p1+p2))
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    object articleCountsOrder extends Ordering[(String, Int)]{
      def compare (a:(String, Int), b:(String, Int)): Int = a._2 compare b._2
    }

    def impl(in: List[String], acc: List[(String, Int)]): List[(String, Int)] = in match {
      case List() => acc
      case _ => {
        impl(in.tail, (in.head,occurrencesOfLang(in.head, rdd)) :: acc)
      }
    }
    val retVal: List[(String, Int)] = impl(langs, List()).sorted(articleCountsOrder.reverse)
    retVal
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {

    // Result using For-Expressions - Compare if using flatMap/Map makes a big difference to speed?
    /*val result = for {
      wikiarticle <- rdd
      lang <- langs
      if wikiarticle.mentionsLanguage(lang)
    } yield (lang, wikiarticle)
    result.groupByKey()
    */


    val index: RDD[Option[(String, WikipediaArticle)]] = rdd.flatMap(article =>
      langs.map(lang => article.mentionsLanguage(lang) match {
        case true => Some(lang, article)
        case false => None
      })).filter(_.isDefined)

    index.map(a=> a.get).groupByKey()

    // Compared the time between the for-comprehension and the flatMap - the times are comparable and are almost the same!!!!!

  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {

    object sorter extends Ordering[(String, Int)] {
      def compare (a: (String, Int), b: (String, Int)): Int =  a._2 compare b._2
    }

    //val res:List[(String, Int)] = (index mapValues (a => a.size) collect() toList).sorted(sorter.reverse)
    //res

    index mapValues (a=> a.size) collect() sorted (sorter.reverse) toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    object sorter extends Ordering[(String, Int)] {
      def compare (a: (String, Int), b: (String, Int)): Int =  a._2 compare b._2
    }

    // Result using For-Expressions - Compare if using flatMap/Map makes a big difference to speed?
    val intermed = for {
      article <- rdd
      lang <- langs
      if (article.mentionsLanguage(lang))
    } yield (lang, article)

    intermed.aggregateByKey[Int](0)((accum:Int, article:WikipediaArticle) => accum+1,
                                                (counta: Int, countb: Int) => counta + countb).
                                                collect().sorted(sorter.reverse).toList
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = timed("Part 2a: making the inverted index", makeIndex(langs, wikiRdd))

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2b: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
