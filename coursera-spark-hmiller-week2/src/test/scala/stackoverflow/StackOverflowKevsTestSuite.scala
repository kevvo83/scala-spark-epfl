package stackoverflow

import org.scalatest.FunSuite
import StackOverflow._
import org.apache.spark.rdd.RDD

class StackOverflowKevsTestSuite extends FunSuite {

  trait TestSuite {
    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")

    val raw = rawPostings(lines)
    //val raw = sc.parallelize(raw_t.takeSample(false, 30000))

    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    val oldcentroids: Array[(Int, Int)] = sampleVectors(vectors)

    //val vectorsSample = vectors.take()
  }

  test ("Test that the Grouped Function Returns 1 (Question, Answer) pair for QID==13730490") {
    new TestSuite {
      val QandAofQID_13730490 = grouped.filter((a) => !(a._2.filter(b => (b._2.parentId.getOrElse(0) == 13730490)).isEmpty)).collect().toList
      assert(QandAofQID_13730490.map({ case ((a: QID, b: Iterable[(Question, Answer)])) => b.size }) === List(1))
    }
  }

  test ("Test that the Grouped Function Returns 4 (Question, Answer) pairs for QID==12590505") {
    new TestSuite {
      // 1 QUESTION of ID 12590505, with 4 ANSWERS of ID 12590505 - so 4 Pairs of (QUESTION, ANSWER)
      val QandAofQID_12590505 = grouped.filter((a) => !(a._2.filter(b => (b._2.parentId.getOrElse(0)==12590505)).isEmpty)).collect().toList
      assert(QandAofQID_12590505(0)._2.size == 4)
      //assert(QandAofQID_12590505.map({case ((a:QID, b:Iterable[(Question, Answer)])) => b.size}) === List(4))
    }
  }

  test ("Test that the Scored Function returns Highest Scored Answer of 6, for the QID==12590505") {
    new TestSuite {
      //assert(scored.filter((a) => (a._1.id == 12590505)).collect().toList.map({case (a:Question,b:Int) => b}) === 6)
      val result = scored.filter((a) => (a._1.id == 12590505)).collect().toList
      assert(result.length == 1)
      assert(result.maxBy(_._2)._2 == 6)
    }
  }

  test ("Test that the Scored Function returns Highest Scored Answer of 6, for the QID==1275960") {
    new TestSuite {
      //assert(scored.filter((a) => (a._1.id == 12590505)).collect().toList.map({case (a:Question,b:Int) => b}) === 6)
      val result = scored.filter((a) => (a._1.id == 1275960)).collect().toList
      assert(result.length == 1)
      assert(result.maxBy(_._2)._2 == 5)
      assert(result(0)._2 == 5)
      assert(result(0)._1.isInstanceOf[Posting])
      assert(result(0)._2.isInstanceOf[HighScore])
    }
  }
    // FILTERED RDD from above Filter ops returns structures that look like below
    /*(13730490,CompactBuffer((Posting(1,13730490,None,None,1,Some(Ruby)),
                              Posting(2,13730567,None,Some(13730490),5,None)
                            )))*/

  test("Test that 'scored' RDD's size is 2121822") {
    new TestSuite {
      assert(scored.collect().toList.length === 2121822)
    }
  }

  test("Verify that sampleVectors method returns an Array[(Int, Int)] of size.m == kMeansCluster") {
    new TestSuite {
      assert(oldcentroids.size === kmeansKernels, "Size of Sample Vectors output must be equal to # of KMeansKernels")
    }
  }

  test ("Test that NewMeans Function returns an Array[(Int, Int] of the same size") {
    new TestSuite {
      val newcentroids = computeNewCentroids(oldcentroids, vectors)
      assert(newcentroids.length === oldcentroids.length, "New Centroid Length must be Equal to Old Centroid Length")
    }
  }


}
