package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import TimeUsage._
import org.junit.Ignore

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  trait Environment {
    val rdd = spark.sparkContext.textFile(fsPath("/timeusage/atussum.csv"))

    val headerColumns = rdd.first().split(",").to[List]

    val schema = dfSchema(headerColumns)

    val (primaryNeedsColumns: List[Column], workColumns: List[Column], otherColumns: List[Column]) = classifiedColumns(headerColumns)

    val primaryneedsactslist: List[String] = List("t01","t03","t11","t1801","t1803")
    val workingactslist: List[String] = List("t05","t1805")

    def checkNeedsColumns(in: List[Column], acc: Boolean, listtobecheckedagainst: List[String]): Boolean =
    {
      def checkarray(input: String): Boolean = {
        var i=0
        var acc: Boolean = false
        while (i<listtobecheckedagainst.length && !acc) {
          if (input.matches("^"+listtobecheckedagainst(i)+".*")) acc = true
          i+=1
        }
        if (!acc) println("Input value " + input + "is not supposed to be in " + listtobecheckedagainst.toString())
        acc
      }
      val returnVal = in match {
        case Nil => acc
        case _ => checkNeedsColumns(in.tail, checkarray(in.head.toString()) && acc, listtobecheckedagainst)
      }
      returnVal
    }
  }

  trait Environment2 extends Environment {

    val data = spark.sparkContext.textFile(fsPath("/timeusage/atussum.csv"))
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .sample(false, 0.20)
      .map(_.split(",").to[List])
      .map(row)

    val initDf =
      spark.createDataFrame(data, schema)

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val timeusagesummaryDs: Dataset[TimeUsageRow] = timeUsageSummaryTyped(summaryDf)
    val timeusagesummartDfwithSql = timeUsageGroupedSql(summaryDf)
  }

  test ("Test the dfSchema() function creates schema correctly") {
    new Environment {
      assert(headerColumns.length === schema.length, "Check that Schema's Length === Number of Columns in the first line")

      assert(schema(0).dataType.isInstanceOf[StringType], "Struct Field " + schema(0).name + " type is meant to be StringType")

      var i: Int = 1
      while (i < schema.length) {
        assert(schema(i).dataType.isInstanceOf[DoubleType], "Struct Field " + schema(i).name + " type is meant to be DoubleType")
        i += 1
      }
    }
  }

  test ("Test the row function works properly") {
    new Environment {
      val testline: List[String] = List("oohlahlah", "1", "3", "4", "5")

      val funcallresult: Row = row(testline)
      assert(funcallresult.length === 5, "The size of the Row needs to be 5")
      assert(funcallresult(0).isInstanceOf[String], "Row(0) should be of type String")
      assert(funcallresult(1).isInstanceOf[Double], "Row(1) should be of type String")
      assert(funcallresult(2).isInstanceOf[Double], "Row(2) should be of type String")
      assert(funcallresult(3).isInstanceOf[Double], "Row(3) should be of type String")
      assert(funcallresult(4).isInstanceOf[Double], "Row(4) should be of type String")

    }
  }

  /*test ("Verify the Initial DataFrame has no Null values in it's Fields"){
    new Environment2 {
      initDf.take(5).foreach(row => assert(row.anyNull, "The Initial DF should Not have any Null values in it's rows"))
    }
  }*/

  test ("Get classified Columns Test - verify that the 1st element of the Tuple returned only contains PRIMARY/WORK NEEDS") {
    new Environment {
      assert(checkNeedsColumns(primaryNeedsColumns, true, primaryneedsactslist), "Only the Activities starting with" + primaryneedsactslist.toString() + "should be in this structure")
      assert(checkNeedsColumns(workColumns, true, workingactslist), "Only the Activities starting with" + workingactslist.toString() + "should be in this structure")
    }
  }

  test ("Tests on timeUsageSummary() DF") {
    new Environment2 {
      summaryDf.collect().foreach(row => {
        assert(row.length === 6, "timeUsageSummary() doesn't return DF with 6-element Rows")
        assert(row(0).isInstanceOf[String], "timeUsageSummary() DF Row(0) 'working' is not of type String")
        assert(row(1).isInstanceOf[String], "timeUsageSummary() DF Row(1) 'sex' is not of type String")
        assert(row(2).isInstanceOf[String], "timeUsageSummary() DF Row(2) 'age' is not of type String")
        assert(row(3).isInstanceOf[Double], "timeUsageSummary() DF Row(3) 'primaryNeeds' is not of type String")
        assert(row(4).isInstanceOf[Double], "timeUsageSummary() DF Row(4) 'work' is not of type String")
        assert(row(5).isInstanceOf[Double], "timeUsageSummary() DF Row(5) 'other' is not of type String")
        assert(!row.anyNull, "timeUsageSummary() DF has rows with Nulls in them")
      })

    }
  }

  test("Test on timeUsageSummaryTyped(), after DF -> DS Conversion") {
    new Environment2 {

      import org.apache.spark.sql.functions._

      // Compare all the elements of the DF vs the DS and ensure that they're all the same!!
      val df: List[Row] = summaryDf.take(10).toList
      val ds: List[TimeUsageRow] = timeusagesummaryDs.take(10).toList

      val leftOuterJoin = summaryDf.join(timeusagesummaryDs,
        (summaryDf("working") === timeusagesummaryDs("working")) && (summaryDf("sex") === timeusagesummaryDs("sex")) &&
          (summaryDf("primaryNeeds") === timeusagesummaryDs("primaryNeeds")),
        "left_outer")

      leftOuterJoin.collect().foreach(row => assert(!row.anyNull, "Key Difference in this row " + row))

      val newSchemaNames: Seq[String] = Seq("working", "sex", "age", "primaryNeeds", "work", "other",
                                              "working2", "sex2", "age2", "primaryNeeds2", "work2", "other2")
      val ldf = leftOuterJoin.toDF(newSchemaNames: _*)

      ldf.
        withColumn("primaryNeedsComp",
          when(ldf("working").isNotNull && ldf("working2").isNotNull && (ldf("primaryNeeds") === ldf("primaryNeeds2")), "OK").
        otherwise("NOK")).collect().
        foreach(row => assert(row.getAs[String]("primaryNeedsComp") !== "NOK"))
    }
  }

}
