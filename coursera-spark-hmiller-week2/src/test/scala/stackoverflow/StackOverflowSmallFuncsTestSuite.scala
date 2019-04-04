package stackoverflow

import org.scalatest.FunSuite
import stackoverflow.StackOverflow.calcMedian

class StackOverflowSmallFuncsTestSuite extends FunSuite {

  test ("test that the calcMedian function works on Odd and Even Lists") {
      val listtobeTested = List(1,3,5,7,8,10,22,34,44,55)
      assert(calcMedian(listtobeTested) == 9)

      val listtobeTesttedOdd = List(1,3,5,7,8,10,22,34,44,55,56)
      assert(calcMedian(listtobeTesttedOdd)==10)
  }
}
