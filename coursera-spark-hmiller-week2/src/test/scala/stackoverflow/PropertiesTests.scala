package stackoverflow

import org.junit.runner.RunWith
import org.scalacheck.Properties
import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalatest._
import org.scalatest.junit.JUnitRunner


class PropertiesTests extends Properties("String") {

  val propMakeList = forAll { n: Int =>
    (n >= 0 && n < 10000) ==> (List.fill(n)("").length == n)
  }

  property ("Length Check on the List") = {
    propMakeList
  }

  property("startsWith") = forAll { (a: String, b: String) =>
    (a+b).startsWith(a)
  }

}
