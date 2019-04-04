import stackoverflow.StackOverflow._

object session {

  val a:List[(Int, List[Int])] = List((1,List(2,3,4,5,6,7,8,9)))

  a.map({case (a:Int,b:Iterable[Int]) => b.size})


  val b: List[Int] = List(1,2,3,4,5)

  b.length


  //Map[Int, Int](1->1, 2->2, 3->3).toList reduce ((a,b)=> a._2 + b._2)


  var first: Array[(Int, (Int, Int))] = Array((9,(9,10)),(10,(10,11)),(11,(11,12)))

  var second:Array[(Int, Int)] = Array((2,3))

  //for (() <- first) yield first.

  for((idx, value) <- first) {
    println("Idx is: " + idx + "and value is: " + value.toString + "\n")
  }

  val testmap: Map[Int, Int] = Map(1->1, 2->2, 3->4, 5->5, 7->9)
  testmap.size


}