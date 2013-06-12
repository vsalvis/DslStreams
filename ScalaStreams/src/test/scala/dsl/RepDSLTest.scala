package dsl


import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.internal.ScalaCompile
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileOutputStream
import scala.reflect.SourceContext
import scala.collection.mutable.HashMap



trait RepStreamProg extends RepStreamOps with NumericOps
  with OrderingOps with OverloadHack {

  def testRepStream(i: Rep[Int], m: RepStreamOp[Int]) = {
    m.onData(unit(0))
    m.onData(unit(1))
    m.onData(unit(2))
    m.onData(unit(3))
    m.onData(unit(4))
    m.onData(i)
    m.flush
    m.onData(unit(42))
    
    println(unit("----"))
  }

  def test1(i: Rep[Int]) = {
    testRepStream(i, new RepMapOp[Int, Int]({x: Rep[Int] => x * unit(2)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepFilterOp[Int]({x: Rep[Int] => x > unit(4)}, new RepPrintOp[Int])))))
  }

  def test2(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int] map ({x: Rep[Int] => x * unit(2)}) map ({x: Rep[Int] => x + unit(1)}) map({x: Rep[Int] => x + unit(1)}) filter ({x: Rep[Int] => x > unit(4)}) print)
  }

  def test3(i: Rep[Int]) = {
    testRepStream(i, new RepMapOp[Int, Int]({x: Rep[Int] => 2 * x}, new RepPrintOp[Int]))
    testRepStream(i, new RepFilterOp[Int]({x: Rep[Int] => x > unit(3)}, new RepPrintOp[Int]))
    val s = new RepReduceOp[Int]({(x, y) => x + y}, new RepPrintOp[Int])
    testRepStream(i, s)
    s.onData(unit(43))
    testRepStream(i, new RepFoldOp[Int, Int]({(x, y) => x + y}, 1, new RepPrintOp[Int]))
    testRepStream(i, new RepFlatMapOp[Int, Int]({x => x :: x :: Nil}, new RepPrintOp[Int]))
    println(unit("===="))
  }

  def test3b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int] map ({x: Rep[Int] => 2 * x}) print)
    testRepStream(i, RepStream[Int] filter ({x: Rep[Int] => x > unit(3)}) print)
    val s: RepStreamOp[Int] = RepStream[Int].reduce({(x, y) => x + y}).print
    testRepStream(i, s)
    s.onData(unit(43))
    testRepStream(i, RepStream[Int].fold[Int]({(x, y) => x + y}, 1) print)
    testRepStream(i, RepStream[Int].flatMap({x => x :: x :: Nil}).print)
    println(unit("===="))
  }

  def test4(i: Rep[Int]) = {
    testRepStream(i, new RepDropOp[Int](2, new RepPrintOp[Int]))
    testRepStream(i, new RepDropWhileOp[Int]({x: Rep[Int] => x > unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepDropWhileOp[Int]({x: Rep[Int] => x < unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeOp[Int](2, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeWhileOp[Int]({x: Rep[Int] => x > unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeWhileOp[Int]({x: Rep[Int] => x < unit(3)}, new RepPrintOp[Int]))
    println(unit("===="))
  }

  def test4b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].drop(2).print)
    testRepStream(i, RepStream[Int].dropWhile({x: Rep[Int] => x > unit(3)}).print)
    testRepStream(i, RepStream[Int].dropWhile({x: Rep[Int] => x < unit(3)}).print)
    testRepStream(i, RepStream[Int].take(2).print)
    testRepStream(i, RepStream[Int].takeWhile({x: Rep[Int] => x > unit(3)}).print)
    testRepStream(i, RepStream[Int].takeWhile({x: Rep[Int] => x < unit(3)}).print)
    println(unit("===="))
  }

  def test5(i: Rep[Int]) = {
    testRepStream(i, new RepPrependOp[Int](unit(-2) :: unit(-1) :: Nil, new RepPrintOp[Int]))
    val s = new RepOffsetOp[Int](1, new RepPrintOp[Int])
    testRepStream(i, s)
    s.onData(unit(6))
    s.onData(unit(7))
    testRepStream(i, new RepOffsetOp[Int](3, new RepPrintOp[Int]))
    println(unit("===="))
  }

  def test5b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].prepend(unit(-2) :: unit(-1) :: Nil).print)
    val s = RepStream[Int].offset(1).print
    testRepStream(i, s)
    s.onData(unit(6))
    s.onData(unit(7))
    testRepStream(i, RepStream[Int].offset(3).print)
    println(unit("===="))
  }

  def test6(i: Rep[Int]) = {
    testRepStream(i, new RepDuplicateOp[Int](new RepMapOp[Int, Int]({x => x + unit(10)}, new RepPrintOp[Int]), new RepMapOp[Int, Int]({x => x}, new RepPrintOp[Int])))
    testRepStream(i, new RepAggregatorOp[Int](new RepPrintOp[List[Int]]))
    println(unit("===="))
  }

  def test6b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].duplicate(RepStream[Int].map({x => x + unit(10)}).print, RepStream[Int].map({x => x}).print))
    testRepStream(i, RepStream[Int].aggregate().print)
    println(unit("===="))
  }
/*
  def test7(i: Rep[Int]) = {
    val (s1, s2) = RepStreamFunctions.zipWith[Int, Int](new RepPrintOp[(Int, Int)])
    testRepStream(i, s1)
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, s2))
    s1.onData(unit(0))
    s1.onData(unit(1))
    s1.onData(unit(2))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, s2))

    testRepStream(i, new RepSplitMergeOp[Int, Int, Int]({s1 => new RepMapOp[Int, Int]({x => x}, s1)},
        {s2 => new RepMapOp[Int, Int]({x => x + 10}, s2)},
        new RepMapOp[(Int, Int), (Int, Boolean)]({x => (x._1 + x._2, x._1 < x._2)}, new RepPrintOp[(Int, Boolean)])))

    val l0 = RepStreamFunctions.multiZipWith[Int](0, new RepPrintOp[List[Int]])
    println(unit(l0.length == 0))
    val l1 = RepStreamFunctions.multiZipWith[Int](1, new RepPrintOp[List[Int]])
    println(unit(l1.length == 1))
    testRepStream(i, l1(0))
    val l2 = RepStreamFunctions.multiZipWith[Int](2, new RepPrintOp[List[Int]])
    println(unit(l2.length == 2))
    testRepStream(i, l2(0))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, l2(1)))
    val l3 = RepStreamFunctions.multiZipWith[Int](3, new RepPrintOp[List[Int]])
    println(unit(l3.length == 3))
    testRepStream(i, l3(0))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, l3(1)))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(2)}, l3(2)))

  }
  
  def test7b(i: Rep[Int]) = {
    val (s1, s2) = RepStream[Int].zipWith(RepStream[Int], RepStream[(Int, Int)].print)
    testRepStream(i, s1)
    testRepStream(i, RepStream[Int].map({x => x + unit(1)}).into(s2))
    s1.onData(unit(0))
    s1.onData(unit(1))
    s1.onData(unit(2))
    testRepStream(i, RepStream[Int].map({x => x + unit(1)}).into(s2))

    testRepStream(i, RepStream[Int].splitMerge(RepStream[Int], RepStream[Int].map({x => x + 10}))
        .map({x => (x._1 + x._2, x._1 < x._2)}).print)

    val l0 = RepStreamFunctions.multiZipWith[Int](0, new RepPrintOp[List[Int]])
    println(unit(l0.length == 0))

    val l1 = RepStream[Int].multiZipWith(1, Nil, RepStream[List[Int]].print)
    println(unit(l1.length == 1))
    testRepStream(i, l1(0))
    val l2 = RepStream[Int].multiZipWith(2, scala.collection.immutable.List(RepStream[Int]), RepStream[List[Int]].print)
    println(unit(l2.length == 2))
    testRepStream(i, l2(0))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, l2(1)))
    val l3 = RepStream[Int].multiZipWith(3, scala.collection.immutable.List(RepStream[Int], RepStream[Int]), RepStream[List[Int]].print)
    println(unit(l3.length == 3))
    testRepStream(i, l3(0))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(1)}, l3(1)))
    testRepStream(i, new RepMapOp[Int, Int]({x => x + unit(2)}, l3(2)))

  }
*/    
  def test8(i: Rep[Int]) = {
    val s = new RepGroupByStreamOp[Int, Int]({x => 2 * x}, new RepPrintOp[HashMap[Int, List[Int]]]) 
    testRepStream(i, s)
    println(unit("===="))
  }
  
  def test8b(i: Rep[Int]) = {
    val s = RepStream[Int].groupByStream({x => 2 * x}).print 
    testRepStream(i, s)
    println(unit("===="))
  }

  def test9(i: Rep[Int]) = {
    testRepStream(i, new RepGroupByOp[Int, Int]({x => 2 * x}, {k => new RepMapOp[Int, Int]({x => x + unit(10) * k}, new RepPrintOp[Int])}))
    println(unit("===="))
  }

  def test9b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].groupBy({x => 2 * x}, {k: Rep[Int] => RepStream[Int].map({x => x + unit(10) * k}).print}))
    println(unit("===="))
  }
/*
  def test10(i: Rep[Int]) = {
    testRepStream(i, new RepMultiSplitOp[Int, Int](3, {(s, n) => new RepMapOp[Int, Int]({x => x * unit(n)}, s)},
        new RepPrintOp[List[Int]]))
    println(unit("===="))
  }

  def test10b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].multiSplit[Int](3, {(s, n) => RepStream[Int].map({x => x * unit(n)}).into(s)}).print)
    println(unit("===="))
  }

  def test11(i: Rep[Int]) = {
    val (s1, s2) = RepStreamFunctions.zipWith[Int, Int](new RepPrintOp[(Int, Int)])
    val m1 = new RepMapOp[Int, Int]({x => x + 1}, s1)
    val m2 = new RepMapOp[Int, Int]({x => x + 10}, s2)

    m1.onData(unit(0))
    m1.onData(unit(1))
    m1.onData(i)
    m2.onData(unit(10))
    m2.onData(unit(11))
  }
  
  def test11b(i: Rep[Int]) = {
    val (s1, s2) = RepStream[Int].zipWith(RepStream[Int], RepStream[(Int, Int)].print)
    val m1 = RepStream[Int].map({x => x + 1}).into(s1)
    val m2 = RepStream[Int].map({x => x + 10}).into(s2)

    m1.onData(unit(0))
    m1.onData(unit(1))
    m1.onData(i)
    m2.onData(unit(10))
    m2.onData(unit(11))
  }
  
  def test12(i: Rep[Int]) = {
    testRepStream(i, new RepSplitMergeOp[Int, Int, Int]({s1 => new RepMapOp[Int, Int]({x => x}, s1)},
        {s2 => new RepMapOp[Int, Int]({x => x + 10}, s2)},
        new RepMapOp[(Int, Int), (Int, Boolean)]({x => (x._1 + x._2, x._1 < x._2)}, new RepPrintOp[(Int, Boolean)])))
  }
  
  def test12b(i: Rep[Int]) = {
    testRepStream(i, RepStream[Int].splitMerge(RepStream[Int], RepStream[Int].map({x => x + 10}))
        .map({x => (x._1 + x._2, x._1 < x._2)}).print)
  }
  
//  def test13(i: Rep[Int]) = {
//    val (s1, s2) = RepStreamFunctions.equiJoin[Int, Int, Int]({x => x}, {x => x}, new RepPrintOp[List[(Int, Int)]])
//    s1.onData(unit(0))
//    s1.onData(unit(1))
//    s1.onData(i)
//    s2.onData(unit(0))
//    s2.onData(unit(1))
//  }
//  
//  def test13b(i: Rep[Int]) = test13(i)
  
*/
  def test14(i: Rep[Int]) = {
    val s = new RepGroupByOp[Int, Int]({x => x * unit(2)}, {k => 
      new RepFilterOp[Int]({x => (x < k)}, 
          new RepMapOp[Int, (Int, Int)]({x => (k, x)},
              new RepReduceOp[(Int, Int)]({(x, y) => (y._1, x._2 + y._2)},
                  new RepPrintOp[(Int, Int)])))})
    s.onData(i)
  }
  
  def test15(i: Rep[Int]) = {
    val s = new RepGroupByOp[Int, Int]({x => x * unit(2)}, {k => 
      new RepFilterOp[Int]({x => (x < k)}, 
          new RepMapOp[Int, (Int, Int)]({x => (k, x)},
              new RepReduceOp[(Int, Int)]({(x, y) => (y._1, x._2 + y._2)},
                  new RepPrintOp[(Int, Int)])))})
    s.onData(i)
    s.onData(i)
    s.flush
  }
/* 
    val op24 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "equiJoin")
    val (a3, b3) = StreamFunctions.equiJoin[Int, Int, Int](x => x, x => x, op24)
    new ListInput(list, a3)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b3)
    op24.verify()
    
    // API tests
    val op38 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "API 10")
    val (a7, b7) = Stream[Int] equiJoin(Stream[Int], {x: Int => x}, {x: Int => x}, op38)
    new ListInput(list, a7)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b7)
    op38.verify()
    
    val op42 = new AssertEqualsOp(list zip (list map (_ + 2)) zip (list map (_ + 4)) map {x => x._1._1 :: x._1._2 :: x._2 :: Nil}, "API 13")
    val list2 = Stream[Int] multiZipWith(3, (Stream[Int] map {_ + 1}) :: (Stream[Int] map {_ + 2}) :: Nil, op42)
    new ListInput(list, list2(0))
    new ListInput(list map (_ + 1), list2(1))
    new ListInput(list map (_ + 2), list2(2))
    op42.verify()
*/
}

trait OrderingOpsExpOpt extends OrderingOpsExp {
  override def ordering_lt[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[Boolean] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].lt(l, r))
    case _ => super.ordering_lt(lhs, rhs)
  }
  override def ordering_lteq[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[Boolean] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].lteq(l, r))
    case _ => super.ordering_lteq(lhs, rhs)
  }
  override def ordering_gt[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[Boolean] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].gt(l, r))
    case _ => super.ordering_gt(lhs, rhs)
  }
  override def ordering_gteq[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[Boolean] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].gteq(l, r))
    case _ => super.ordering_gteq(lhs, rhs)
  }
  override def ordering_equiv[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[Boolean] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].equiv(l, r))
    case _ => super.ordering_equiv(lhs, rhs)
  }
  override def ordering_max[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[T] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].max(l, r))
    case _ => super.ordering_max(lhs, rhs)
  }
  override def ordering_min[T:Ordering:Manifest](lhs: Exp[T], rhs: Exp[T])(implicit pos: SourceContext): Rep[T] = (lhs, rhs) match {
    case (Const(l), Const(r)) => unit(implicitly[Ordering[T]].min(l, r))
    case _ => super.ordering_min(lhs, rhs)
  }
}


class TestRepStreamOps extends FileDiffSuite {

  val prefix = "test-out/"

  def testRepStream1 = {
    withOutFile(prefix+"stream1"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with BooleanOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test1 _ , "test1", printWriter)
        val test = compile(test1)
        test(5)

      }
    }
    assertFileEqualsCheck(prefix+"stream1")
  }

  def testRepStream2 = {
    withOutFile(prefix+"stream2"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test2 _ , "test2", printWriter)
        val test = compile(test2)
        test(5)

      }
    }
    assertFileEqualsCheck(prefix+"stream2")
  }

  def testRepStream3 = {
    withOutFile(prefix+"stream3"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test3 _ , "test3", printWriter)
        val test = compile(test3)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream3")
  }

  def testRepStream3b = {
    withOutFile(prefix+"stream3"){  // API same as non-API
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test3b _ , "test3", printWriter)
        val test = compile(test3b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream3") // API same as non-API
  }
  
  def testRepStream4 = {
    withOutFile(prefix+"stream4"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test4 _ , "test4", printWriter)
        val test = compile(test4)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream4")
  }
  
  def testRepStream4b = {
    withOutFile(prefix+"stream4"){  // API same as non-API
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test4b _ , "test4", printWriter)
        val test = compile(test4b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream4")  // API same as non-API
  }
  
  def testRepStream5 = {
    withOutFile(prefix+"stream5"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test5 _ , "test5", printWriter)
        val test = compile(test5)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream5")
  }
  
  def testRepStream5b = {
    withOutFile(prefix+"stream5"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test5b _ , "test5", printWriter)
        val test = compile(test5b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream5")
  }
  
  def testRepStream6 = {
    withOutFile(prefix+"stream6"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test6 _ , "test6", printWriter)
        val test = compile(test6)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream6")
  }
  
  def testRepStream6b = {
    withOutFile(prefix+"stream6"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test6b _ , "test6", printWriter)
        val test = compile(test6b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream6")
  }
/*    
  def testRepStream7 = {
    withOutFile(prefix+"stream7"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test7 _ , "test7", printWriter)
        val test = compile(test7)
        test(5)

      }
    }
    assertFileEqualsCheck(prefix+"stream7")
  }
  
  def testRepStream7b = {
    withOutFile(prefix+"stream7"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test7b _ , "test7", printWriter)
        val test = compile(test7b)
        test(5)

      }
    }
    assertFileEqualsCheck(prefix+"stream7")
  }
    */
  def testRepStream8 = {
    withOutFile(prefix+"stream8"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test8 _ , "test8", printWriter)
        val test = compile(test8)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream8")
  }
  
  def testRepStream8b = {
    withOutFile(prefix+"stream8"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test8b _ , "test8", printWriter)
        val test = compile(test8b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream8")
  }

  def testRepStream9 = {
    withOutFile(prefix+"stream9"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test9 _ , "test9", printWriter)
        val test = compile(test9)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream9")
  }
  
  def testRepStream9b = {
    withOutFile(prefix+"stream9"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test9b _ , "test9", printWriter)
        val test = compile(test9b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream9")
  }
  /*
  def testRepStream10 = {
    withOutFile(prefix+"stream10"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test10 _ , "test10", printWriter)
        val test = compile(test10)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream10")
  }
  
  def testRepStream10b = {
    withOutFile(prefix+"stream10"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test10b _ , "test10", printWriter)
        val test = compile(test10b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream10")
  }
  
  def testRepStream11 = {
    withOutFile(prefix+"stream11"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test11 _ , "test11", printWriter)
        val test = compile(test11)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream11")
  }
  
  def testRepStream11b = {
    withOutFile(prefix+"stream11"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test11b _ , "test11", printWriter)
        val test = compile(test11b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream11")
  }  
  
  def testRepStream12 = {
    withOutFile(prefix+"stream12"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test12 _ , "test12", printWriter)
        val test = compile(test12)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream12")
  }
  
  def testRepStream12b = {
    withOutFile(prefix+"stream12"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test12b _ , "test12", printWriter)
        val test = compile(test12b)
        test(5)
        test(5)
      }
    }
    assertFileEqualsCheck(prefix+"stream12")
  }
  */
  
//  def testRepStream13 = {
//    withOutFile(prefix+"stream13"){
//      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
//        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>
//
//        val printWriter = new java.io.PrintWriter(System.out)
//
//        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
//          with ScalaGenOrderingOps { val IR: self.type = self }
//
//        codegen.emitSource(test13 _ , "test13", printWriter)
//        val test = compile(test13)
//        test(5)
//        test(5)
//      }
//    }
//    assertFileEqualsCheck(prefix+"stream13")
//  }
//  
//  def testRepStream13b = {
//    withOutFile(prefix+"stream13"){
//      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
//        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>
//
//        val printWriter = new java.io.PrintWriter(System.out)
//
//        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
//          with ScalaGenOrderingOps { val IR: self.type = self }
//
//        codegen.emitSource(test13b _ , "test13", printWriter)
//        val test = compile(test13b)
//        test(5)
//        test(5)
//      }
//    }
//    assertFileEqualsCheck(prefix+"stream13")
//  }
  
  def testRepStream14 = {
    withOutFile(prefix+"stream14"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test14 _ , "test14", printWriter)
        val test = compile(test14)
        test(0)
        test(1)
        test(-1)
        test(2)
        test(-2)
        test(2)
        test(3)
        test(4)
      }
    }
    assertFileEqualsCheck(prefix+"stream14")
  }

  def testRepStream15 = {
    withOutFile(prefix+"stream15"){
      new RepStreamProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with ScalaCompile{ self =>

        val printWriter = new java.io.PrintWriter(System.out)

        val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }

        codegen.emitSource(test15 _ , "test15", printWriter)
        val test = compile(test15)
        test(0)
        test(1)
        test(-1)
        test(2)
        test(-2)
        test(2)
        test(3)
        test(4)
      }
    }
    assertFileEqualsCheck(prefix+"stream15")
  }

}
