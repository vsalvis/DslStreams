package dsl


import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.internal.ScalaCompile
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileOutputStream
import scala.reflect.SourceContext


trait RepStreamProg extends RepStreamOps with NumericOps
  with OrderingOps with OverloadHack
  {

  def testRepStream(i: Rep[Int], m: RepStreamOp[Int]) = {
    m.onData(unit(0))
    m.onData(unit(1))
    m.onData(unit(2))
    m.onData(unit(3))
    m.onData(unit(4))
    m.onData(i)
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
    testRepStream(i, new RepReduceOp[Int]({(x, y) => x + y}, new RepPrintOp[Int]))
    testRepStream(i, new RepFoldOp[Int, Int]({(x, y) => x + y}, unit(1), new RepPrintOp[Int]))
    testRepStream(i, new RepFlatMapOp[Int, Int]({x => x :: x :: Nil}, new RepPrintOp[Int]))
  }
  
  def test4(i: Rep[Int]) = {
    testRepStream(i, new RepDropOp[Int](2, new RepPrintOp[Int]))
    testRepStream(i, new RepDropWhileOp[Int]({x: Rep[Int] => x > unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepDropWhileOp[Int]({x: Rep[Int] => x < unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeOp[Int](2, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeWhileOp[Int]({x: Rep[Int] => x > unit(3)}, new RepPrintOp[Int]))
    testRepStream(i, new RepTakeWhileOp[Int]({x: Rep[Int] => x < unit(3)}, new RepPrintOp[Int]))
  }

  def test5(i: Rep[Int]) = {
    testRepStream(i, new RepPrependOp[Int](unit(-2) :: unit(-1) :: Nil, new RepPrintOp[Int]))
    val s = new RepOffsetOp[Int](1, new RepPrintOp[Int])
    testRepStream(i, s)
    s.onData(unit(6))
    s.onData(unit(7))
    testRepStream(i, new RepOffsetOp[Int](3, new RepPrintOp[Int]))
  }

  def test6(i: Rep[Int]) = {
// TODO why can't I use IfThenElse here?
    //testRepStream(i, new RepGroupByOp[Int, Int]({x => x * unit(3)}, {x => if (x < unit(10)) { new RepMapOp[Int, Int]({x => -x}, new RepPrintOp[Int]) } else { new RepPrintOp[Int] }}))
    testRepStream(i, new RepGroupByOp[Int, Int]({x => 2 * x}, {k => new RepMapOp[Int, Int]({x => x + unit(10) * k}, new RepPrintOp[Int])}))
// TODO map in LMS?    
    //testRepStream(i, new RepGroupByStreamOp[Int, Int]({x => 2 * x}, new RepPrintOp[Map[Rep[Int], List[Rep[Int]]]]))
    testRepStream(i, new RepDuplicateOp[Int](new RepMapOp[Int, Int]({x => x + unit(10)}, new RepPrintOp[Int]), new RepMapOp[Int, Int]({x => x}, new RepPrintOp[Int])))
    testRepStream(i, new RepAggregatorOp[Int](new RepPrintOp[List[Int]]))
  }
  
  def test7(i: Rep[Int]) = {
    // TODO can remove unused Variables/stores?
    val (s1, s2) = RepStreamFunctions.zipWith[Int, Int](new RepPrintOp[(Int, Int)])
    testRepStream(i, s1)
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

/* 
    val op23e = new AssertEqualsOp((0::0::0::0::0::Nil) :: (0::1::2::3::4::Nil) :: (0::2::4::6::8::Nil) ::  Nil, "MultiSplitOp")
    new ListInput(0 :: 1 :: 2 :: Nil, new MultiSplitOp[Int, Int](5, (next, index) => new MapOp(x => x * index, next), op23e))
    op23e.verify()

    val op24 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "equiJoin")
    val (a3, b3) = StreamFunctions.equiJoin[Int, Int, Int](x => x, x => x, op24)
    new ListInput(list, a3)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b3)
    op24.verify()
*/    

/*  // Test Flush: only tests that flush is passed through
    object FlushTest {
      var ctr = 0
    }
    
    class FlushTest(create: StreamOp[Int] => StreamOp[Int], onDataCalledWith: Option[Int] = None) {
      FlushTest.ctr += 1
      var isFlushed = false
      val flusher = new StreamOp[Int] {
        def onData(data: Int) = onDataCalledWith match { 
          case None => println("onData of FlushTest has been called by " + FlushTest.ctr + " with data " + data)
          case Some(`data`) => 
          case Some(other) => println("onData of FlushTest has been called by " + FlushTest.ctr + " with data " + data + " instead of " + other)
        }
        
        def flush = { isFlushed = true }
      }
      val s = create(flusher)
      s.onData(1)
      s.flush
      if (!isFlushed) println("Not flushed: " + FlushTest.ctr) 
    }
    
    new FlushTest(s => new MapOp[Int, Int](x => x, s), Some(1)) // Nr. 1
    new FlushTest(s => new FilterOp[Int](x => x > 2, s))
    new FlushTest(s => new ReduceOp[Int]((x, y) => x + y, s), Some(1))
    new FlushTest(s => new FoldOp[Int, Int]((x, y) => 0, 0, s), Some(0))
    new FlushTest(s => new FlatMapOp[Int, Int](x => x :: Nil, s), Some(1))
    new FlushTest(s => new DropOp[Int](1, s))
    new FlushTest(s => new DropWhileOp[Int](x => x < 1, s), Some(1))
    new FlushTest(s => new TakeOp[Int](1, s), Some(1))
    new FlushTest(s => new TakeWhileOp[Int](x => x > 1, s))
    new FlushTest(s => new PrependOp[Int](1 :: Nil, s), Some(1)) // Nr. 10
    new FlushTest(s => new OffsetOp[Int](1, s))
    new FlushTest(s => new GroupByOp[Int, Int](x => x, x => s), Some(1))

    class GenericFlushTest[A, B](create: StreamOp[A] => StreamOp[B], onDataCalledWith: Option[A] = None) {
      FlushTest.ctr += 1
      var isFlushed = false
      val flusher = new StreamOp[A] {
        def onData(data: A) = onDataCalledWith match { 
          case None => println("onData of FlushTest has been called by " + FlushTest.ctr + " with data " + data)
          case Some(`data`) => 
          case Some(other) => println("onData of FlushTest has been called by " + FlushTest.ctr + " with data " + data + " instead of " + other)
        }
        
        def flush = { isFlushed = true }
      }
      val s = create(flusher)
      s.flush
      if (!isFlushed) println("Not flushed: " + FlushTest.ctr) 
    }

    new GenericFlushTest[Map[Int, List[Int]], Int](s => new GroupByStreamOp[Int, Int](x => x, s))
    new GenericFlushTest[List[(Int, Int)], Int](s => StreamFunctions.equiJoin[Int, Int, Int](x => x, x => x, s)._1)
    new GenericFlushTest[List[(Int, Int)], Int](s => StreamFunctions.equiJoin[Int, Int, Int](x => x, x => x, s)._2)
    
    // Test ordering of effects of flushing
    
    val op26 = new AssertEqualsOp(5 :: 8 :: 14 :: 24 :: 5 :: 8 :: 14 :: 24 :: Nil, "chained FoldOp")
    val op27 = new FoldOp[Int, Int](((x, y) => x + y), 0, new FoldOp[Int, Int](((x, y) => x + y), 4, op26))
    new ListInput(list, op27)
    op27.flush
    new ListInput(list, op27)
    op26.verify()
    
    val zipWithFlushTest = new StreamOp[(Int, Int)] {
      var state = 0
    val buffer = new scala.collection.mutable.Queue[Int]
    
    def onData(data: (Int, Int)) = {
        state match {
          case 0 => verifyEquals(state, data, (1, 2))
          case 1 => verifyEquals(state, data, (2, 3))
          case 2 => verifyEquals(state, data, (3, 4))
          case 3 => verifyEquals(state, data, (4, 5))
          case 4 => println("zipWithTest failed in state " + state + " because flush was expected, but data " + data + " was received")
          case 5 => verifyEquals(state, data, (5, 7))
          case 6 => println("zipWithTest failed in state " + state + " because flush was expected, but data " + data + " was received")
          case 7 => verifyEquals(state, data, (7, 9))
          case _ => println("zipWithTest failed in unknown state: " + state)
        }
        state += 1
    }
      
      def verifyEquals(state: Int, expected: (Int, Int), actual: (Int, Int)) = expected match {
        case `actual` =>
        case _ => println("zipWithTest failed in state " + state + ". Expected: " + expected + ", actual: " + actual)
      }
    
      def verifyEnd = if(state != 8) println("zipWithTest finished in state " + state + " instead of " + 10)
    
    override def flush() = {
        state match {
          case 4 =>
          case 6 =>
          case _ => println("zipWithTest failed in state " + state + " because flush was called, but data was expected")
        }
        state += 1
    }
    }

    val (a6, b6) = StreamFunctions.zipWith[Int, Int](zipWithFlushTest)
    new ListInput(list, a6)
    new ListInput(list map (_ + 1), b6)
    a6.flush
    b6.onData(6)
    b6.flush
    b6.onData(7)
    b6.onData(8)
    a6.onData(5)
    b6.flush
    a6.onData(6)
    a6.flush
    a6.onData(7)
    b6.onData(9)
    zipWithFlushTest.verifyEnd

    println("StreamOp Tests SUCCESSFUL")
 */
/*
    val op29 = new AssertEqualsOp[Int](3 :: 4 :: Nil, "API 1")
    new ListInput(1 :: 2 :: 3 :: Nil, Stream[Int] map {_ + 1} filter {_ > 2} into op29)
    op29.verify()

    val op30 = new AssertEqualsOp[List[Int]](List(2) :: List(3,2) :: Nil, "API 2")
    new ListInput(1 :: 2 :: 3 :: Nil, Stream[Int] drop 1 aggregate() into op30)
    op30.verify()

    val op31 = new AssertEqualsOp[Int](2 :: 0 :: 3 :: 0 :: Nil, "API 3")
    new ListInput(1 :: 2 :: 3 :: Nil, Stream[Int] dropWhile {_ < 2} flatMap {_ :: 0 :: Nil} into op31)
    op31.verify()

    val op32 = new AssertEqualsOp[Int](5 :: 7 :: 10 :: Nil, "API 4")
    new ListInput(1 :: 2 :: 3 :: Nil, Stream[Int] fold({(x: Int, y: Int) => x + y}, 4) into op32)
    op32.verify()

    val op33 = new AssertEqualsOp[Map[Int, List[Int]]](Map(1 -> List(1)) :: Map(1 -> List(1), 2 -> List(2)) :: Map(1 -> List(1), 2 -> List(2), 0 -> List(3)) :: Map(1 -> List(4, 1), 2 -> List(2), 0 -> List(3)) :: Map(1 -> List(4, 1), 2 -> List(5, 2), 0 -> List(3)) :: Nil, "API 5")
    new ListInput(1 :: 2 :: 3 :: 4 :: 5 :: Nil, Stream[Int] groupByStream {_ % 3} into op33)
    op33.verify()

    val op34 = new AssertEqualsOp[Int](-1 :: 0 :: 2 :: 5 :: Nil, "API 6")
    new ListInput(1 :: 2 :: 3 :: 4 :: 5 :: Nil, Stream[Int] offset 2 prepend (-1 :: 0 :: Nil) reduce {(x: Int, y: Int) => x + y} into op34)
    op34.verify()

    val op35 = new AssertEqualsOp[Int](1 :: 2 :: 3 :: Nil, "API 7")
    new ListInput(1 :: 2 :: 3 :: 4 :: 5 :: Nil, Stream[Int] take 4 takeWhile {_ < 4} into op35)
    op35.verify()

    val op36a = new AssertEqualsOp[Int](1 :: 2 :: 3 :: Nil, "API 8a")
    val op36b = new AssertEqualsOp[Int](1 :: 2 :: 3 :: Nil, "API 8b")
    new ListInput(1 :: 2 :: 3 :: 4 :: 5 :: Nil, Stream[Int] take 4 takeWhile {_ < 4} duplicate(Stream[Int] into op36a, Stream[Int] into op36b))
    op36a.verify()
    op36b.verify()

    val op37 = new AssertEqualsOp[Int](2 :: 4 :: 6 :: 8 :: 10 :: Nil, "API 9")
    new ListInput(1 :: 2 :: 3 :: 4 :: 5 :: Nil, Stream[Int] splitMerge (Stream[Int], Stream[Int]) map (x => x._1 + x._2) into op37)
    op37.verify()
    
    val op38 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "API 10")
    val (a7, b7) = Stream[Int] equiJoin(Stream[Int], {x: Int => x}, {x: Int => x}, op38)
    new ListInput(list, a7)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b7)
    op38.verify()
    
    val op39 = new AssertEqualsOp[Int](1 :: 1 :: Nil, "API 11a")
    val op40 = new AssertEqualsOp[Int](0 :: Nil, "API 11b")
    n = 0;
    new ListInput(1 :: 0 :: 1 :: Nil, Stream[Int] groupBy(x => x, {x: Int => if (n == 0) { n = 1; op39 } else op40}))
    op39.verify()
    op40.verify()
    
    val op41 = new AssertEqualsOp(list zip (list map (_ + 1)), "API 12")
    val (a8, b8) = Stream[Int] zipWith(Stream[Int], op41)
    new ListInput(list, a8)
    new ListInput(list map (_ + 1), b8)
    op41.verify()

    val op42 = new AssertEqualsOp(list zip (list map (_ + 2)) zip (list map (_ + 4)) map {x => x._1._1 :: x._1._2 :: x._2 :: Nil}, "API 13")
    val list2 = Stream[Int] multiZipWith(3, (Stream[Int] map {_ + 1}) :: (Stream[Int] map {_ + 2}) :: Nil, op42)
    new ListInput(list, list2(0))
    new ListInput(list map (_ + 1), list2(1))
    new ListInput(list map (_ + 2), list2(2))
    op42.verify()

    val op43 = new AssertEqualsOp((0::0::0::0::0::Nil) :: (0::1::2::3::4::Nil) :: (0::2::4::6::8::Nil) ::  Nil, "API 14")
    new ListInput(0 :: 1 :: 2 :: Nil, Stream[Int] multiSplit (5, (next: StreamOp[Int], index) => new MapOp(x => x * index, next)) into op43)
    op43.verify()

    println("API Tests SUCCESSFUL")
    
    // Examples for report
    
    new ListInput(List.range(0, 6), 
        new MapOp({x: Int => 3 * x}, 
            new DuplicateOp(
              new FilterOp({x: Int => x % 2 == 0}, 
                  new MapOp({x: Int => 2 * x + " (even)"},
                      new PrintlnOp)),
              new FilterOp({x: Int => x % 2 == 1}, 
                  new MapOp({x: Int => 3 * x + " (odd)"},
                      new PrintlnOp)))))

    // Creating a StreamOp:
    val stream01 = new MapOp({x: Int => 2 * x}, new PrintlnOp)
    // Adding on the left:
    val stream02 = new FilterOp({x: Int => x % 2 == 0}, stream01)
    // Cannot add on right, stream finishes with PrintlnOp
    
    // Creating a Stream of Ints:
    val stream1: Stream[Int, Int] = Stream[Int] map {3 * _}
    // Adding operations on the right:
    val stream2: Stream[Int, Int] = stream1 map {_ - 1}
    // Creating another Stream of Ints:
    val stream3: Stream[Int, Int] = Stream[Int] filter {_ % 2 == 0}
    // Adding it on the left:
    val stream4: Stream[Int, Int] = stream3 into stream2
    // Instantiating the StreamOps:
    val streamOp: StreamOp[Int] = stream4.print
    // Use the StreamOps:
    new ListInput(List.range(0, 6), streamOp)
    
    new ListInput(List.range(0, 6), 
        Stream[Int] map {3 * _} duplicate (
            Stream[Int] filter {_ % 2 == 0} map {2 * _ + " (even)"} print,
            Stream[Int] filter {_ % 2 == 1} map {3 * _ + " (odd)"} print))
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

      }
    }
    assertFileEqualsCheck(prefix+"stream3")
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

      }
    }
    assertFileEqualsCheck(prefix+"stream4")
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

      }
    }
    assertFileEqualsCheck(prefix+"stream6")
  }
  
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
}