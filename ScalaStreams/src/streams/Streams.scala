package streams

import scala.actors.Actor._

abstract class StreamOp[A] {
  def onData(data: A)
  
  def flush
}

case class Flush

class StreamSynchronizer[A] {
  
  def getSynchronizedStream(next: StreamOp[A]): StreamOp[A] = new StreamOp[A] {
    def onData(data: A) = synchronizationActor ! (next, data)
	def flush = synchronizationActor ! (next, Flush)  // should this be "?!" ?
  }

  private val synchronizationActor = actor {
    loop {
      react {
        case (next: StreamOp[A], Flush) => next.flush 
        case (next: StreamOp[A], data: A) => next.onData(data)
      }
    }
  }
}

class IdentityOp[A](next: StreamOp[A]) extends StreamOp[A] {
  def onData(data: A) = next.onData(data)
  def flush = next.flush
}

class MapOp[A, B](f: A => B, next: StreamOp[B]) extends StreamOp[A] {
  def onData(data: A) = next.onData(f(data))
  
  def flush = next.flush
}

class FilterOp[A](p: A => Boolean, next: StreamOp[A]) extends StreamOp[A] {
  def onData(data: A) = if (p(data)) next.onData(data)
  
  def flush = next.flush
}

class ReduceOp[A](f: (A, A) => A, next: StreamOp[A]) extends StreamOp[A] {
  var result: A = null.asInstanceOf[A]
  
  def onData(data: A) = {
    if (result == null) {
      result = data
    } else {
      result = f(result, data)
      next.onData(result)
    }
  }
  
  def flush = {
    result = null.asInstanceOf[A]
    next.flush
  }
}

class FoldOp[A, B](f: (A, B) => B, z: B, next: StreamOp[B]) extends StreamOp[A] {
  var result = z
  
  def onData(data: A) = {
    result = f(data, result); next.onData(result)
  }
  
  def flush = {
    result = z
    next.flush
  }
}

class FlatMapOp[A, B](f: A => List[B], next: StreamOp[B]) extends StreamOp[A] {
  def onData(data: A) = f(data) foreach next.onData

  def flush = next.flush
}

class DropOp[A](n: Int, next: StreamOp[A]) extends StreamOp[A] {
  var num = n
  def onData(data: A) = {
    if (num > 0) {
      num -= 1
    } else {
      next.onData(data)
    }
  }
  
  def flush = {
    num = n
    next.flush
  }
}

class DropWhileOp[A](p: A => Boolean, next: StreamOp[A]) extends StreamOp[A] {
  var dropping = true
  def onData(data: A) = {
    if (dropping && !p(data)) {
      dropping = false
    }
    if (!dropping) {
      next.onData(data)
    }
  }
  
  def flush = {
    dropping = true
    next.flush
  }
}

class TakeOp[A](n: Int, next: StreamOp[A]) extends StreamOp[A] {
  var num = n
  def onData(data: A) = {
    if (num > 0) {
      num -= 1
      next.onData(data)
    }
  }
  
  def flush = {
    num = n
    next.flush
  }
}

class TakeWhileOp[A](p: A => Boolean, next: StreamOp[A]) extends StreamOp[A] {
  var taking = true
  def onData(data: A) = {
    if (taking && !p(data)) {
      taking = false
    }
    if (taking) {
      next.onData(data)
    }
  }
  
  def flush = {
    taking = true
    next.flush
  }
}

class PrependOp[A](list: List[A], next: StreamOp[A]) extends StreamOp[A] {
  list foreach next.onData
  
  def onData(data: A) = next.onData(data)
  
  def flush = {
    next.flush
    list foreach next.onData
  }
}

class OffsetOp[A](n: Int, next: StreamOp[A]) extends StreamOp[A] {
  val buffer = new scala.collection.mutable.Queue[A]
  
  def onData(data: A) = {
    if (buffer.size == n) {
      next.onData(buffer.dequeue)
    }
    buffer += data
  }

  def flush = {
    buffer.dequeueAll(_ => true)
    next.flush
  }
}

class GroupByOp[A, B](keyFun: A => B, streamOpFun: B => StreamOp[A]) extends StreamOp[A] {
  val map = new scala.collection.mutable.HashMap[B, StreamOp[A]]()
  
  def onData(data: A) = {
    val key = keyFun(data)
    map.get(key) match {
      case None => {
        val streamOp = streamOpFun(key)
        map += ((key, streamOp))
        streamOp.onData(data)
      }
      case Some(op) => op.onData(data)
    }
  }

  def flush = {
    map.values foreach { _.flush }
  }
}

class GroupByStreamOp[A, B](keyFun: A => B, next: StreamOp[Map[B, List[A]]]) extends StreamOp[A] {
  val map = new scala.collection.mutable.HashMap[B, List[A]]()
  
  def onData(data: A) = {
    val key = keyFun(data)
    map.get(key) match {
      case None => {
        map += ((key, data :: Nil))
      }
      case Some(list) => map += ((key, data :: list))
    }
    next.onData(map.toMap)
  }
  
  def flush = {
    map.clear
    next.flush
  }
}

class DuplicateOp[A](next1: StreamOp[A], next2: StreamOp[A]) extends StreamOp[A] {
  def onData(data: A) = {
    next1.onData(data)
    next2.onData(data)
  }
  
  def flush = {
    next1.flush
    next2.flush
  }
}

class AggregatorOp[A](next: StreamOp[List[A]]) extends StreamOp[A] {
  var list: List[A] = Nil 
  def onData(data: A) = {
    list = data :: list
    next.onData(list)
  }
  def flush = next.flush
}

class SplitOp[A, B, C, D, E, F](split: A => (B, C), first: StreamOp[D] => StreamOp[B],
    second: StreamOp[E] => StreamOp[C], merge: (D, E) => F, next: StreamOp[F]) extends StreamOp[A] {
  val (firstZip, secondZip) = new StreamFunctions().zipWith(merge, next)
  val (firstStream, secondStream) = (first(firstZip), second(secondZip))
    
  def onData(data: A) = {
    val (b, c) = split(data)
    firstStream.onData(b)
    secondStream.onData(c)
  }
  
  def flush = {
    firstStream.flush
    secondStream.flush
  }
}

class MultiSplitOp[A, B](num: Int, split: (A, Int) => B, streams: (StreamOp[B], Int) => StreamOp[B], next: StreamOp[List[B]]) extends StreamOp[A] {
  val zippedStreams = new StreamFunctions().multiZipWith(num, next).zipWithIndex.map(x => streams(x._1, x._2))

  def onData(data: A) = {
    zippedStreams.zipWithIndex.foreach(x => x._1 onData split(data, x._2))
  }
  def flush = { zippedStreams.foreach(_.flush) }
}

class StreamFunctions {
  
  // Changed output to List[(A, B)], free GroupBy, can always flatten after
  def equiJoin[A, B, K] (keyFunA: A => K, keyFunB: B => K, next: StreamOp[List[(A, B)]]): (StreamOp[A], StreamOp[B]) = {
    val aMap = new scala.collection.mutable.HashMap[K, List[A]]
    val bMap = new scala.collection.mutable.HashMap[K, List[B]]

    class JoinOp[C](map: scala.collection.mutable.HashMap[K, List[C]], keyFun: C => K) extends StreamOp[C] {
      def onData(data: C) = {
        val key = keyFun(data)
        map += ((key, data :: (map.get(key) match {
          case None => Nil
          case Some(list) => list
        })))
        (aMap.get(key), bMap.get(key)) match {
          case (Some(as), Some(bs)) => next.onData(for (a <- as; b <- bs) yield (a, b))
          case _ =>
        }
      }
      
      def flush = {
        map.clear
        next.flush // Will flush next twice if both input streams are flushed
      }
    }
    
    (new JoinOp(aMap, keyFunA), new JoinOp(bMap, keyFunB))
  }
  
  def multiZipWith[A] (num: Int, next: StreamOp[List[A]]): List[StreamOp[A]] = num match {
    case x if x <= 0 => Nil
    case 1 => new MapOp((x: A) => x :: Nil, next) :: Nil
    case 2 => {
      val (a, b) = zipWith[A, A, List[A]](_ :: _ :: Nil, next)
      a :: b :: Nil
    }
    case x => {
      val (a, b) = zipWith[List[A], List[A], List[A]](_ ::: _, next)
      multiZipWith(x / 2 + x % 2, a) ::: multiZipWith(x / 2, b)
    }
  }
  
  def zipWith[A, B, C] (f: (A, B) => C, next: StreamOp[C]): (StreamOp[A], StreamOp[B]) = {
    val leftBuffer = new scala.collection.mutable.Queue[A]
    val rightBuffer = new scala.collection.mutable.Queue[B]
    var leftWaitingForFlush = false
    var rightWaitingForFlush = false
  
	val left = new StreamOp[A] {
	  def onData(data: A) = {
	    if (!leftWaitingForFlush) {
	      if (rightBuffer.isEmpty) {
	        leftBuffer += data
	      } else {
	        next.onData(f(data, rightBuffer.dequeue))
	      }
	    }
      }
	  
	  def flush = {
	    leftBuffer.clear
	    if (leftWaitingForFlush) {
	      leftWaitingForFlush = false
   	      next.flush
	    } else {
	      rightWaitingForFlush = true
	    }
	  }
	}
  
	val right = new StreamOp[B] {
	  def onData(data: B) = {
	    if (!rightWaitingForFlush) {
  	      if (leftBuffer.isEmpty) {
	        rightBuffer += data
	      } else {
	        next.onData(f(leftBuffer.dequeue, data))
	      }
	    }
      }

	  def flush = {
	    rightBuffer.clear
	    if (rightWaitingForFlush) {
	      rightWaitingForFlush = false
  	      next.flush
	    } else {
	      leftWaitingForFlush = true
	    }
	  }
    } 
	  
    (left, right)
  }
}

abstract class StreamInput[A](stream: StreamOp[A])

class ListInput[A](input: List[A], stream: StreamOp[A]) extends StreamInput[A](stream) {
  input foreach stream.onData
}

class ElementInput[A](input: A, stream: StreamOp[A]) extends StreamInput[A](stream) {
  stream.onData(input)
}


abstract class StreamOutput[A] extends StreamOp[A] {
  def flush = {}
}

class PrintlnOp[A] extends StreamOutput[A] {
  def onData(data: A) = println(data)
}

class PrintListOp[A] extends StreamOutput[A] {
  var started = false
  def onData(data: A) = {
    if (started) print(" :: ") else started = true
    print(data)
  }
}

class NamedPrintOp[A] extends StreamOutput[A] {
  var first: Option[A] = None
  
  def onData(data: A) = {
    first match {
      case None => {
        first = Some(data)
        println("started " + data)
      }
      case Some(d) => println("cont " + d + " with " + data)
    }
  }
}

class AssertEqualsOp[A](expected: List[A], opDescription: String, verifyOnFlush: Boolean = false) extends StreamOutput[A] {
  val buffer = new scala.collection.mutable.Queue[A]
  
  def onData(data: A) = {
    buffer += data
  }
  
  def verify(reportSuccess: Boolean = false) = {
    if (buffer.size < expected.size) {
      report("Not enough elements.")
    } else if (buffer.size > expected.size) {
      report("Too many elements.")
    } else if (!(buffer.toList equals expected)) {
      report("Wrong elements.")
    } else if (reportSuccess) {
      println("Success: " + opDescription)
    }
  }
  
  override def flush() = {
    if (verifyOnFlush) {
      verify(true)
    }
  }
  
  def report(error: String) = println(opDescription + ": " + error + " Expected: " + expected + " Actual: " + buffer.toList)
}


object Streams {
  def testTests = {
    val list = 1 :: 2 :: 3 :: 4 :: Nil
    
    val op0 = new AssertEqualsOp(1 :: Nil, "Success")
    new ElementInput(1, op0)
    op0.verify()
    
    val op1 = new AssertEqualsOp[Int](Nil, "assert too many")
    new ElementInput(1, op1)
    op1.verify()
    
    val op2 = new AssertEqualsOp(list, "assert not enough")
    new ElementInput(1, op2)
    op2.verify()
    
    new ListInput(list, new PrintlnOp)
    new ListInput(list, new PrintListOp)
    
    println
  }
  
  def main(args: Array[String]) {
    val list = 1 :: 2 :: 3 :: 4 :: Nil
    
//    testTests
   
    // Test Inputs
    
    val op0 = new AssertEqualsOp(1 :: Nil, "ElementInput")
    new ElementInput(1, op0)
    op0.verify()
    
    val op1 = new AssertEqualsOp(list, "ListInput")
    new ListInput(list, op1)
    op1.verify()
    
    // Test all Ops
    
    val op2 = new AssertEqualsOp(list map (_ + 1), "MapOp")
    new ListInput(list, new MapOp[Int, Int](_ + 1, op2))
    op2.verify()

    val op3 = new AssertEqualsOp(list filter (_ > 2), "FilterOp")
    new ListInput(list, new FilterOp[Int](_ > 2, op3))
    op3.verify()
      
    val op4 = new AssertEqualsOp(list filter (_ > 2) map (_ + 1), "FilterOp MapOp")
    new ListInput(list, new FilterOp[Int](_ > 2, new MapOp[Int, Int](_ + 1, op4)))
    op4.verify()
    
    val op5 = new AssertEqualsOp(list zip (list map (_ + 1)), "zipWith")
    val (a1, b1) = new StreamFunctions().zipWith[Int, Int, (Int, Int)]((_, _), op5)
    new ListInput(list, a1)
    new ListInput(list map (_ + 1), b1)
    op5.verify()

    val op6 = new AssertEqualsOp((list, (list map (_ + 1))).zipped map (_ + _), "zipWith map")
    val (a2, b2) = new StreamFunctions().zipWith[Int, Int, Int]((_ + _), op6)
    new ListInput(list, a2)
    new ListInput(list map (_ + 1), b2)
    op6.verify()
    
    val op7 = new AssertEqualsOp(list flatMap (x => x :: x :: Nil), "FlatMapOp")
    new ListInput(list, new FlatMapOp[Int, Int]((x => x :: x :: Nil), op7))
    op7.verify()
    
    val op8 = new AssertEqualsOp(3 :: 6 :: 10 :: Nil, "ReduceOp")
    new ListInput(list, new ReduceOp[Int](((x, y) => x + y), op8))
    op8.verify()
    
    val op28 = new AssertEqualsOp(-3 :: 0 :: -4 :: Nil, "ReduceOp Minus")
    new ListInput(list, new ReduceOp[Int](((x, y) => -x - y), op28))
    op28.verify()

    val op9 = new AssertEqualsOp(1 :: 3 :: 6 :: 10 :: Nil, "FoldOp")
    new ListInput(list, new FoldOp[Int, Int](((x, y) => x + y), 0, op9))
    op9.verify()

    val op10 = new AssertEqualsOp(list drop 1, "DropOp 1")
    new ListInput(list, new DropOp[Int](1, op10))
    op10.verify()

    val op11 = new AssertEqualsOp(list drop 0, "DropOp 0")
    new ListInput(list, new DropOp[Int](0, op11))
    op11.verify()
    
    val op12 = new AssertEqualsOp(list dropWhile (_ < 2), "DropWhileOp < 2")
    new ListInput(list, new DropWhileOp[Int](_ < 2, op12))
    op12.verify()
    
    val op13 = new AssertEqualsOp(list dropWhile (_ > 2), "DropWhileOp > 2")
    new ListInput(list, new DropWhileOp[Int](_ > 2, op13))
    op13.verify()

    val op14 = new AssertEqualsOp(list take 0, "TakeOp 0")
    new ListInput(list, new TakeOp[Int](0, op14))
    op14.verify()
    
    val op15 = new AssertEqualsOp(list takeWhile (_ < 2), "TakeWhileOp < 2")
    new ListInput(list, new TakeWhileOp[Int](_ < 2, op15))
    op15.verify()
    
    val op16 = new AssertEqualsOp(list takeWhile (_ > 2), "TakeWhileOp > 2")
    new ListInput(list, new TakeWhileOp[Int](_ > 2, op16))
    op16.verify()

    val op17 = new AssertEqualsOp(-1 :: 0 :: list, "PrependOp")
    new ListInput(list, new PrependOp[Int](-1 :: 0 :: Nil, op17))
    op17.verify()
    
    val op20 = new AssertEqualsOp[Int](list take 3, "OffsetOp")
    new ListInput(list, new OffsetOp[Int](1, op20))
    op20.verify()

    val op21 = new AssertEqualsOp[Int](1 :: 1 :: Nil, "GroupByOp 1")
    val op22 = new AssertEqualsOp[Int](0 :: Nil, "GroupByOp 2")
    var n = 0;
    new ListInput(1 :: 0 :: 1 :: Nil, new GroupByOp[Int, Int](x => x, x => if (n == 0) { n = 1; op21 } else op22))
    op21.verify()
    op22.verify()

    val op23 = new AssertEqualsOp[Map[Int, List[Int]]](Map(1 -> (1 :: Nil)) :: Map(1 -> (1 :: Nil), 0 -> (0 :: Nil))
        :: Map(1 -> (1 :: 1 :: Nil), 0 -> (0 :: Nil)) :: Nil, "GroupByStreamOp")
    new ListInput(1 :: 0 :: 1 :: Nil, new GroupByStreamOp[Int, Int](x => x, op23))
    op23.verify()

    val op23b = new AssertEqualsOp(list, "DuplicateOp 1")
    val op23c = new AssertEqualsOp(list, "DuplicateOp 2")
    new ListInput(list, new DuplicateOp[Int](op23b, op23c))
    op23b.verify()
    op23c.verify()

    val op23d = new AssertEqualsOp(list map { -_}, "SplitOp")
    new ListInput(list, new SplitOp[Int, Int, Int, Int, Int, Int]((x) => (x, x), (x) => new MapOp(x => x, x), (x) => new MapOp(-2 * _, x), (x, y) => x + y, op23d))
    op23d.verify()
 
    val op23e = new AssertEqualsOp((0::0::0::0::0::Nil) :: (0::1::2::3::4::Nil) :: (0::2::4::6::8::Nil) ::  Nil, "MultiSplitOp")
    new ListInput(0 :: 1 :: 2 :: Nil, new MultiSplitOp[Int, Int](5, (data, index) => data, (next, index) => new MapOp(x => x * index, next), op23e))
    op23e.verify()

    val op24 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "equiJoin")
    val (a3, b3) = new StreamFunctions().equiJoin[Int, Int, Int](x => x, x => x, op24)
    new ListInput(list, a3)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b3)
    op24.verify()
    
    // With synchronization
    
    print("synchronized equiJoin? ")
    val op25 = new AssertEqualsOp[List[(Int, Int)]](((2, 2) :: Nil) :: ((1,1) :: Nil) :: ((3,3) :: Nil) :: ((4,4) :: Nil) :: ((1,1) :: (1,1) :: Nil) :: Nil, "synchronized equiJoin", true)
    val (a4, b4) = new StreamFunctions().equiJoin[Int, Int, Int](x => x, x => x, op25)
    val synch = new StreamSynchronizer[Int]
    val (a5, b5) = (synch.getSynchronizedStream(a4), synch.getSynchronizedStream(b4))
    new ListInput(list, a5)
    new ListInput(0 :: 2 :: 1 :: 3 :: 4 :: 1 :: Nil, b5)
    a5.flush
    // op25 is verified on flush, "Success" has to be printed
    
    // Test Flush: only tests that flush is passed through
    
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
    new GenericFlushTest[List[(Int, Int)], Int](s => new StreamFunctions().equiJoin[Int, Int, Int](x => x, x => x, s)._1)
    new GenericFlushTest[List[(Int, Int)], Int](s => new StreamFunctions().equiJoin[Int, Int, Int](x => x, x => x, s)._2)
    
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

    val (a6, b6) = new StreamFunctions().zipWith[Int, Int, (Int, Int)]((_, _), zipWithFlushTest)
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
    
    println("END OF TESTS")
  }
  
}

// multiplex/split (backpressure) /duplicate
// scala pipes, db toaster updated
// optimization: ordered/unordered stream, flush? window? aggregate
// streamit compiler?
