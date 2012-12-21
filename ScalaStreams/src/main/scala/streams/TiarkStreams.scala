//package streams
//
//
//// stream ops: basic stream processing components
//
//abstract class StreamFunc[A,B] { self =>
//  def isReady: Boolean
//  def +=(x: A): Unit
//  def eof(): Unit
//  def result: B
//
//  // transformer api
//  def map[B1](f: B => B1) = new StreamFunc[A,B1] {
//    def isReady = self.isReady
//    def +=(x: A) = self += x
//    def eof() = self.eof()
//    def result = f(self.result)
//  }
//  def zip[A1,B1](other: StreamFunc[A1,B1]) = new StreamFunc[(A,A1),(B,B1)] {
//    def isReady = self.isReady || other.isReady
//    def +=(x: (A,A1)) = { self += x._1; other += x._2 }
//    def eof() = { self.eof(); other.eof() }
//    def result = (self.result, other.result)
//  }
//}
//
//abstract class StreamOp[A] extends StreamFunc[A,Unit] {
//  def isReady: Boolean
//  def +=(x:A): Unit
//  def eof(): Unit
//  def result: Unit = {}
//}
//
//object StreamFunc {
//  // consumers
//  def print[A] = new StreamOp[A] {
//    def isReady = true
//    def +=(x: A) = println(x)
//    def eof() = println("<eof>")
//  }
//  def collect[A] = new StreamFunc[A,List[A]] {
//    var s: List[A] = Nil
//    def +=(x:A) = s = s :+ x // TODO
//    def isReady = true
//    def eof() = {}
//    def list = s
//    def result = s
//  }
//  def sum[A:Numeric]: StreamFunc[A,A] = new StreamFunc[A,A] {
//    var s = implicitly[Numeric[A]].zero
//    def isReady = true
//    def +=(x: A) = {
//      s = implicitly[Numeric[A]].plus(s,x)
//    }
//    def eof() = {}
//    def result = s
//  }
//  def count[A] = new StreamFunc[A,Int] {
//    var s = 0
//    def isReady = true
//    def +=(x: A) = s += 1
//    def eof() = {}
//    def result = s
//  }
//
//  def groupBy[K,A,B](sel: A => K, out: => StreamFunc[A,B]) = new Group[A,K,B,StreamFunc[A,B]](sel, () => out)
//
//  class Group[A,K,B,SF<:StreamFunc[A,B]](sel: A => K, out: () => SF) extends StreamFunc[A,scala.collection.mutable.HashMap[K,B]] {
//    def isReady = true // ok?
//    def eof() = {}     // ok?
//    val map = new scala.collection.mutable.HashMap[K, SF]
//    def +=(x: A): Unit = {
//      val out2 = map.getOrElseUpdate(sel(x), out())
//      out2 += x
//    }
//    def get(x: K): SF = map.getOrElse(x,out())
//    def result = map.map { case (k,v) => (k,v.result) }
//  }
//
//  // transformers
//  def tally[A:Numeric,B](out: StreamFunc[A,B]): StreamFunc[A,B] = new StreamFunc[A,B] {
//    var s = implicitly[Numeric[A]].zero
//    def isReady = out.isReady
//    def +=(x: A) = {
//      s = implicitly[Numeric[A]].plus(s,x)
//      out += s
//    }
//    def eof() = out.eof()
//    def result = out.result
//  }
//  def filter[A,B](p: A => Boolean)(out: StreamFunc[A,B]) = new StreamFunc[A,B] {
//    def isReady = out.isReady
//    def +=(x: A) = if (p(x)) out += x
//    def eof() = out.eof()
//    def result = out.result
//  }
//  def takeWhile[A,B](p: A => Boolean, out: StreamFunc[A,B]) = new StreamFunc[A,B] {
//    var running = out.isReady
//    def isReady = running
//    def +=(x: A) = if (running) {
//      if (p(x)) out += x
//      else { running = false; out.eof() }
//    }
//    def eof() = if (running) out.eof()
//    def result = out.result
//  }
//  def map[A,A1,B](f: A => A1)(out: StreamFunc[A1,B]) = new StreamFunc[A,B] {
//    def isReady = out.isReady
//    def +=(x: A) = out += f(x)
//    def eof() = out.eof()
//    def result = out.result
//  }
//  def flatMap[A,A1,B](f: A => Generator[A1])(out: StreamFunc[A1,B]) = new StreamFunc[A,B] {
//    val out2 = new StreamOp[A1] {
//      def isReady = out.isReady
//      def +=(x: A1) = out += x
//      def eof() = {}
//    }
//    def isReady = out.isReady
//    def +=(x: A) = f(x).into(out2)
//    def eof() = out.eof()
//    def result = out.result
//  }
//
//  // binary transformers
//  def select[A1,A2,B1,B2](out1: StreamFunc[A1,B1], out2: StreamFunc[A2,B2]) = 
//  new StreamFunc[Either[A1,A2],(B1,B2)] {
//    def isReady = out1.isReady || out2.isReady
//    def +=(x: Either[A1,A2]) = x match {
//      case Left(x) => out1 += x
//      case Right(x) => out2 += x
//    }
//    def eof() = { out1.eof(); out2.eof() }
//    def result = (out1.result, out2.result)
//  }
//
//
//  def zip[A1,A2,B](out: StreamFunc[(A1,A2),B]): (StreamFunc[A1,B], StreamFunc[A2,B]) = {
//    var bufferL: List[A1] = Nil
//    var bufferR: List[A2] = Nil
//
//    val left = new StreamFunc[A1,B] {
//      def +=(x: A1): Unit = {
//        if (bufferR.nonEmpty) {
//          val b = bufferR.head
//          bufferR = bufferR.tail
//          out += (x,b)
//        } else
//          bufferL = bufferL :+ x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = out.result
//    }
//
//    val right = new StreamFunc[A2,B] {
//      def +=(x: A2): Unit = {
//        if (bufferL.nonEmpty) {
//          val a = bufferL.head
//          bufferL = bufferL.tail
//          out += (a,x)
//        } else
//          bufferR = bufferR :+ x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = out.result
//    }
//    (left,right)
//  }
//
//  def cartesian[A,B,C](r: StreamFunc[(A,B),C]): (StreamFunc[A,C], StreamFunc[B,C]) = {
//    val bufferA = collect[A]
//    val bufferB = collect[B]
//
//    val left = new StreamFunc[A,C] {
//      def +=(x:A): Unit = {
//        for (b <- bufferB.list)
//          r += (x,b)
//        bufferA += x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = r.result
//    }
//
//    val right = new StreamFunc[B,C] {
//      def +=(x:B): Unit = {
//        for (a <- bufferA.list)
//          r += (a,x)
//        bufferB += x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = r.result
//    }
//    (left,right)
//  }
//
//  def join[K,A,B,C](selA: A=>K, selB: B=>K, r: StreamFunc[(A,B),C]): (StreamFunc[A,C], StreamFunc[B,C]) = {
//    val bufferA = groupBy(selA, collect[A])
//    val bufferB = groupBy(selB, collect[B])
//
//    val left = new StreamFunc[A,C] {
//      def +=(x:A): Unit = {
//        for (b <- bufferB.get(selA(x)).result)
//          r += (x,b)
//        bufferA += x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = r.result
//    }
//
//    val right = new StreamFunc[B,C] {
//      def +=(x:B): Unit = {
//        for (a <- bufferA.get(selB(x)).result)
//          r += (a,x)
//        bufferB += x
//      }
//      def isReady = true // TODO
//      def eof() = { } // TODO
//      def result = r.result
//    }
//    (left,right)
//  }
//
//
//}
//
//
//
//
//// streams: transform stream ops
//
//object Stream {
//  def apply[A] = new Stream[A,A] {
//    def into[B](out: StreamFunc[A,B]): StreamFunc[A,B] = out
//  }
//}
//
//abstract class Stream[A,B] { self =>
//  def into[C](out: StreamFunc[B,C]): StreamFunc[A,C]
//
//  // consumer api
//  def print = into(StreamFunc.print)
//  def collect = into(StreamFunc.collect)
//  def sum(implicit ev: Numeric[B]) = into(StreamFunc.sum)
//  def groupBy[K,C](sel: B => K)(inner: Stream[B,B] => StreamFunc[B,C]) = into(StreamFunc.groupBy(sel, inner(Stream[B])))
//
//  // transformer api
//  def tally(implicit ev: Numeric[B]) = new Stream[A,B] {
//    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.tally(out))
//  }
//  def filter(p: B => Boolean) = new Stream[A,B] {
//    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.filter(p)(out))
//  }
//  def takeWhile(p: B => Boolean) = new Stream[A,B] {
//    def into[C](out: StreamFunc[B,C]) = self.into(StreamFunc.takeWhile(p, out))
//  }
//  def map[B1](f: B => B1) = new Stream[A,B1] {
//    def into[C](out: StreamFunc[B1,C]) = self.into(StreamFunc.map(f)(out))
//  }
//  def flatMap[B1](f: B => Generator[B1]) = new Stream[A,B1] {
//    def into[C](out: StreamFunc[B1,C]) = self.into(StreamFunc.flatMap(f)(out))
//  }
//
//  // binary transformer api
//  def zip[A1,B1](other: Stream[A1,B1]) = new Stream[Either[A,A1],(B,B1)] {
//    def into[C](out: StreamFunc[(B,B1),C]) = {
//      val (left,right) = StreamFunc.zip(out)
//      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
//    }
//  }
//  def cartesian[A1,B1](other: Stream[A1,B1])= new Stream[Either[A,A1],(B,B1)] {
//    def into[C](out: StreamFunc[(B,B1),C]) = {
//      val (left,right) = StreamFunc.cartesian(out)
//      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
//    }
//  }
//  def join[K,A1,B1](other: Stream[A1,B1])(selA: B => K, selB: B1 => K) = new Stream[Either[A,A1],(B,B1)] {
//    def into[C](out: StreamFunc[(B,B1),C]) = {
//      val (left,right) = StreamFunc.join(selA, selB, out)
//      StreamFunc.select(self.into(left),other.into(right)).map(_._1)
//    }
//  }
//}
//
//
//// generators: produce values from external sources
//
//abstract class Generator[A] { self =>
//  def into[B](out: StreamFunc[A,B]): B
//
//  // consumer api
//  def sum(implicit ev: Numeric[A]) = into(StreamFunc.sum)
//
//  // transformer api 
//  def filter(p: A => Boolean) = new Generator[A] {
//    def into[B](out: StreamFunc[A,B]): B = self.into(Stream[A].filter(p).into(out))
//  }
//  def takeWhile(p: A => Boolean) = new Generator[A] {
//    def into[B](out: StreamFunc[A,B]): B = self.into(Stream[A].takeWhile(p).into(out))
//  }
//  def map[A1](f: A => A1) = new Generator[A1] {
//    def into[B](out: StreamFunc[A1,B]): B = self.into(Stream[A].map(f).into(out))
//  }
//  def flatMap[A1](f: A => Generator[A1]) = new Generator[A1] {
//    def into[B](out: StreamFunc[A1,B]): B = self.into(Stream[A].flatMap(f).into(out))
//  }
//}
//
//
