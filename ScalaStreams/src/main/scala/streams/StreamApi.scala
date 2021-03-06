package streams

import streams._

object Stream {
  def apply[A] = new Stream[A,A] {
    def into(out: StreamOp[A]): StreamOp[A] = out
  }
}

abstract class Stream[A,B] { self =>
  def into(out: StreamOp[B]): StreamOp[A]
  def into[C](next: Stream[B, C]): Stream[A, C] = new Stream[A, C] {
    def into(out: StreamOp[C]) = self.into(next.into(out))
  }
  
  def print = into(new PrintlnOp[B]())
  def printTo(out: java.io.StringWriter) = into(new PrintListToWriterOp[B](out))
  
  
  def aggregate() = new Stream[A,List[B]] {
    def into(out: StreamOp[List[B]]) = self.into(new AggregatorOp[B](out))
  }
  def drop(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new DropOp(n, out))
  }
  def dropWhile(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new DropWhileOp(p, out))
  }
  def filter(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new FilterOp(p, out))
  }
  def flatMap[C](f: B => List[C]) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new FlatMapOp(f, out))
  }
  def fold[C](f: (B, C) => C, z: C) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new FoldOp(f, z, out))
  }
  def groupByStream[K](keyF: B => K) = new Stream[A,Map[K, List[B]]] {
    def into(out: StreamOp[Map[K, List[B]]]) = self.into(new GroupByStreamOp(keyF, out))
  }
  def map[C](f: B => C) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new MapOp(f, out))
  }
  def multiSplit[C](num: Int, streams: (StreamOp[C], Int) => StreamOp[B]) = new Stream[A, List[C]] {
    def into(out: StreamOp[List[C]]) = self.into(new MultiSplitOp(num, streams, out))
  }
  def offset(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new OffsetOp(n, out))
  }
  def prepend(list: List[B]) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new PrependOp(list, out))
  }
  def reduce(f: (B, B) => B) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new ReduceOp(f, out))
  }
  def splitMerge[C,D](first: Stream[B,C], second: Stream[B,D]) = new Stream[A,Pair[C,D]] {
    def into(out: StreamOp[Pair[C,D]]) = self.into(
        new SplitMergeOp({x: StreamOp[C] => first into x}, {y: StreamOp[D] => second into y}, out))
  }
  def take(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new TakeOp(n, out))
  }
  def takeWhile(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new TakeWhileOp(p, out))
  }
  
  // special functions: Those are not simple Streams because they have
  // multiple in- or output streams
  def duplicate(first: StreamOp[B], second: StreamOp[B]) = {
    self.into(new DuplicateOp(first, second))
  }
  def equiJoin[C, D, K](other: Stream[C,D], keyFunThis: B => K, keyFunOther: D => K, next: StreamOp[List[(B,D)]]) = {
    val (a, b) = StreamFunctions.equiJoin(keyFunThis, keyFunOther, next)
    (self.into(a), other.into(b))
  }
  def groupBy[K](keyF: B => K, streamF: K => StreamOp[B]) = {
    self.into(new GroupByOp(keyF, streamF))
  }
  def multiZipWith(num: Int, others: List[Stream[A,B]], next: StreamOp[List[B]]) = {
    val list = StreamFunctions.multiZipWith(num, next)
    self.into(list(0)) :: others.zip(list.drop(1)).map({x => x._1.into(x._2)})
  }
  def zipWith[C,D](other: Stream[C,D], next: StreamOp[Pair[B, D]]) = {
    val (a, b) = StreamFunctions.zipWith(next)
    (self.into(a), other.into(b))
  }
}


//object APIStreamOp {
//  def apply[A] = new APIStreamOp[A,A] {
//    def into(out: StreamOp[A]): StreamOp[A] = out
//  }
//}
//
//abstract class APIStreamOp[A,B] { self =>
//  def into(out: StreamOp[B]): StreamOp[A]
//  def into[C](next: APIStreamOp[B, C]): APIStreamOp[A, C] = new APIStreamOp[A, C] {
//    def into(out: StreamOp[C]) = self.into(next.into(out))
//  }
//  
//  def print = into(new PrintlnOp[B]())
//  
//  def aggregate() = new APIStreamOp[A,List[B]] {...}
//  def drop(n: Int) = new APIStreamOp[A,B] {...}
//  def dropWhile(p: B => Boolean) = new APIStreamOp[A,B] {...}
//  def filter(p: B => Boolean) = new APIStreamOp[A,B] {...}
//  def flatMap[C](f: B => List[C]) = new APIStreamOp[A,C] {...}
//  def fold[C](f: (B, C) => C, z: C) = new APIStreamOp[A,C] {...}
//  def groupByAPIStreamOp[K](keyF: B => K) = new APIStreamOp[A,Map[K, List[B]]] {...}
//  def map[C](f: B => C) = new APIStreamOp[A,C] {...}
//  def multiSplit[C](num: Int, APIStreamOps: (StreamOp[C], Int) => StreamOp[B]) = new APIStreamOp[A, List[C]] {...}
//  def offset(n: Int) = new APIStreamOp[A,B] {...}
//  def prepend(list: List[B]) = new APIStreamOp[A,B] {...}
//  def reduce(f: (B, B) => B) = new APIStreamOp[A,B] {...}
//  def splitMerge[C,D](first: APIStreamOp[B,C], second: APIStreamOp[B,D]) = new APIStreamOp[A,Pair[C,D]] {...}
//  def take(n: Int) = new APIStreamOp[A,B] {...}
//  def takeWhile(p: B => Boolean) = new APIStreamOp[A,B] {...}
//  
//  // special functions: Those are not simple Streams because they have
//  // multiple in- or output streams
//  def duplicate(first: StreamOp[B], second: StreamOp[B]) = {...}
//  def equiJoin[C, D, K](other: APIStreamOp[C,D], keyFunThis: B => K, keyFunOther: D => K, next: StreamOp[List[(B,D)]]) = {...}
//  def groupBy[K](keyF: B => K, APIStreamOpF: K => StreamOp[B]) = {...}
//  def multiZipWith(num: Int, others: List[APIStreamOp[A,B]], next: StreamOp[List[B]]) = {...}
//  def zipWith[C,D](other: APIStreamOp[C,D], next: StreamOp[Pair[B, D]]) = {...}
//}