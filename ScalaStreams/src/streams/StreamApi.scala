package streams

import streams._

object Stream {
  def apply[A] = new Stream[A,A] {
    def into(out: StreamOp[A]): StreamOp[A] = out
  }
}

abstract class Stream[A,B] { self =>
  def into(out: StreamOp[B]): StreamOp[A] //B, C => A, C
  
  // consumer api
  def print = into(new PrintListOp())
  def aggregate() = new Stream[A,List[B]] {
    def into(out: StreamOp[List[B]]) = self.into(new AggregatorOp[B](out))
  }
  def drop(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new DropOp(n, out))
  }
  def dropWhile(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new DropWhileOp(p, out))
  }
//  def duplicateOp(next1, next2) ???
  def filter(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new FilterOp(p, out))
  }
  def flatMap[C](f: B => List[C]) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new FlatMapOp(f, out))
  }
  def fold[C](f: (B, C) => C, z: C) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new FoldOp(f, z, out))
  }
//  def groupBy[K](keyF: B => K, streamF: K => StreamOp[B]) = new Stream[A,B] {
//    def into(out: StreamOp[B]) = self.into(new GroupByOp(keyF, streamF))
//  }
  
  //---------------
  def groupByStream[K](keyF: B => K) = new Stream[A,Map[K, List[B]]] {
    def into(out: StreamOp[Map[K, List[B]]]) = self.into(new GroupByStreamOp(keyF, out))
  }
//  def identity() = new Stream[A,B] {
//    def into(out: StreamOp[B]) = self.into(out)
//  }
  def map[C](f: B => C) = new Stream[A,C] {
    def into(out: StreamOp[C]) = self.into(new MapOp(f, out))
  }
//  def multiSplit
  def offset(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new OffsetOp(n, out))
  }
  def prepend(list: List[B]) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new PrependOp(list, out))
  }
  def reduce(f: (B, B) => B) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new ReduceOp(f, out))
  }
//  def split
  def take(n: Int) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new TakeOp(n, out))
  }
  def takeWhile(p: B => Boolean) = new Stream[A,B] {
    def into(out: StreamOp[B]) = self.into(new TakeWhileOp(p, out))
  }
}
