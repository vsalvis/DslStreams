package dsl

import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import streams._

////StreamOps
//abstract class StreamOp[Rep[A]] {
//  def onData(data: Rep[A])
//}
//class MapOp[Rep[A], Rep[B]](f: Rep[A] => Rep[B], next: StreamOp[Rep[B]]) extends StreamOp[Rep[A]] {
//  def onData(data: Rep[A]) = next.onData(f(data))
//}
//class PrintlnOp[Rep[A]] extends StreamOp[Rep[A]] {
//  def onData(data: Rep[A]) = println(data)
//}
//class ListInput[Rep[A]](input: List[A], stream: StreamOp[Rep[A]]) {
//  input foreach stream.onData
//}
//
//
////API
//object Stream {
//  def apply[A] = new Stream[A,A] {
//    def into(out: StreamOp[A]): StreamOp[A] = out
//  }
//}
//
//abstract class Stream[A,B] { self =>
//  def into(out: StreamOp[B]): StreamOp[A]
//  def into[C](next: Stream[B, C]): Stream[A, C] = new Stream[A, C] {
//    def into(out: StreamOp[C]) = self.into(next.into(out))
//  }
//  
//  def print = into(new PrintlnOp[B]())
//  
//  def map[C](f: B => C) = new Stream[A,C] {
//    def into(out: StreamOp[C]) = self.into(new MapOp(f, out))
//  }
//}

// Concepts and concrete syntax
trait StreamDSL { this: Base =>

  // Concepts
  def scale[A:Manifest](s: Rep[Stream[A,Double]], k: Rep[Double]): Rep[Stream[A,Double]]
  def map[A:Manifest, B:Manifest, C:Manifest](s: Rep[Stream[A,B]], f: Rep[B] => Rep[C]): Rep[Stream[A,C]]
  
  // Concrete syntax
  final def infix_*[A: Manifest](s: Rep[Stream[A,Double]], k: Rep[Double]): Rep[Stream[A,Double]] = scale[A](s, k)
}

// Intermediate representation
trait StreamDSLExp extends StreamDSL with FunctionsExp with MathOpsExp with NumericOpsExp with NumericOpsExpOpt with StringOpsExp { this: BaseExp =>

  case class Scale[A](s: Exp[Stream[A,Double]], k: Exp[Double]) extends Def[Stream[A,Double]]
  override def scale[A:Manifest](s: Exp[Stream[A,Double]], k: Exp[Double]) = Scale[A](s, k)

  case class Map[A,B,C](s: Exp[Stream[A,B]], f: Exp[B => C]) extends Def[Stream[A,C]]
  override def map[A:Manifest, B:Manifest, C:Manifest](s: Exp[Stream[A,B]], f: Exp[B] => Exp[C]) = Map[A,B,C](s, doLambdaDef(f))

}

// Optimizations working on the intermediate representation
trait StreamDSLOpt extends StreamDSLExp { this: BaseExp =>

  override def scale[A:Manifest](s: Exp[Stream[A,Double]], k: Exp[Double]) = k match {
    case Const(1.0) => s
    case _ => s match {
      case Def(Scale(s1: Exp[Stream[A,Double]], k1: Exp[Double])) => super.scale[A](s1, k * k1)
//      case Def(Map(s1, f)) => super.map(s1, {x: A => k * f(x)})
      case _ => super.scale(s, k)
    }
  }
  
  override def map[A:Manifest, B:Manifest, C:Manifest](s: Exp[Stream[A,B]], f: Exp[B] => Exp[C]) = s match {
//    case Def(Scale(s1, k)) => super.map(s1, {x => f(k * x)})
//    case Def(Map(s1, Def(Lambda(f1,_,_)))) => super.map(s1, {x => f(f1(x))})
    case _ => super.map(s, f)
  }

}

// Scala code generator
trait ScalaGenStreamDSL extends ScalaGenBase with ScalaGenFunctions {
  val IR: BaseExp with StreamDSLExp
  import IR._

  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
    case Scale(s, k) => {
      emitValDef(sym, quote(s) + ".map(x => x * " + quote(k) + ")")
    }
    case Map(s, f) => {
      emitValDef(sym, quote(s) + ".map(" + quote(f) + ")")
    }
    case _ => super.emitNode(sym, node)
  }

}

// Usage
trait Prog { this: Base with StreamDSL with MathOps with NumericOps with StringOps =>

  def f(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,Double]] = s * unit(42.0)

  def g(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,Double]] = s * unit(1.0)
  
  def h(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = map[Double,Double,Double](s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
  
  def test(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = 
    map(map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}),
        {(x: Rep[Double]) => x + unit(3.0)})
  
  def f1(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = s * unit(42.0) * unit(2.0) * unit(3.0)
  def f2(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0) * unit(3.0)
  def f3(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = map(s * unit(42.0) * unit(3.0), {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0)
  def f4(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,String]] = map[Int,Double,String](s, {(x: Rep[Double]) => "'" + x + "'"})

}

object Usage extends App {
  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps with ScalaGenStringOps { val IR: self.type = self }
    codegen.emitSource(f, "F", new java.io.PrintWriter(System.out))
    codegen.emitSource(g, "G", new java.io.PrintWriter(System.out))
    codegen.emitSource(h, "H", new java.io.PrintWriter(System.out))
    codegen.emitSource(test, "Test", new java.io.PrintWriter(System.out))
    codegen.emitSource(f1, "F1", new java.io.PrintWriter(System.out))
    codegen.emitSource(f2, "F2", new java.io.PrintWriter(System.out))
    codegen.emitSource(f3, "F3", new java.io.PrintWriter(System.out))
    codegen.emitSource(f4, "F4", new java.io.PrintWriter(System.out))
  }

  val f = concreteProg.compile(concreteProg.f)
  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 42: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, f(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val g = concreteProg.compile(concreteProg.g)
  println("\nscaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 1.0: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, g(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val h = concreteProg.compile(concreteProg.h)
  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, h(Stream[Double]) print)

  println
  val f4 = concreteProg.compile(concreteProg.f4)
  println("\n'2.5*x' for x = 1 :: 2 :: 3 :: Nil: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, f4(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val test = concreteProg.compile(concreteProg.test)
  println("\n2^x + 3 for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, test(Stream[Double]) print)

  println
}


