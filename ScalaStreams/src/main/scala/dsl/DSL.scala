package dsl

import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import streams._

// Concepts and concrete syntax
trait StreamDSL { this: Base =>

  // Concepts
  type DoubleStream
  def DoubleStream_scale(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream]
//  def DoubleStream_map(s: Rep[DoubleStream], f: Rep[Double] => Rep[Double]): Rep[DoubleStream]
  
  // Concrete syntax
  final def infix_*(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream] = DoubleStream_scale(s, k)
}

// Intermediate representation
trait StreamDSLExp extends StreamDSL with FunctionsExp { this: BaseExp =>

  case class DoubleStreamScale(s: Exp[DoubleStream], k: Exp[Double]) extends Def[DoubleStream]
  override def DoubleStream_scale(s: Exp[DoubleStream], k: Exp[Double]) = DoubleStreamScale(s, k)

//  case class DoubleStreamMap(s: Exp[DoubleStream], f: Exp[Double => Double]) extends Def[DoubleStream]
//  override def DoubleStream_map(s: Exp[DoubleStream], f: Exp[Double => Double]) = DoubleStreamMap(s, f)

  override type DoubleStream = Stream[Double, Double]
}

// Optimizations working on the intermediate representation
trait StreamDSLOpt extends StreamDSLExp { this: BaseExp =>

  override def DoubleStream_scale(s: Exp[DoubleStream], k: Exp[Double]) = k match {
    case Const(1.0) => s
    case _ => super.DoubleStream_scale(s, k)
  }

}

// Scala code generator
trait ScalaGenStreamDSL extends ScalaGenBase with ScalaGenFunctions {
  val IR: BaseExp with StreamDSLExp
  import IR._

  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
    case DoubleStreamScale(s, k) => {
      emitValDef(sym, quote(s) + ".map(x => x * " + quote(k) + ")")
    }
//    case DoubleStreamMap(s, f) => {
//      emitValDef(sym, quote(s) + ".map(" + quote(f) + ")")
//    }
    case _ => super.emitNode(sym, node)
  }

}

// Usage
trait Prog { this: Base with StreamDSL =>

  def f(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(42.0)

  def g(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(1.0)
  
//  def h(s: Rep[DoubleStream]): Rep[DoubleStream] = DoubleStream_map(s, {(x: Double) => scala.math.pow(2.0, x)})

}

object Usage extends App {

  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL { val IR: self.type = self }
    codegen.emitSource(f, "F", new java.io.PrintWriter(System.out))
    codegen.emitSource(g, "G", new java.io.PrintWriter(System.out))
//    codegen.emitSource(h, "H", new java.io.PrintWriter(System.out))
  }

  val f = concreteProg.compile(concreteProg.f)
  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 42: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, f(Stream[Double]) print)

  println
  val g = concreteProg.compile(concreteProg.g)
  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 1.0: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, g(Stream[Double]) print)

//  val h = concreteProg.compile(concreteProg.h)
//  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
//  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, h(Stream[Double]) print)

  println
}