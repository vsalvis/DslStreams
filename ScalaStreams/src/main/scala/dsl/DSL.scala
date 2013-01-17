package dsl

import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import streams._

// Concepts and concrete syntax
trait StreamDSL { this: Base =>

  // Concepts
  type DoubleStream
  def scale(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream]
  def map(s: Rep[DoubleStream], f: Rep[Double] => Rep[Double]): Rep[DoubleStream]
  
  // Concrete syntax
  final def infix_*(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream] = scale(s, k)
}

// Intermediate representation
trait StreamDSLExp extends StreamDSL with FunctionsExp with MathOpsExp with NumericOpsExp with NumericOpsExpOpt { this: BaseExp =>

  case class Scale(s: Exp[DoubleStream], k: Exp[Double]) extends Def[DoubleStream]
  override def scale(s: Exp[DoubleStream], k: Exp[Double]) = Scale(s, k)

  case class Map(s: Exp[DoubleStream], f: Exp[Double => Double]) extends Def[DoubleStream]
  override def map(s: Exp[DoubleStream], f: Exp[Double] => Exp[Double]) = Map(s, doLambdaDef(f))

  override type DoubleStream = Stream[Double, Double]
  
}

// Optimizations working on the intermediate representation
trait StreamDSLOpt extends StreamDSLExp { this: BaseExp =>

  override def scale(s: Exp[DoubleStream], k: Exp[Double]) = k match {
    case Const(1.0) => s
    case _ => s match {
      case Def(Scale(s1, k1)) => super.scale(s1, k * k1)
      case Def(Map(s1, f)) => super.map(s1, {x => k * f(x)})
      case _ => super.scale(s, k)
    }
  }
  
  override def map(s: Exp[DoubleStream], f: Exp[Double] => Exp[Double]) = s match {
    case Def(Scale(s1, k)) => super.map(s1, {x => f(k * x)})
    case Def(Map(s1, f1)) => super.map(s1, {x => f(f1(x))})
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
trait Prog { this: Base with StreamDSL with MathOps with NumericOps =>

  def f(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(42.0)

  def g(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(1.0)
  
  def h(s: Rep[DoubleStream]): Rep[DoubleStream] = map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
  
  def test(s: Rep[DoubleStream]): Rep[DoubleStream] = 
    map(map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}),
        {(x: Rep[Double]) => x + unit(3.0)})
  
  def f1(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(42.0) * unit(2.0) * unit(3.0)
  def f2(s: Rep[DoubleStream]): Rep[DoubleStream] = map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0) * unit(3.0)
  def f3(s: Rep[DoubleStream]): Rep[DoubleStream] = map(s * unit(42.0) * unit(3.0), {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0)

}

object Usage extends App {
  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps { val IR: self.type = self }
    codegen.emitSource(f, "F", new java.io.PrintWriter(System.out))
    codegen.emitSource(g, "G", new java.io.PrintWriter(System.out))
    codegen.emitSource(h, "H", new java.io.PrintWriter(System.out))
    codegen.emitSource(test, "Test", new java.io.PrintWriter(System.out))
    codegen.emitSource(f1, "F1", new java.io.PrintWriter(System.out))
    codegen.emitSource(f2, "F2", new java.io.PrintWriter(System.out))
    codegen.emitSource(f3, "F3", new java.io.PrintWriter(System.out))
  }

  val f = concreteProg.compile(concreteProg.f)
  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 42: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, f(Stream[Double]) print)

  println
  val g = concreteProg.compile(concreteProg.g)
  println("\nscaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 1.0: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, g(Stream[Double]) print)

  println
  val h = concreteProg.compile(concreteProg.h)
  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, h(Stream[Double]) print)

  println
  val test = concreteProg.compile(concreteProg.test)
  println("\n2^x + 3 for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, test(Stream[Double]) print)

  println
}

// Without optimization:
class Test extends (Stream[Double,Double]=>Stream[Double,Double]) {
  def apply(x0:Stream[Double,Double]): Stream[Double,Double] = {
    val x3 = {x1: (Double) => 
      val x2 = Math.pow(2.0,x1)
      x2: Double
    }
    val x4 = x0.map(x3)  // MapOp
      val x7 = {x5: (Double) => 
      val x6 = x5 + 3.0
      x6: Double
    }
    val x8 = x4.map(x7)  // MapOp
    x8
  }
}
// With optimization:
class TestOpt extends (Stream[Double,Double]=>Stream[Double,Double]) {
  def apply(x0:Stream[Double,Double]): Stream[Double,Double] = {
    val x3 = {x1: (Double) => 
      val x2 = Math.pow(2.0,x1)
      x2: Double
    }
    val x8 = {x5: (Double) => 
      val x6 = x3(x5)
      val x7 = x6 + 3.0
      x7: Double
    }
    val x9 = x0.map(x8)  // MapOp
    x9
  }
}
