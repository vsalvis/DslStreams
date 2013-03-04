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
trait StreamDSLExp extends StreamDSL with FunctionsExp with MathOpsExp with NumericOpsExp with NumericOpsExpOpt { this: BaseExp =>

  case class Scale[A](s: Exp[Stream[A,Double]], k: Exp[Double]) extends Def[Stream[A,Double]]
  override def scale[A:Manifest](s: Exp[Stream[A,Double]], k: Exp[Double]) = Scale[A](s, k)

  case class Map[A,B,C](s: Exp[Stream[A,B]], f: Exp[B => C])(implicit val mA: Manifest[A], val mB: Manifest[B], val mC: Manifest[C]) extends Def[Stream[A,C]]
  override def map[A:Manifest, B: Manifest, C:Manifest](s: Exp[Stream[A,B]], f: Exp[B] => Exp[C]) = Map[A,B,C](s, doLambdaDef(f))

}

// Optimizations working on the intermediate representation
trait StreamDSLOpt extends StreamDSLExp { this: BaseExp =>

  override def scale[A:Manifest](s: Exp[Stream[A,Double]], k: Exp[Double]) = k match {
    case Const(1.0) => s
    case _ => s match {
      case Def(Scale(s1, k1)) => super.scale[A](s1, numeric_times(k, k1))
      case Def(m@Map(s1: Exp[Stream[A, Any]], f: (Any => Exp[Double]))) => {
//        type tp = m.mB.erasure
//        val tp = m.mB.erasure
        super.map[A, Any, Double](s1, (x: Any)  => numeric_times(k, f(x)))(manifest[A], m.mB, manifest[Double]) 
      }
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

// Usage Example: First, we write a program in the DSL, importing the necessary LMS primitives.
trait Prog { this: Base with StreamDSL with MathOps with NumericOps =>
  def dsl_f(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,Double]] = {
    s * unit(42.0)
  }

  def dsl_g(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = {
    map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
  }
  
  def dsl_h(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = { 
    map(map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}),
        {(x: Rep[Double]) => x + unit(3.0)})
  }
}

object Usage extends App {
  // We then instantiate the program, so that we can generate regular Scala code from the DSL code.
  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps { val IR: self.type = self }
  }
  // Import the dsl functions and the methods to compile and generate code.
  import concreteProg._


  // ============F=============
  
  // The function f takes a Stream[Int, Double] and scales it by a factor 42.
  // Let's compile it so that we can use it in this regular Scala code.
  val scala_f = compile(dsl_f)
  
  // Let's print the generated code to satisfy our curiosity.
  codegen.emitSource(dsl_f, "F", new java.io.PrintWriter(System.out))
  
  // Let's create a Stream[Int, Double] that adds 1.5 to all its elements.
  val streamAdd = Stream[Int] map({x: Int => x + 1.5})
  
  // Let's apply f to streamAdd and finish the Stream with the command to print all elements.
  val streamAddFPrint = scala_f(streamAdd).print
  
  // Let's print the results of passing List(1, 2, 3) through the stream.
  println("Applying {x => 42 * (x + 1.5)} to List(1, 2, 3):")
  new streams.ListInput(List(1, 2, 3), streamAddFPrint)
  println("\n")
  
  
  // ============G=============
  
  // The function g takes a Stream[Double, Double] and maps its elements x to 2ˆx.
  val scala_g = compile(dsl_g)
  codegen.emitSource(dsl_g, "", new java.io.PrintWriter(System.out))
  
  // Let's apply g to a newly created Stream[Double, Double] and print all elements.
  val streamGPrint = scala_g(Stream[Double]).print
  
  println("Applying {x => 2.0ˆx} to List(1.0, 2.0, 3.0):")
  new streams.ListInput(List(1.0, 2.0, 3.0), streamGPrint)
  println("\n")
  
  
  

}


