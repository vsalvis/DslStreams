//package dsl
//
//import scala.virtualization.lms.common
//import scala.virtualization.lms.common._
//import scala.virtualization.lms.common.Functions
//import streams._
//
//// Concepts and concrete syntax
//trait StreamDSL { this: Base =>
//
//  // Concepts
//  type DoubleStream
//  def doubleStream_scale(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream]
//  def doubleStream_map(s: Rep[DoubleStream], f: Rep[Double] => Rep[Double]): Rep[DoubleStream]
//  
//  // Concrete syntax
//  final def infix_*(s: Rep[DoubleStream], k: Rep[Double]): Rep[DoubleStream] = doubleStream_scale(s, k)
//}
//
//// Intermediate representation
//trait StreamDSLExp extends StreamDSL with FunctionsExp with MathOpsExp { this: BaseExp =>
//
//  case class DoubleStreamScale(s: Exp[DoubleStream], k: Exp[Double]) extends Def[DoubleStream]
//  override def doubleStream_scale(s: Exp[DoubleStream], k: Exp[Double]) = DoubleStreamScale(s, k)
//
//  case class DoubleStreamMap(s: Exp[DoubleStream], f: Exp[Double => Double]) extends Def[DoubleStream]
//  override def doubleStream_map(s: Exp[DoubleStream], f: Exp[Double] => Exp[Double]) = DoubleStreamMap(s, doLambdaDef(f))
//
//  override type DoubleStream = Stream[Double, Double]
//  
//}
//
//// Optimizations working on the intermediate representation
//trait StreamDSLOpt extends StreamDSLExp with NumericOpsExp with NumericOpsExpOpt { this: BaseExp =>
//
//  override def doubleStream_scale(s: Exp[DoubleStream], k: Exp[Double]) = k match {
//    case Const(1.0) => s
//    case _ => s match {
//      case Def(DoubleStreamScale(s1, k1)) => super.doubleStream_scale(s1, k * k1)
//      case Def(DoubleStreamMap(s1, f)) => super.doubleStream_map(s1, {x => k * f(x)})
//      case _ => super.doubleStream_scale(s, k)
//    }
//  }
//  
//  override def doubleStream_map(s: Exp[DoubleStream], f: Exp[Double] => Exp[Double]) = s match {
//    case Def(DoubleStreamScale(s1, k)) => super.doubleStream_map(s1, {x => f(k * x)})
//    case Def(DoubleStreamMap(s1, f1)) => super.doubleStream_map(s1, {x => f(f1(x))})
//    case _ => super.doubleStream_map(s, f)
//  }
//
//}
//
//// Scala code generator
//trait ScalaGenStreamDSL extends ScalaGenBase with ScalaGenFunctions {
//  val IR: BaseExp with StreamDSLExp
//  import IR._
//
//  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
//    case DoubleStreamScale(s, k) => {
//      emitValDef(sym, quote(s) + ".map(x => x * " + quote(k) + ")")
//    }
//    case DoubleStreamMap(s, f) => {
//      emitValDef(sym, quote(s) + ".map(" + quote(f) + ")")
//    }
//    case _ => super.emitNode(sym, node)
//  }
//
//}
//
//// Usage
//trait Prog { this: Base with StreamDSL with MathOps =>
//
//  def f(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(42.0)
//
//  def g(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(1.0)
//  
//  def h(s: Rep[DoubleStream]): Rep[DoubleStream] = doubleStream_map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
//  def j(s: Rep[DoubleStream]): Rep[DoubleStream] = doubleStream_map(doubleStream_map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}), {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
//  
//  def f1(s: Rep[DoubleStream]): Rep[DoubleStream] = s * unit(42.0) * unit(2.0) * unit(3.0)
//  def f2(s: Rep[DoubleStream]): Rep[DoubleStream] = doubleStream_map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0) * unit(3.0)
//  def f3(s: Rep[DoubleStream]): Rep[DoubleStream] = doubleStream_map(s * unit(42.0) * unit(3.0), {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0)
//
//}
//
//object Usage extends App {
//
//  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with CompileScala { self =>
//    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps { val IR: self.type = self }
//    codegen.emitSource(f, "F", new java.io.PrintWriter(System.out))
//    codegen.emitSource(g, "G", new java.io.PrintWriter(System.out))
//    codegen.emitSource(h, "H", new java.io.PrintWriter(System.out))
//    codegen.emitSource(j, "J", new java.io.PrintWriter(System.out))
//    codegen.emitSource(f1, "F1", new java.io.PrintWriter(System.out))
//    codegen.emitSource(f2, "F2", new java.io.PrintWriter(System.out))
//    codegen.emitSource(f3, "F3", new java.io.PrintWriter(System.out))
//  }
//
//  val f = concreteProg.compile(concreteProg.f)
//  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 42: ")
//  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, f(Stream[Double]) print)
//
//  println
//  val g = concreteProg.compile(concreteProg.g)
//  println("\nscaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 1.0: ")
//  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, g(Stream[Double]) print)
//
//  println
//  val h = concreteProg.compile(concreteProg.h)
//  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
//  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, h(Stream[Double]) print)
//
//  println
//  val j = concreteProg.compile(concreteProg.j)
//  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
//  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, j(Stream[Double]) print)
//
//  println
//}