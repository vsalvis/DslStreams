package dsl


import scala.virtualization.lms.common._
import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.internal._
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileOutputStream
import scala.reflect.SourceContext
import scala.collection.mutable.HashMap


// Usage Example: First, we write a program in the DSL.
trait RepStreamUsageProg extends RepStreamOps with NumericOps
  with OrderingOps with OverloadHack with LiftNumeric {

  def onData1(i: Rep[Int]) = {
    val s = RepStream[Int]
        .map({x: Rep[Int] => x * unit(2)})
        .filter({x: Rep[Int] => x > unit(3)})
        .flatMap({x: Rep[Int] => x :: (x + unit(1)) :: Nil})
        .print
    s.onData(i)
  }
  
def onData2(i: Rep[Int]) = {
  val s = new RepFoldOp[Int, Int]({(x, y) => x + y}, 0, new RepPrintOp[Int])
//  val s = RepStream[Int].fold[Int]({(x, y) => x + y}, 0).print
  s.onData(i)
}
  
def abs(x: Rep[Int]): Rep[Int] = {
  if(x >= 0) { x } else { 0 - x }
}
def times3(x: Rep[Int]): Rep[Int] = x * 3

def absPlus(x: Rep[Int]) = {
  println(times3(abs(1)))
  println(times3(abs(-2)))
  println(times3(abs(x)))
}
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

object RepUsage extends App {
  // We then instantiate the program, so that we can generate regular Scala code from the DSL code.
  val concreteProg = new RepStreamUsageProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with OrderingOpsExpOpt with BooleanOpsExpOpt with ScalaCompile { self =>
    override val codegen = new ScalaGenRepStreamOps with ScalaGenNumericOps
          with ScalaGenOrderingOps { val IR: self.type = self }
  }
  // Import the dsl functions and the methods to compile and generate code.
  import concreteProg._
  val printWriter = new java.io.PrintWriter(System.out)
  codegen.emitSource(onData1 _ , "onData1", printWriter)
  
  val scala_onData1 = concreteProg.compile(onData1)
  
  scala_onData1(1)
  scala_onData1(2)
  scala_onData1(3)
  scala_onData1(4)

  codegen.emitSource(onData2 _ , "onData2", printWriter)
  
  val scala_onData2 = concreteProg.compile(onData2)
  
  scala_onData2(1)
  scala_onData2(2)
  scala_onData2(3)
  scala_onData2(4)

  codegen.emitSource(absPlus _, "absPlus", printWriter)
  
  val scala_absPlus = concreteProg.compile(absPlus)
  
  scala_absPlus(-1)
  scala_absPlus(-1)
}

///*****************************************
//  Emitting Generated Code                  
//*******************************************/
//class onData1 extends ((Int)=>(Unit)) {
//def apply(x0:Int): Unit = {
//val x1 = x0 * 2
//val x2 = x1 > 3
//val x7 = if (x2) {
//val x4 = println(x1)
//val x3 = x1 + 1
//val x5 = println(x3)
//()
//} else {
//()
//}
//()
//}
//}
///*****************************************
//  End of Generated Code                  
//*******************************************/
//compilation: ok
//4
//5
//6
//7
//8
//9
///*****************************************
//  Emitting Generated Code                  
//*******************************************/
//class onData2(px1:Array[Int]) extends ((Int)=>(Unit)) {
//  def apply(x0:Int): Unit = {
//    val x1 = px1 // static data: Array(0)
//    val x2 = x1(0)
//    val x3 = x0 + x2
//    val x4 = x1(0) = x3
//    val x5 = println(x3)
//    ()
//  }
//}
///*****************************************
//  End of Generated Code                  
//*******************************************/
//compilation: ok
//1
//3
//6
//10
///*****************************************
//  Emitting Generated Code                  
//*******************************************/
//class absPlus extends ((Int)=>(Unit)) {
//def apply(x32:Int): Unit = {
//val x33 = println(3)
//val x34 = println(6)
//val x35 = x32 >= 0
//val x37 = if (x35) {
//x32
//} else {
//val x36 = 0 - x32
//x36
//}
//val x38 = x37 * 3
//val x39 = println(x38)
//x39
//}
//}
///*****************************************
//  End of Generated Code                  
//*******************************************/
//compilation: ok
//3
//6
//3
//3
//6
//3


