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
  with OrderingOps with OverloadHack {

  def onData1(i: Rep[Int]) = {
    val s = RepStream[Int]
        .map({x: Rep[Int] => x * unit(2)})
        .filter({x: Rep[Int] => x > unit(3)})
        .flatMap({x: Rep[Int] => x :: (x + unit(1)) :: Nil})
        .print
    s.onData(i)
  }
  
  def onData2(i: Rep[Int]) = {
    val s = RepStream[Int].fold[Int]({(x, y) => x + y}, 1).print
    s.onData(i)
  }
}

object RepUsage extends App {
  // We then instantiate the program, so that we can generate regular Scala code from the DSL code.
  val concreteProg = new RepStreamUsageProg with RepStreamOpsExp with NumericOpsExp with NumericOpsExpOpt
        with OrderingOpsExp with BooleanOpsExpOpt with ScalaCompile { self =>
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
//class onData2(px19:Array[Int]) extends ((Int)=>(Unit)) {
//  def apply(x18:Int): Unit = {
//    val x19 = px19 // static data: Array(1)
//    val x20 = x19(0)
//    val x21 = x18 + x20
//    val x22 = x19(0) = x21
//    val x23 = println(x21)
//    ()
//  }
//}
///*****************************************
//  End of Generated Code                  
//*******************************************/
//compilation: ok
//2
//4
//7
//11
//without state
//class onData2() extends ((Int)=>(Unit)) {
//def apply(x18:Int): Unit = {
//val x19 = x18 + 1
//val x20 = println(x19)
//()
//}
//}

