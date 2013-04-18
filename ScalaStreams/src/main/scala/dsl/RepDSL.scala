package dsl

import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileOutputStream
import scala.reflect.SourceContext

trait RepStreamOps extends Variables with While
  with NumericOps with OrderingOps with IfThenElse
  with MiscOps with EmbeddedControls with Equal {
  
  abstract class RepStreamOp[A] {
    def onData(data: Rep[A])
//    def flush
  }

  class RepMapOp[A, B](f: Rep[A] => Rep[B], next: RepStreamOp[B]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = next.onData(f(data))
    
//    def flush = next.flush
  }

  class RepFilterOp[A](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = if (p(data)) next.onData(data)
    
//    def flush = next.flush
  }
  
  class RepPrintOp[A]() extends RepStreamOp[A] {
    def onData(data: Rep[A]) = println(data)
  }
  
//  class ListInput[A](l: List[Rep[A]], next: StreamOp[A]) {
//    
//  }
}

trait RepStreamProg extends RepStreamOps with NumericOps
  with OrderingOps with PrimitiveOps with Equal
  with Structs with MiscOps with ArrayOps with LiftVariables with OverloadHack
  with ListOps
  {


  def test1() = {
    val m = new RepMapOp[Int, Int]({x: Rep[Int] => x * unit(2)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepPrintOp[Int]))

    m.onData(unit(0))
    m.onData(unit(1))
    m.onData(unit(2))
    m.onData(unit(3))
  }

}

class TestRepStreamOps extends FileDiffSuite {

  val prefix = "test-out/"

  def testRepStream1 = {
    withOutFile(prefix+"generator-simple"){
       new RepStreamProg with NumericOpsExp
        with OrderingOpsExp with PrimitiveOpsExp with EqualExp
        with StructExp with StructExpOptCommon with ArrayOpsExp
        with MiscOpsExp with ListOpsExp with ScalaCompile{ self =>     /* MyScalaCompile -> ScalaCompile*/

        val printWriter = new java.io.PrintWriter(System.out)

        //test1: first "loop"
        val codegen = new ScalaGenGeneratorOps with ScalaGenNumericOps
          with ScalaGenOrderingOps with ScalaGenPrimitiveOps with ScalaGenEqual
          with ScalaGenArrayOps with ScalaGenStruct with ScalaGenMiscOps
          with ScalaGenVariables { val IR: self.type = self }

        codegen.emitSource2(test1 _ , "test1", printWriter)
        val testc1 = compile2(test1)
        testc1()

      }
    }
//    assertFileEqualsCheck(prefix+"generator-simple")
  }
}

