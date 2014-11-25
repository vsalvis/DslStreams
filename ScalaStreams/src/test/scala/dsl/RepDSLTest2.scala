package dsl

import streams._
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.internal.ScalaCompile
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter



trait RepStreamProg2 extends RepStreamOps {
  def test1(): RepStreamOp[Int] = {
    new RepMapOp[Int, Int](_ * unit(2), new RepFoldOp[Int, Int](_ + _, 0, new RepPrintOp[Int]))
  }
  def test2(): RepStreamOp[Int] = {
    new RepMapOp[Int, Int]({x: Rep[Int] => x * unit(2)}, 
        new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, 
            new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, 
                new RepFilterOp[Int]({x: Rep[Int] => x > unit(4)}, 
                    new RepPrintOp[Int]))))
  }
}

class TestRepStreamOps2 extends FileDiffSuite {

  val prefix = "test-out/test2/"
    
  def testRepStream1 = {
    val i = 1
    withOutFile(prefix + "stream" + i){
      new RepStreamProg2 with RepStreamCompile { self =>
        val test = test1 _

        val printWriter = new java.io.PrintWriter(System.out)
        codegen.emitSourceStream(test(), "test" + i, printWriter)
        val streamOp = compileStream(test())
        streamOp.onData(1)
        streamOp.onData(2)
        streamOp.onData(3)
        streamOp.onData(4)
        streamOp.flush
        streamOp.onData(1)
        streamOp.onData(2)
      }
    }
    assertFileEqualsCheck(prefix + "stream" + i)
  }
  def testRepStream1b = {
    val i = 1
    withOutFile(prefix + "stream" + i + "b"){
      new RepStreamProg2 with RepStreamCompile { self =>
        val test = test1 _

        val printWriter = new java.io.PrintWriter(System.out)

//        val stream = test()
        val onDataFun: (Int => Unit) = compile(test().onData _)
        val flushFun: (Unit => Unit) = compile({x: Rep[Unit] => test().flush})

        onDataFun(1)
        onDataFun(2)
        onDataFun(3)
        onDataFun(4)
        flushFun()
        onDataFun(1)
        onDataFun(2)
      }
    }
    assertFileEqualsCheck(prefix + "stream" + i + "b")
  }
  def testRepStream2 = {
    val i = 2
    withOutFile(prefix + "stream" + i){
      new RepStreamProg2 with RepStreamCompile { self =>
        val test = test2 _

        val printWriter = new java.io.PrintWriter(System.out)
        codegen.emitSourceStream(test(), "test" + i, printWriter)
        val streamOp = compileStream(test())
        streamOp.onData(1)
        streamOp.onData(2)
        streamOp.onData(3)
        streamOp.flush
        streamOp.onData(1)
        streamOp.onData(2)
        streamOp.onData(3)
        streamOp.flush
      }
    }
    assertFileEqualsCheck(prefix + "stream" + i)
  }
}
