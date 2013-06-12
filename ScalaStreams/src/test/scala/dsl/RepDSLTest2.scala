package dsl

import streams._
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.internal.ScalaCompile
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter



trait RepStreamProg2 extends RepStreamOps {
  def test1(): RepStreamOp[Int] = {
    new RepReduceOp[Int]({(x, y) => x + y}, new RepPrintOp[Int])
  }
  def test2(): RepStreamOp[Int] = {
    new RepMapOp[Int, Int]({x: Rep[Int] => x * unit(2)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepFilterOp[Int]({x: Rep[Int] => x > unit(4)}, new RepPrintOp[Int]))))
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
        streamOp.flush
        streamOp.onData(1)
        streamOp.onData(2)
        streamOp.onData(3)
        streamOp.flush
      }
    }
    assertFileEqualsCheck(prefix + "stream" + i)
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

trait RepStreamCompile extends RepStreamOpsExp with ScalaCompile { self =>
    val codegen = new ScalaGenRepStreamOps {
      val IR: self.type = self
      
      def emitSourceStream[T: Manifest](s: RepStreamOp[T], className: String, out: PrintWriter): Unit = {
        emitSource(s.onData _, className + "$onData", out)
        emitSource({x: Rep[Unit] => s.flush}, className + "$flush", out)
      }
    }
  
  def compileStream[T: Manifest](s: RepStreamOp[T]): StreamOp[T] = {
    new StreamOp[T] {
      val onDataFun: (T => Unit) = compile(s.onData _)
      val flushFun: (Unit => Unit) = compile({x: Rep[Unit] => s.flush})

      def onData(data: T) = onDataFun(data)
      def flush = flushFun()
    }
  }
}

