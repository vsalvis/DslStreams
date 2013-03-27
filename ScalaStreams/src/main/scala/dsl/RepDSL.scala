package dsl

import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions

trait RepStreamDSL { this: Base =>

  // Concepts
  abstract class RepStreamOp {
    def onData(data: Rep[Int])
  }
  class MapOp(f: Rep[Int] => Rep[Int], next: RepStreamOp) extends RepStreamOp {
    def onData(data: Rep[Int]) = next.onData(f(data))
  }
  class PrintlnOp extends RepStreamOp {
    def onData(data: Rep[Int]) = println(data)
  }
  class ListInput(input: Rep[List[Int]], stream: RepStreamOp) {
    input foreach stream.onData
  }
  
//  //API
//  object RepStream {
//    def apply = new RepStream {
//      def into(out: RepStreamOp): RepStreamOp = out
//    }
//  }
//  
//  abstract class RepStream { self =>
//    def into(out: RepStreamOp): RepStreamOp
//    def into[C](next: RepStream): RepStream = new RepStream {
//      def into(out: RepStreamOp) = self.into(next.into(out))
//    }
//    
//    def print = into(new PrintlnOp)
//    
//    def map(f: Rep[Int] => Rep[Int]) = new RepStream {
//      def into(out: RepStreamOp) = self.into(new MapOp(f, out))
//    }
//  }
}

trait RepStreamDSLExp extends RepStreamDSL with FunctionsExp with MathOpsExp with NumericOpsExp with NumericOpsExpOpt { this: BaseExp =>
  
}

trait ScalaGenRepStreamDSL extends ScalaGenBase with ScalaGenFunctions {
  val IR: BaseExp with RepStreamDSLExp
  import IR._

  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
    case _ => super.emitNode(sym, node)
  }
}

// Usage Example: First, we write a program in the DSL, importing the necessary LMS primitives.
trait Prog { this: Base with RepStreamDSL with MathOps with NumericOps =>
  def dsl_f(): Unit = {
    new ListInput(List(unit(1), unit(2), unit(3)), (RepStream.apply) map({x => x * unit(2)}) map({x => x + unit(3)}) print)
  }
}

object Usage extends App {
  // We then instantiate the program, so that we can generate regular Scala code from the DSL code.
  val concreteProg = new Prog with EffectExp with RepStreamDSLExp /* with StreamDSLOpt */ with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenRepStreamDSL with ScalaGenMathOps with ScalaGenNumericOps { val IR: self.type = self }
  }
  // Import the dsl functions and the methods to compile and generate code.
  import concreteProg._


  // ============F=============
  
  // The function f takes a Stream[Int, Double] and scales it by a factor 42.
  // Let's compile it so that we can use it in this regular Scala code.
  val scala_f = compile(dsl_f)
  
  // Let's print the generated code to satisfy our curiosity.
  codegen.emitSource(dsl_f, "F", new java.io.PrintWriter(System.out))
  
  scala_f()
}  
