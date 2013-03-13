package dsl

import collection.mutable.Stack
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import streams._
import java.io.StringWriter
import java.io.PrintWriter


class DSLTest extends FlatSpec with ShouldMatchers {
  
  val printall = false 
  trait Prog { this: Base with StreamDSL with MathOps with NumericOps with StringOps =>
    def t0(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,Double]] = 
      s * unit(42.0)
    def t1(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,Double]] = 
      s * unit(1.0)
    def t2(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = 
      map[Double,Double,Double](s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)})
    def t3(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] = 
      map(map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}), {(x: Rep[Double]) => x + unit(3.0)})
    def t4(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] =
      s * unit(42.0) * unit(2.0) * unit(3.0)
    def t5(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] =
      map(s, {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0) * unit(3.0)
    def t6(s: Rep[Stream[Double,Double]]): Rep[Stream[Double,Double]] =
      map(s * unit(42.0) * unit(3.0), {(x: Rep[Double]) => Math.pow(unit(2.0), x)}) * unit(2.0)
    def t7(s: Rep[Stream[Int,Double]]): Rep[Stream[Int,String]] =
      map[Int,Double,String](s, {(x: Rep[Double]) => "'" + x + "'"})
    def t8(s: Rep[Stream[Char,Double]]): Rep[Stream[Char,Double]] =
      s * unit(42.0) * unit(2.0) * unit(3.0)
  }
  
  def getCode(nr: Int): String = {
    val output = new java.io.StringWriter
    // Reinstantiate Prog to avoid generation of continuous variable names that would
    // make generated function code depend on order of code generation.
    val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with MathOpsExp with NumericOpsExp with StringOpsExp with CompileScala { self =>
      override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps with ScalaGenStringOps { val IR: self.type = self }
    }
    import concreteProg._
    nr match {
        case 0 => codegen.emitSource(t0, "Class" + nr, new java.io.PrintWriter(output))
        case 1 => codegen.emitSource(t1, "Class" + nr, new java.io.PrintWriter(output))
        case 2 => codegen.emitSource(t2, "Class" + nr, new java.io.PrintWriter(output))
        case 3 => codegen.emitSource(t3, "Class" + nr, new java.io.PrintWriter(output))
        case 4 => codegen.emitSource(t4, "Class" + nr, new java.io.PrintWriter(output))
        case 5 => codegen.emitSource(t5, "Class" + nr, new java.io.PrintWriter(output))
        case 6 => codegen.emitSource(t6, "Class" + nr, new java.io.PrintWriter(output))
        case 7 => codegen.emitSource(t7, "Class" + nr, new java.io.PrintWriter(output))
        case 8 => codegen.emitSource(t8, "Class" + nr, new java.io.PrintWriter(output))
    }
    output.toString
  }
      
  val expectedCode: Map[Int, String] = Map(
      0 -> """((streams.Stream[Int, Double])=>(streams.Stream[Int, Double])) {
def apply(x0:streams.Stream[Int, Double]): streams.Stream[Int, Double] = {
val x1 = x0.map(x => x * 42.0)
x1
}
}
""",
      1 -> """((streams.Stream[Int, Double])=>(streams.Stream[Int, Double])) {
def apply(x0:streams.Stream[Int, Double]): streams.Stream[Int, Double] = {
x0
}
}
""",
      2 -> """((streams.Stream[Double, Double])=>(streams.Stream[Double, Double])) {
def apply(x0:streams.Stream[Double, Double]): streams.Stream[Double, Double] = {
val x3 = {x1: (Double) => 
val x2 = java.lang.Math.pow(2.0,x1)
x2: Double
}
val x4 = x0.map(x3)
x4
}
}
""",
      3 -> """((streams.Stream[Double, Double])=>(streams.Stream[Double, Double])) {
def apply(x0:streams.Stream[Double, Double]): streams.Stream[Double, Double] = {
val x8 = {x5: (Double) => 
val x6 = java.lang.Math.pow(2.0,x5)
val x7 = x6 + 3.0
x7: Double
}
val x9 = x0.map(x8)
x9
}
}
""",
      4 -> """((streams.Stream[Double, Double])=>(streams.Stream[Double, Double])) {
def apply(x0:streams.Stream[Double, Double]): streams.Stream[Double, Double] = {
val x3 = x0.map(x => x * 252.0)
x3
}
}
""",
      5 -> """((streams.Stream[Double, Double])=>(streams.Stream[Double, Double])) {
def apply(x0:streams.Stream[Double, Double]): streams.Stream[Double, Double] = {
val x14 = {x10: (Double) => 
val x11 = java.lang.Math.pow(2.0,x10)
val x12 = 2.0 * x11
val x13 = 3.0 * x12
x13: Double
}
val x15 = x0.map(x14)
x15
}
}
""",
      6 -> """((streams.Stream[Double, Double])=>(streams.Stream[Double, Double])) {
def apply(x0:streams.Stream[Double, Double]): streams.Stream[Double, Double] = {
val x12 = {x8: (Double) => 
val x9 = 126.0 * x8
val x10 = java.lang.Math.pow(2.0,x9)
val x11 = 2.0 * x10
x11: Double
}
val x13 = x0.map(x12)
x13
}
}
""",
      7 -> """((streams.Stream[Int, Double])=>(streams.Stream[Int, java.lang.String])) {
def apply(x0:streams.Stream[Int, Double]): streams.Stream[Int, java.lang.String] = {
val x4 = {x1: (Double) => 
val x2 = "'"+x1
val x3 = x2+"'"
x3: java.lang.String
}
val x5 = x0.map(x4)
x5
}
}
""",
      8 -> """((streams.Stream[Char, Double])=>(streams.Stream[Char, Double])) {
def apply(x0:streams.Stream[Char, Double]): streams.Stream[Char, Double] = {
val x3 = x0.map(x => x * 252.0)
x3
}
}
"""    
  )
  def getExpectedCode(n: Int): String = {
    "/*****************************************\n" +
    "  Emitting Generated Code                  \n" +
    "*******************************************/\n" +
    "class Class" + n + " extends " + expectedCode(n) +
    "/*****************************************\n" +
    "  End of Generated Code                  \n" +
    "*******************************************/\n"
  }
  
  def getOutput(n: Int): String = {
    val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with MathOpsExp with NumericOpsExp with StringOpsExp with CompileScala { self =>
      override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps with ScalaGenStringOps { val IR: self.type = self }
    }
    import concreteProg._
    val output = new java.io.StringWriter
//    n match {
//        case 0 => compile(t0)
//        case 1 => compile(t1)
//        case 2 => compile(t2)
//        case 3 => compile(t3)
//        case 4 => compile(t4)
//        case 5 => compile(t5)
//        case 6 => compile(t6)
//        case 7 => compile(t7)
//        case 8 => compile(t8)
//    }
    val t0 = concreteProg.compile(concreteProg.t0)
    new streams.ListInput(1 :: 2 :: 3 :: Nil, t0(Stream[Int] map({x: Int => 2.5 * x})) printTo(output))
    println("==out===" + output.toString)
    ???
  }
  def getExpectedOutput(n: Int): String = {
    ???
  }
      
  val codeTable = Table("n", List.range(0, expectedCode.size):_*)
  
  if (printall) {
    forAll (codeTable) { (n:Int) =>
      println("%generated: " + n + "%" + getCode(n) + "%")
      println("%expected:  " + n + "%" + getExpectedCode(n) + "%")
    } 
  }
  "All functions" should "generate the expected code" in {
    forAll (codeTable) { (n: Int) =>
      getCode(n) should equal (getExpectedCode(n))
    }
  }
  
//  val outputTable = Table("n", List.range(0, expectedOutput.size):_*)
//  
//  "All functions" should "execute correctly" in {
//    forAll (outputTable) { (n: Int) =>
//      getOutput(n) should equal (getExpectedOutput(n))
//    }
//  }


/*  val f = compile(f)
  println("scaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 42: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, f(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val g = compile(g)
  println("\nscaling 1.0 :: 2.0 :: 3.0 :: Nil by a factor of 1.0: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, g(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val h = compile(h)
  println("\n2^x for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, h(Stream[Double]) print)

  println
  val f4 = compile(f4)
  println("\n'2.5*x' for x = 1 :: 2 :: 3 :: Nil: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, f4(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val f4 = compile(f4)
  println("\n'2.5*x' for x = 1 :: 2 :: 3 :: Nil: ")
  new streams.ListInput(1 :: 2 :: 3 :: Nil, f4(Stream[Int] map({x: Int => 2.5 * x})) print)

  println
  val test = compile(test)
  println("\n2^x + 3 for x = 1.0 :: 2.0 :: 3.0 :: Nil: ")
  
  new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, test(Stream[Double]) print)

  println
  */
}
