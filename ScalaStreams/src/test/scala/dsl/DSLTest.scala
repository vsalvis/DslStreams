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
    nr match {
        case 0 => concreteProg.codegen.emitSource(concreteProg.t0, "Class" + nr, new java.io.PrintWriter(output))
        case 1 => concreteProg.codegen.emitSource(concreteProg.t1, "Class" + nr, new java.io.PrintWriter(output))
        case 2 => concreteProg.codegen.emitSource(concreteProg.t2, "Class" + nr, new java.io.PrintWriter(output))
        case 3 => concreteProg.codegen.emitSource(concreteProg.t3, "Class" + nr, new java.io.PrintWriter(output))
        case 4 => concreteProg.codegen.emitSource(concreteProg.t4, "Class" + nr, new java.io.PrintWriter(output))
        case 5 => concreteProg.codegen.emitSource(concreteProg.t5, "Class" + nr, new java.io.PrintWriter(output))
        case 6 => concreteProg.codegen.emitSource(concreteProg.t6, "Class" + nr, new java.io.PrintWriter(output))
        case 7 => concreteProg.codegen.emitSource(concreteProg.t7, "Class" + nr, new java.io.PrintWriter(output))
        case 8 => concreteProg.codegen.emitSource(concreteProg.t8, "Class" + nr, new java.io.PrintWriter(output))
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
  
  val concreteProg = new Prog with EffectExp with StreamDSLExp with StreamDSLOpt with MathOpsExp with NumericOpsExp with StringOpsExp with CompileScala { self =>
    override val codegen = new ScalaGenEffect with ScalaGenStreamDSL with ScalaGenMathOps with ScalaGenNumericOps with ScalaGenStringOps { val IR: self.type = self }
  }
  val t0 = concreteProg.compile(concreteProg.t0)
  val t3 = concreteProg.compile(concreteProg.t3)
  val t8 = concreteProg.compile(concreteProg.t8)
  val output: List[String] = List(
      { out: java.io.StringWriter => new streams.ListInput(1 :: 2 :: 3 :: Nil, t0(Stream[Int] map({x: Int => 2.5 * x})) printTo(out)) },
      { out: java.io.StringWriter => new streams.ListInput(1 :: 2 :: 3 :: Nil, t0(Stream[Int] map({x: Int => x % 2})) printTo(out)) },
      { out: java.io.StringWriter => new streams.ListInput(1.0 :: 2.0 :: 3.0 :: Nil, t3(Stream[Double] map({x: Double => x * 2})) printTo(out)) },
      { out: java.io.StringWriter => new streams.ListInput('a' :: 'b' :: 'c' :: Nil, t8(Stream[Char] map({x: Char => x.toInt})) printTo(out)) }
  ).map {f => val out = new java.io.StringWriter; f(out); "List(" + out.toString + ")"}

  
  val expectedOutput: List[String] = List(
    List(105.0, 210.0, 315.0),
    List(42.0, 0.0, 42.0),
    List(7.0, 19.0, 67.0),
    List(24444.0, 24696.0, 24948.0)
  ) map (_.toString)
      
  val codeTable = Table("n", List.range(0, expectedCode.size):_*)
  
  if (printall) {
    forAll (codeTable) { (n:Int) =>
      println("%generated: " + n + "%" + getCode(n) + "%")
      println("%expected:  " + n + "%" + getExpectedCode(n) + "%")
    } 
  }
  "All functions" should "generate the expected code (whitebox tests)" in {
    forAll (codeTable) { (n: Int) =>
      getCode(n) should equal (getExpectedCode(n))
    }
  }
  
  val outputTable = Table("n", List.range(0, expectedOutput.size):_*)
  
  "All functions" should "execute correctly (blackbox tests)" in {
    forAll (outputTable) { (n: Int) =>
      output(n) should equal (expectedOutput(n))
    }
  }
}
