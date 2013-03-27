//package dsl
//
//import scala.virtualization.lms.common
//import scala.virtualization.lms.common._
//import scala.virtualization.lms.common.Functions
//
//trait RepStreamOps { this: Base =>
//
//  
//  object RepStream {
//    def apply(input: Rep[List[Int]])(implicit pos: SourceContext) = stream_input(input)
//  }
//
//  implicit def varToRepStreamOps(x: Var[RepStream]) = new RepStreamOpsCls(readVar(x))
//  implicit def repToRepStreamOps(a: Rep[RepStream]) = new RepStreamOpsCls(a)
//  
//  class RepStreamOpsCls(s: Rep[RepStream]) {
//    def map(f: Rep[Int] => Rep[Int]) = stream_map(s, f)
//    def print() = stream_print(s)
//  }
//  
//  def stream_input(input: Rep[List[Int]]): Rep[RepStream]
//  def stream_map(s: Rep[RepStream], f: Rep[Int] => Rep[Int]): Rep[RepStream]
//  def stream_print(s: Rep[RepStream]): Rep[RepStream]
//}
//
//trait RepStreamExp extends RepStreamOps with EffectExp with VariablesExp {
//  case class StreamInput(input: Rep[List[Int]]) extends Def[RepStream]
//  case class StreamMap(s: Exp[RepStream], x: Sym[Int], block: Block[Int]) extends Def[RepStream]
//  case class StreamPrint(s: Exp[RepStream]) extends Def[RepStream]
//  
//  def stream_input(input: Rep[List[Int]])(implicit pos: SourceContext) = StreamInput(input)
//  def stream_map(s: Exp[RepStream], f: Exp[Int] => Exp[Int])(implicit pos: SourceContext) = {
//    val a = fresh[Int]
//    val b = reifyEffects(f(a))
//    reflectEffect(StreamMap(s, a, b), summarizeEffects(b).star)
//  }
//  def stream_print(s: Exp[RepStream])(implicit pos: SourceContext) = StreamPrint(s)
//
//  
//  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = {
//    (e match {
//      case StreamInput(l) => stream_input(f(l))
//      case _ => super.mirror(e,f)
//    }).asInstanceOf[Exp[A]] // why??
//  }
//  
//  override def syms(e: Any): List[Sym[Any]] = e match {
//    case StreamMap(s, x, body) => syms(a) ::: syms(body)
//    case _ => super.syms(e)
//  }
//
//  override def boundSyms(e: Any): List[Sym[Any]] = e match {
//    case StreamMap(s, x, body) => x :: effectSyms(body)
//    case _ => super.boundSyms(e)
//  }
//
//  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
//    case StreamMap(s, x, body) => freqNormal(s):::freqHot(body)
//    case _ => super.symsFreq(e)
//  }  
//}
//
//trait RepStreamOpsExpOpt extends RepStreamOpsExp {}
//
//trait ScalaGenRepStreamOps extends GenericNestedCodegen with ScalaGenEffect {
//  val IR: RepStreamOpsExp
//  import IR._
//
//  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
//    case StreamInput(input) =>
//    case StreamMap(s, x, block) =>
//    case StreamPrint(s) =>
//
//    
////    case ListNew(xs) => emitValDef(sym, "List(" + (xs map {quote}).mkString(",") + ")")
////    case ListMap(l,x,blk) => 
////      stream.println("val " + quote(sym) + " = " + quote(l) + ".map{")
////      stream.println(quote(x) + " => ")
////      emitBlock(blk)
////      stream.println(quote(getBlockResult(blk)))
////      stream.println("}")
//    case _ => super.emitNode(sym, rhs)
//  }
//}
//  
//  /////////// List 
////  //API
////  object RepStream {
////    def apply = new RepStream {
////      def into(out: RepStreamOp): RepStreamOp = out
////    }
////  }
////  
////  abstract class RepStream { self =>
////    def into(out: RepStreamOp): RepStreamOp
////    def into[C](next: RepStream): RepStream = new RepStream {
////      def into(out: RepStreamOp) = self.into(next.into(out))
////    }
////    
////    def print = into(new PrintlnOp)
////    
////    def map(f: Rep[Int] => Rep[Int]) = new RepStream {
////      def into(out: RepStreamOp) = self.into(new MapOp(f, out))
////    }
////  }
//}
//
