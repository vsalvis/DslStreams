package dsl

import scala.virtualization.lms.common
import scala.virtualization.lms.common._
import scala.virtualization.lms.common.Functions
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileOutputStream
import scala.reflect.SourceContext

trait RepStreamOps extends IfThenElse with MiscOps with BooleanOps
   with Variables with ListOps with EmbeddedControls with TupleOps {
  
  abstract class RepStreamOp[A] {
    def onData(data: Rep[A])
    def flush
  }
  
  // TODO synchronization?

  class RepMapOp[A, B](f: Rep[A] => Rep[B], next: RepStreamOp[B]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = next.onData(f(data))
    def flush = next.flush
  }
  
  class RepFilterOp[A](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = if (p(data)) next.onData(data)
    def flush = next.flush
  }

  
  class RepReduceOp[A](f: (Rep[A], Rep[A]) => Rep[A], next: RepStreamOp[A]) extends RepStreamOp[A] {
    var result: Rep[A] = null.asInstanceOf[Rep[A]]
    
    def onData(data: Rep[A]) = {
      if (result == null) {
        result = data
      } else {
        result = f(result, data)
        next.onData(result)
      }
    }
    
    def flush = {
      result = null.asInstanceOf[Rep[A]]
      next.flush
    }
  }
  
  class RepFoldOp[A, B](f: (Rep[A], Rep[B]) => Rep[B], z: Rep[B], next: RepStreamOp[B]) extends RepStreamOp[A] {
    var result = z
    
    def onData(data: Rep[A]) = {
      result = f(data, result); next.onData(result)
    }
    
    def flush = {
      result = z
      next.flush
    }
  }
  
  class RepFlatMapOp[A, B](f: Rep[A] => List[Rep[B]], next: RepStreamOp[B]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = f(data) foreach next.onData
  
    def flush = next.flush
  }
  
    
  class RepDropOp[A](n: Int, next: RepStreamOp[A]) extends RepStreamOp[A] {
    var num = n
    def onData(data: Rep[A]) = {
      if (num > 0) {
        num -= 1
      } else {
        next.onData(data)
      }
    }
    
    def flush = {
      num = n
      next.flush
    }
  }

  class RepDropWhileOp[A](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A]) extends RepStreamOp[A] {
    var dropping = unit(true)
    def onData(data: Rep[A]) = {
      dropping = dropping && p(data)
// TODO LMS bug?      
//      if (dropping && !p(data)) {
//        dropping = unit(false)
//      }
      
      if (!dropping) {
        next.onData(data)
      }
    }
    
    def flush = {
      dropping = unit(true)
      next.flush
    }
  }

  class RepTakeOp[A](n: Int, next: RepStreamOp[A]) extends RepStreamOp[A] {
    var num = n
    def onData(data: Rep[A]) = {
      if (num > 0) {
        num -= 1
        next.onData(data)
      }
    }
    
    def flush = {
      num = n
      next.flush
    }
  }

  class RepTakeWhileOp[A](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A]) extends RepStreamOp[A] {
    var taking = unit(true)
    def onData(data: Rep[A]) = {
      if (taking && !p(data)) {
        taking = unit(false)
      }
      if (taking) {
        next.onData(data)
      }
    }
    
    def flush = {
      taking = unit(true)
      next.flush
    }
  }

  class RepPrependOp[A](list: List[Rep[A]], next: RepStreamOp[A]) extends RepStreamOp[A] {
    list foreach next.onData
    
    def onData(data: Rep[A]) = next.onData(data)
    
    def flush = {
      next.flush
      list foreach next.onData
    }
  }
  
  // TODO is this Queue ok? what if the data comes in dynamically?
  // test5 seems to show it's working, but why?
  class RepOffsetOp[A](n: Int, next: RepStreamOp[A]) extends RepStreamOp[A] {
    val buffer = new scala.collection.mutable.Queue[Rep[A]]
    
    def onData(data: Rep[A]) = {
      if (buffer.size == n) {
        next.onData(buffer.dequeue)
      }
      buffer += data
    }
  
    def flush = {
      buffer.dequeueAll(_ => true)
      next.flush
    }
  }

  class RepGroupByOp[A, B](keyFun: Rep[A] => Rep[B], streamOpFun: Rep[B] => RepStreamOp[A]) extends RepStreamOp[A] {
    val map = new scala.collection.mutable.HashMap[Rep[B], RepStreamOp[A]]()
    
    def onData(data: Rep[A]) = {
      val key = keyFun(data)
      map.get(key) match {
        case None => {
          val streamOp = streamOpFun(key)
          map += ((key, streamOp))
          streamOp.onData(data)
        }
        case Some(op) => op.onData(data)
      }
    }
  
    def flush = {
      map.values foreach { _.flush }
    }
  }

  /*
  //TODO remove this one? Map in LMS?
  class RepGroupByStreamOp[A, B](keyFun: Rep[A] => Rep[B], next: RepStreamOp[Map[Rep[B], List[Rep[A]]]]) extends RepStreamOp[A] {
    val map = new scala.collection.mutable.HashMap[Rep[B], List[Rep[A]]]()
    
    def onData(data: Rep[A]) = {
      val key = keyFun(data)
      map.get(key) match {
        case None => {
          map += ((key, data :: Nil))
        }
        case Some(list) => map += ((key, data :: list))
      }
      next.onData(map.toMap)
    }
    
    def flush = {
      map.clear
      next.flush
    }
  }
  */

  class RepDuplicateOp[A](next1: RepStreamOp[A], next2: RepStreamOp[A]) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = {
      next1.onData(data)
      next2.onData(data)
    }
    
    def flush = {
      next1.flush
      next2.flush
    }
  }
  
  // TODO better to use List[A] or List[Rep[A]]?
  class RepAggregatorOp[A: Manifest](next: RepStreamOp[List[A]]) extends RepStreamOp[A] {
    var list: Rep[List[A]] = List() 
    def onData(data: Rep[A]) = {
      list = data :: list
      next.onData(list)
    }
    def flush = next.flush
  }
  
  class RepSplitMergeOp[A, B: Manifest, C: Manifest](first: RepStreamOp[B] => RepStreamOp[A],
      second: RepStreamOp[C] => RepStreamOp[A], next: RepStreamOp[(B, C)]) extends RepStreamOp[A] {
    val (firstZip, secondZip) = RepStreamFunctions.zipWith(next)
    val (firstStream, secondStream) = (first(firstZip), second(secondZip))
      
    def onData(data: Rep[A]) = {
      firstStream.onData(data)
      secondStream.onData(data)
    }
    
    def flush = {
      firstStream.flush
      secondStream.flush
    }
  }
  
  /*
  class RepMultiSplitOp[A, B](num: Int, streams: (RepStreamOp[B], Int) => RepStreamOp[A], next: RepStreamOp[List[Rep[B]]]) extends RepStreamOp[A] {
    val zippedStreams = StreamFunctions.multiZipWith(num, next).zipWithIndex.map(x => streams(x._1, x._2))
  
    def onData(data: Rep[A]) = {
      zippedStreams foreach {_.onData(data)}
    }
    def flush = { zippedStreams.foreach(_.flush) }
  }
  
  */
  
  object RepStreamFunctions {
  
    // TODO Map in LMS?
/*    def equiJoin[A, B, K] (keyFunA: A => K, keyFunB: B => K, next: StreamOp[List[(A, B)]]): (StreamOp[A], StreamOp[B]) = {
      val aMap = new scala.collection.mutable.HashMap[K, List[A]]
      val bMap = new scala.collection.mutable.HashMap[K, List[B]]
  
      class JoinOp[C](map: scala.collection.mutable.HashMap[K, List[C]], keyFun: C => K) extends StreamOp[C] {
        def onData(data: C) = {
          val key = keyFun(data)
          map += ((key, data :: (map.get(key) match {
            case None => Nil
            case Some(list) => list
          })))
          (aMap.get(key), bMap.get(key)) match {
            case (Some(as), Some(bs)) => next.onData(for (a <- as; b <- bs) yield (a, b))
            case _ =>
          }
        }
        
        def flush = {
          map.clear
          next.flush // Will flush next twice if both input streams are flushed
        }
      }
      
      (new JoinOp(aMap, keyFunA), new JoinOp(bMap, keyFunB))
    }
    */
    
    
    //TODO use regular scala.collections.mutable.Queue like OffsetOp?
    def zipWith[A: Manifest, B: Manifest] (next: RepStreamOp[(A, B)]): (RepStreamOp[A], RepStreamOp[B]) = {
      var leftBuffer: Var[List[A]] = var_new(List[A]())
      var rightBuffer: Var[List[B]] = var_new(List[B]())
      var leftWaitingForFlush = false
      var rightWaitingForFlush = false
    
    val left = new RepStreamOp[A] {
      def onData(data: Rep[A]) = {
        if (!leftWaitingForFlush) {
          if (rightBuffer.isEmpty) {
            leftBuffer = leftBuffer ++ List(data)
          } else {
            val right = rightBuffer.head
            rightBuffer = rightBuffer.tail
            next.onData((data, right))
          }
        }
      }
      
      def flush = {
        leftBuffer = List()
        if (leftWaitingForFlush) {
          leftWaitingForFlush = false
            next.flush
        } else {
          rightWaitingForFlush = true
        }
      }
    }
    
    val right = new RepStreamOp[B] {
      def onData(data: Rep[B]) = {
        if (!rightWaitingForFlush) {
          if (leftBuffer.isEmpty) {
            rightBuffer = rightBuffer ++ List(data)
          } else {
            val left = leftBuffer.head
            leftBuffer = leftBuffer.tail
            next.onData((left, data))
          }
        }
      }
  
      def flush = {
        rightBuffer = List()
        if (rightWaitingForFlush) {
          rightWaitingForFlush = false
            next.flush
        } else {
          leftWaitingForFlush = true
        }
      }
      } 
      
      (left, right)
    }
    
      // TODO better to use List[A] or List[Rep[A]]?
    def multiZipWith[A: Manifest] (num: Int, next: RepStreamOp[List[A]]): List[RepStreamOp[A]] = num match {
      case x if x <= 0 => Nil
      case 1 => new RepMapOp((x: Rep[A]) => List(x), next) :: Nil
      case 2 => {
        val (a, b) = zipWith[A, A](new RepMapOp({x => List(x._1, x._2)}, next))
        a :: b :: Nil
      }
      case x => {
        val (a, b) = zipWith[List[A], List[A]](new RepMapOp({x => x._1 ++ x._2}, next))
        multiZipWith(x / 2 + x % 2, a) ::: multiZipWith(x / 2, b)
      }
    }
  }
  /* TODO next:  StreamInput/Output */
  
// ----------
  abstract class RepStreamOutput[A] extends RepStreamOp[A] {
    def flush = {}
  }
  class RepPrintOp[A]() extends RepStreamOutput[A] {
    def onData(data: Rep[A]) = println(data)
  }
  
  object RepStream {
    def apply[A] = new RepStream[A,A] {
      def into(out: RepStreamOp[A]): RepStreamOp[A] = out
    }
  }
  
  abstract class RepStream[A,B] { self =>
    def into(out: RepStreamOp[B]): RepStreamOp[A]
    def into[C](next: RepStream[B, C]): RepStream[A, C] = new RepStream[A, C] {
      def into(out: RepStreamOp[C]) = self.into(next.into(out))
    }
    
    def print = into(new RepPrintOp[B]())
    
    def filter(p: Rep[B] => Rep[Boolean]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepFilterOp(p, out))
    }
    def map[C](f: Rep[B] => Rep[C]) = new RepStream[A,C] {
      def into(out: RepStreamOp[C]) = self.into(new RepMapOp(f, out))
    }
  }
}

trait BooleanOpsExpOpt extends BooleanOpsExp {
  override def boolean_negate(lhs: Exp[Boolean])(implicit pos: SourceContext) : Exp[Boolean] = lhs match {
    case Const(b) => unit(!b)
    case _ => super.boolean_negate(lhs)
  }
  override def boolean_and(lhs: Exp[Boolean], rhs: Exp[Boolean])(implicit pos: SourceContext) : Exp[Boolean] = (lhs, rhs) match {
    case (Const(true), b) => b
    case (Const(false), b) => unit(false)
    case (b, Const(true)) => b
    case (b, Const(false)) => unit(false)
    case _ => super.boolean_and(lhs, rhs)
  }
  override def boolean_or(lhs: Exp[Boolean], rhs: Exp[Boolean])(implicit pos: SourceContext) : Exp[Boolean] = (lhs, rhs) match {
    case (Const(true), b) => unit(true)
    case (Const(false), b) => b
    case (b, Const(true)) => unit(true)
    case (b, Const(false)) => b
    case _ => super.boolean_or(lhs, rhs)
  }
}

trait ListOpsExpOpt2 extends ListOpsExpOpt {
  override def list_isEmpty[A:Manifest](xs: Exp[List[A]])(implicit pos: SourceContext) : Exp[Boolean] = xs match {
    case Def(ListNew(Seq())) => unit(true)
    case Def(ListNew(_)) => unit(false)
    case _ => super.list_isEmpty(xs)
  }
  override def list_head[A:Manifest](xs: Exp[List[A]])(implicit pos: SourceContext): Exp[A] = xs match {
    case Def(ListNew(Seq(y, _*))) => y
    case _ => super.list_head(xs)
  }
  //TODO get types right
  override def list_tail[A:Manifest](xs: Exp[List[A]])(implicit pos: SourceContext): Exp[List[A]] = xs match {
    case Def(ListNew(Seq(y, ys@_*))) => list_new(ys)
    case _ => super.list_tail(xs)
  } 
}

trait RepStreamOpsExp extends RepStreamOps 
    with IfThenElseExp with IfThenElseExpOpt with BooleanOpsExp with BooleanOpsExpOpt with EqualExpBridge
    with MiscOpsExp with ListOpsExp with ListOpsExpOpt with ListOpsExpOpt2 with TupleOpsExp with VariablesExp with VariablesExpOpt

trait ScalaGenRepStreamOps extends ScalaGenIfThenElse with ScalaGenMiscOps with ScalaGenBooleanOps
    with ScalaGenListOps with ScalaGenTupleOps with ScalaGenVariables
{
  val IR: RepStreamOpsExp
//  import IR._
//  
//  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
//    case _ => { Predef.println("sym: " + sym + ", node: " + node.toString); super.emitNode(sym, node) }
//  }

}


