package dsl

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
import streams._

trait RepStreamOps extends IfThenElse with MiscOps with BooleanOps
   with Variables with ListOps with EmbeddedControls with TupleOps
   with HashMapOps with StaticData with ArrayOps with CastingOps
   with OrderingOps with NumericOps with Equal with Effects with Functions
   with While with IterableOps with MathOps {
  
  abstract class RepStreamOp[A: Manifest] {
    def onData(data: Rep[A]): Rep[Unit]
    def flush: Rep[Unit]
    def collectState(): Rep[List[Any]]
    def setState(l: Rep[List[Any]]): Rep[Unit]
  }
  
  abstract class StatelessRepStreamOp[A: Manifest, B: Manifest](nextS: RepStreamOp[B]) extends RepStreamOp[A] {
    def collectState: Rep[List[Any]] = nextS.collectState
    def setState(l: Rep[List[Any]]) = nextS.setState(l)
  }
  
  class RepMapOp[A: Manifest, B: Manifest](f: Rep[A] => Rep[B], next: RepStreamOp[B])(implicit pos: SourceContext) extends StatelessRepStreamOp[A, B](next) {
    def onData(data: Rep[A]) = next.onData(f(data))
    def flush = next.flush
  }
  
  class RepFilterOp[A: Manifest](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A])(implicit pos: SourceContext) extends StatelessRepStreamOp[A, A](next) {
    def onData(data: Rep[A]) = if (p(data)) next.onData(data)
    def flush = next.flush
  }

  class RepReduceOp[A: Manifest](f: (Rep[A], Rep[A]) => Rep[A], next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val resultState = new Array[A](1)
    val initState = new Array[Boolean](1)
    initState(0) = true
    
    def collectState: Rep[List[Any]] =
        (staticData(resultState))(manifest[Array[A]])(unit(0)) :: 
        (staticData(initState))(manifest[Array[Boolean]])(unit(0)) :: 
        next.collectState
    def setState(s: Rep[List[Any]]) = {
      val res: Rep[A] = rep_asinstanceof[Any, A](s.head, manifest[Any], manifest[A])
      val tail0: Rep[List[Any]] = s.tail
      val init: Rep[Boolean] = rep_asinstanceof[Any, Boolean](tail0.head, manifest[Any], manifest[Boolean])
      val tail: Rep[List[Any]] = tail0.tail
      (staticData(resultState))(manifest[Array[A]])(unit(0)) = res
      (staticData(initState))(manifest[Array[Boolean]])(unit(0)) = init
      next.setState(tail)
    }

    def onData(data: Rep[A]) = {
      val resultStateR: Rep[Array[A]] = staticData(resultState)
      val initStateR: Rep[Array[Boolean]] = staticData(initState)
      
      if (initStateR(unit(0))) {
        initStateR(unit(0)) = unit(false)
        resultStateR(unit(0)) = data
      } else {
        val result = f(resultStateR(unit(0)), data)
        resultStateR(unit(0)) = result
        next.onData(result)
      }
    }
    
    def flush = {
      val initStateR: Rep[Array[Boolean]] = staticData(initState)
      initStateR(unit(0)) = unit(true)
      next.flush
    }
  }

  class RepFoldOp[A: Manifest, B: Manifest](f: (Rep[A], Rep[B]) => Rep[B], z: B, next: RepStreamOp[B])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[B](1)
    state(0) = z
  
    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[B]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[B] = rep_asinstanceof[Any, B](s.head, manifest[Any], manifest[B])
      (staticData(state))(manifest[Array[B]])(unit(0)) = head
      next.setState(s.tail)
    }
  
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[B]] = staticData(state)
      val result = f(data, stateR(unit(0)))
      stateR(unit(0)) = result
      next.onData(result)
    }
    
    def flush = {
      val stateR: Rep[Array[B]] = staticData(state)
      stateR(unit(0)) = unit(z)
      next.flush
    }
  }
  
  class RepAddFoldOp[A: Manifest, B: Manifest](f: (Rep[A], Rep[B]) => Rep[B], z: B, a: Rep[A], next: RepStreamOp[B])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[B](1)
    state(0) = z
  
    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[B]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[B] = rep_asinstanceof[Any, B](s.head, manifest[Any], manifest[B])
      (staticData(state))(manifest[Array[B]])(unit(0)) = head
      next.setState(s.tail)
    }
  
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[B]] = staticData(state)
      val result = f(a, f(data, stateR(unit(0))))
      stateR(unit(0)) = result
      next.onData(result)
    }
    
    def flush = {
      val stateR: Rep[Array[B]] = staticData(state)
      stateR(unit(0)) = unit(z)
      next.flush
    }
  }
  
  class RepFlatMapOp[A: Manifest, B: Manifest](f: Rep[A] => List[Rep[B]], next: RepStreamOp[B])(implicit pos: SourceContext) extends StatelessRepStreamOp[A, B](next) {
    def onData(data: Rep[A]) = f(data) foreach next.onData
  
    def flush = next.flush
  }

  class RepDropOp[A: Manifest](n: Int, next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[Int](1)
    state(0) = n

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[Int]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[Int] = rep_asinstanceof[Any, Int](s.head, manifest[Any], manifest[Int])
      (staticData(state))(manifest[Array[Int]])(unit(0)) = head
      next.setState(s.tail)
    }

    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[Int]] = staticData(state)
      val num: Rep[Int] = stateR(unit(0))
      if (num > unit(0)) {
        stateR(unit(0)) = num - unit(1)
      } else {
        next.onData(data)
      }
    }
    
    def flush = {
      val stateR: Rep[Array[Int]] = staticData(state)
      stateR(unit(0)) = unit(n)
      next.flush
    }
  }

  class RepDropWhileOp[A: Manifest](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[Boolean](1)
    state(0) = true

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[Boolean]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[Boolean] = rep_asinstanceof[Any, Boolean](s.head, manifest[Any], manifest[Boolean])
      (staticData(state))(manifest[Array[Boolean]])(unit(0)) = head
      next.setState(s.tail)
    }
    
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      stateR(unit(0)) = stateR(unit(0)) && p(data)
      if (!stateR(unit(0))) {
        next.onData(data)
      }
    }
    
    def flush = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      stateR(unit(0)) = unit(true)
      next.flush
    }
  }
  
  class RepTakeOp[A: Manifest](n: Int, next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[Int](1)
    state(0) = n

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[Int]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[Int] = rep_asinstanceof[Any, Int](s.head, manifest[Any], manifest[Int])
      (staticData(state))(manifest[Array[Int]])(unit(0)) = head
      next.setState(s.tail)
    }

    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[Int]] = staticData(state)
      val num: Rep[Int] = stateR(unit(0))
      if (num > unit(0)) {
        stateR(unit(0)) = num - unit(1)
        next.onData(data)
      }
    }
    
    def flush = {
      val stateR: Rep[Array[Int]] = staticData(state)
      stateR(unit(0)) = unit(n)
      next.flush
    }
  }

  class RepTakeWhileOp[A: Manifest](p: Rep[A] => Rep[Boolean], next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[Boolean](1)
    state(0) = true

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[Boolean]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[Boolean] = rep_asinstanceof[Any, Boolean](s.head, manifest[Any], manifest[Boolean])
      (staticData(state))(manifest[Array[Boolean]])(unit(0)) = head
      next.setState(s.tail)
    }
    
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      stateR(unit(0)) = stateR(unit(0)) && p(data)
      if (stateR(unit(0))) {
        next.onData(data)
      }
    }
    
    def flush = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      stateR(unit(0)) = unit(true)
      next.flush
    }
  }

  class RepPrependOp[A: Manifest](list: List[Rep[A]], next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[Boolean](1)
    state(0) = false

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[Boolean]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[Boolean] = rep_asinstanceof[Any, Boolean](s.head, manifest[Any], manifest[Boolean])
      (staticData(state))(manifest[Array[Boolean]])(unit(0)) = head
      next.setState(s.tail)
    }
    
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      if (!stateR(unit(0))) {
        stateR(unit(0)) = unit(true)
        list foreach next.onData
      }
      next.onData(data)
    }
    
    def flush = {
      val stateR: Rep[Array[Boolean]] = staticData(state)
      stateR(unit(0)) = unit(false)
      next.flush
    }
  }
  
  class RepOffsetOp[A: Manifest](n: Int, next: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val bufferState = new Array[A](n)
    val fullState = new Array[Boolean](1)
    val nextState = new Array[Int](1)
    fullState(0) = false
    nextState(0) = 0
    
    def collectState: Rep[List[Any]] =
        (staticData(fullState))(manifest[Array[Boolean]])(unit(0)) :: 
        (staticData(nextState))(manifest[Array[Int]])(unit(0)) :: 
        list_fromseq((staticData(bufferState))(manifest[Array[A]]).toSeq) ::
        next.collectState
    def setState(s: Rep[List[Any]]) = {
      val full: Rep[Boolean] = rep_asinstanceof[Any, Boolean](s.head, manifest[Any], manifest[Boolean])
      val tail0: Rep[List[Any]] = s.tail
      val nextI: Rep[Int] = rep_asinstanceof[Any, Int](tail0.head, manifest[Any], manifest[Int])
      val tail1: Rep[List[Any]] = tail0.tail
      val buffer: Rep[List[A]] = rep_asinstanceof[Any, List[A]](tail1.head, manifest[Any], manifest[List[A]])
      
      (staticData(fullState))(manifest[Array[Boolean]])(unit(0)) = full
      (staticData(nextState))(manifest[Array[Int]])(unit(0)) = nextI
      array_copy(list_toarray(buffer), unit(0), (staticData(bufferState))(manifest[Array[A]]), unit(0), unit(n))
      next.setState(tail1.tail)
    }
  
    def onData(data: Rep[A]) = {
      val bufferStateR: Rep[Array[A]] = staticData(bufferState)
      val fullStateR: Rep[Array[Boolean]] = staticData(fullState)
      val nextStateR: Rep[Array[Int]] = staticData(nextState)
      
      if (!fullStateR(unit(0))) {
        val nextI = nextStateR(unit(0))
        bufferStateR.update(nextI, data)
        nextStateR(unit(0)) = (nextI + unit(1))
        if (nextI == unit(n - 1)) {
          fullStateR(unit(0)) = unit(true)
          nextStateR(unit(0)) = unit(0)
        }
      } else {
        val nextI = nextStateR(unit(0))
        next.onData(bufferStateR(nextI))
        bufferStateR.update(nextI, data)
        val nextI2 = (nextI + unit(1))
        nextStateR(unit(0)) = nextI2 - unit(n) * (nextI2 / unit(n)) 
      }
    }
    
    def flush = {
      val fullStateR: Rep[Array[Boolean]] = staticData(fullState)
      val nextStateR: Rep[Array[Int]] = staticData(nextState)
      fullStateR(unit(0)) = unit(false)
      nextStateR(unit(0)) = unit(0)
      next.flush
    }
  }

  class RepGroupByOp[A: Manifest, B: Manifest](keyFun: Rep[A] => Rep[B], streamOpFun: Rep[B] => RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val streamOpState = new HashMap[B, List[Any]]()
    val emptyState = new Array[List[Any]](1)

    def collectState: Rep[List[Any]] =
      List((staticData(emptyState))(manifest[Array[List[Any]]])(unit(0)),
           (staticData(streamOpState))(manifest[HashMap[B, List[Any]]]))
    def setState(s: Rep[List[Any]]) = {
      val empty: Rep[List[Any]] = rep_asinstanceof[Any, List[Any]](s.head, manifest[Any], manifest[List[Any]])
      (staticData(emptyState))(manifest[Array[List[Any]]])(unit(0)) = empty
      val map: Rep[HashMap[B, List[Any]]] = (staticData(streamOpState))(manifest[HashMap[B, List[Any]]])
      map.clear
      val newMap: Rep[HashMap[B, List[Any]]] = rep_asinstanceof[Any, HashMap[B, List[Any]]](s.head, manifest[Any], manifest[HashMap[B, List[Any]]])
      newMap.keys.foreach{ key => map.update(key, newMap(key)) }
    }
    
    def onData(data: Rep[A]) = {
      val map: Rep[HashMap[B, List[Any]]] = (staticData(streamOpState))(manifest[HashMap[B, List[Any]]])
      val key: Rep[B] = keyFun(data)
      val next = streamOpFun(key)
      if (map.size == unit(0)) {
        val empty = next.collectState
        (staticData(emptyState))(manifest[Array[List[Any]]])(unit(0)) = empty 
        next.setState(empty)
      } else {
        if (map.contains(key)) {
          next.setState(map(key))
        } else {
          next.setState((staticData(emptyState))(manifest[Array[List[Any]]])(unit(0)))
        }
      }
      next.onData(data)
      map.update(key, next.collectState)
    }
  
    def flush = {
      val map: Rep[HashMap[B, List[Any]]] = (staticData(streamOpState))(manifest[HashMap[B, List[Any]]])
      map.keys.foreach { key =>
        val stream = streamOpFun(key)
        stream.setState(map(key))
        stream.flush
        map.update(key, stream.collectState)
      }
    }
  }

  class RepGroupByStreamOp[A: Manifest, B: Manifest](keyFun: Rep[A] => Rep[B], next: RepStreamOp[HashMap[B, List[A]]])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[scala.collection.mutable.HashMap[B, List[A]]](1)
    state(0) = new scala.collection.mutable.HashMap[B, List[A]]()
    
    def collectState: Rep[List[Any]] =
      (staticData(state))(manifest[Array[scala.collection.mutable.HashMap[B, List[A]]]])(unit(0)) ::
      next.collectState
    def setState(s: Rep[List[Any]]) = {
      val map: Rep[scala.collection.mutable.HashMap[B, List[A]]] = rep_asinstanceof[Any, scala.collection.mutable.HashMap[B, List[A]]](s.head, manifest[Any], manifest[scala.collection.mutable.HashMap[B, List[A]]])
      (staticData(state))(manifest[Array[scala.collection.mutable.HashMap[B, List[A]]]])(unit(0)) = map
      next.setState(s.tail)
    }
    
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[HashMap[B, List[A]]]] = staticData(state)
      val map: Rep[HashMap[B, List[A]]] = stateR(unit(0))

      val key = keyFun(data)
      if (map.contains(key)) { // why does hashmap_update result in "write to non-mutable Sym..."?
        hashmap_unsafe_update(map, key, data :: map(key))
      } else {
        hashmap_unsafe_update(map, key, List(data))
      }
      stateR(unit(0)) = map
      next.onData(map)
    }
    
    def flush = {
      val stateR: Rep[Array[HashMap[B, List[A]]]] = staticData(state)
      val map: Rep[HashMap[B, List[A]]] = stateR(unit(0))
      hashmap_unsafe_clear(map)
      stateR(unit(0)) = map
      next.flush
    }
  }

  class RepDuplicateOp[A: Manifest](next1: RepStreamOp[A], next2: RepStreamOp[A])(implicit pos: SourceContext) extends RepStreamOp[A] {
    def collectState: Rep[List[Any]] = {
      val l1: Rep[List[Any]] = next1.collectState
      val l2: Rep[List[Any]] = next2.collectState
      List((l1, l2))
    }
    def setState(s: Rep[List[Any]]) = {
      val lists: Rep[(List[Any], List[Any])] = rep_asinstanceof[Any, (List[Any], List[Any])](s.head, manifest[Any], manifest[(List[Any], List[Any])])
      next1.setState(lists._1)
      next2.setState(lists._2)
    } 

    def onData(data: Rep[A]) = {
      next1.onData(data)
      next2.onData(data)
    }
    
    def flush = {
      next1.flush
      next2.flush
    }
  }

  class RepAggregatorOp[A: Manifest](next: RepStreamOp[List[A]])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val state = new Array[List[A]](1)
    state(0) = Nil

    def collectState: Rep[List[Any]] = (staticData(state))(manifest[Array[List[A]]])(unit(0)) :: next.collectState
    def setState(s: Rep[List[Any]]) = {
      val head: Rep[List[A]] = rep_asinstanceof[Any, List[A]](s.head, manifest[Any], manifest[List[A]])
      (staticData(state))(manifest[Array[List[A]]])(unit(0)) = head
      next.setState(s.tail)
    }
  
    def onData(data: Rep[A]) = {
      val stateR: Rep[Array[List[A]]] = staticData(state)
      
      var list: Rep[List[A]] = stateR(unit(0))
      list = data :: list
      
      stateR(unit(0)) = list
      
      next.onData(list)
    }
  
    def flush = {
      val stateR: Rep[Array[List[A]]] = staticData(state)
      stateR(unit(0)) = List()
      next.flush
    }
  }
/*
  // TODO persist state. Naive way isn't working because need complete staging of RepStreamOps.
//  class RepSplitMergeOp[A: Manifest, B: Manifest, C: Manifest](first: RepStreamOp[B] => RepStreamOp[A],
//      second: RepStreamOp[C] => RepStreamOp[A], next: RepStreamOp[(B, C)])(implicit pos: SourceContext) extends RepStreamOp[A] {
//    val (firstZip, secondZip) = RepStreamFunctions.zipWith(next)
//    val state = new Array[RepStreamOp[A]](2)
//    state(0) = first(firstZip)
//    state(1) = second(secondZip)
//      
//    def onData(data: Rep[A]) = {
//      val stateR: Rep[Array[RepStreamOp[A]]] = staticData(state)
//      rep_onData(stateR(unit(0)), data)
//      rep_onData(stateR(unit(1)), data)
//    }
//    
//    def flush = {
//      val stateR: Rep[Array[RepStreamOp[A]]] = staticData(state)
//      rep_flush(stateR(unit(0)))
//      rep_flush(stateR(unit(1)))
//    }
//  }
  class RepSplitMergeOp[A: Manifest, B: Manifest, C: Manifest](first: RepStreamOp[B] => RepStreamOp[A],
      second: RepStreamOp[C] => RepStreamOp[A], next: RepStreamOp[(B, C)])(implicit pos: SourceContext) extends RepStreamOp[A] {
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
  
  // TODO persist state
  class RepMultiSplitOp[A: Manifest, B: Manifest](num: Int, streams: (RepStreamOp[B], Int) => RepStreamOp[A], next: RepStreamOp[List[B]])(implicit pos: SourceContext) extends RepStreamOp[A] {
    val zippedStreams = RepStreamFunctions.multiZipWith(num, next).zipWithIndex.map(x => streams(x._1, x._2))
  
    def onData(data: Rep[A]) = {
      zippedStreams foreach {_.onData(data)}
    }
    def flush = { zippedStreams.foreach(_.flush) }
  }
  
  
  object RepStreamFunctions {
  
    // TODO again get illegal sharing of mutable objects, on the vars, even after reconstructing the lists.
//    def equiJoin[A: Manifest, B: Manifest, K: Manifest] (keyFunA: Rep[A] => Rep[K], keyFunB: Rep[B] => Rep[K], 
//        next: RepStreamOp[List[(A, B)]]): (RepStreamOp[A], RepStreamOp[B]) = {
//      val aMap = HashMap[K, List[A]]()
//      val bMap = HashMap[K, List[B]]()
//  
//      class JoinOp[C: Manifest](map: Rep[HashMap[K, List[C]]], keyFun: Rep[C] => Rep[K])(implicit pos: SourceContext) extends RepStreamOp[C] {
//        def onData(data: Rep[C]) = {
//          val key = keyFun(data)
//          if (map.contains(key)) {
//            hashmap_unsafe_update(map, key, data :: map(key))
//          } else {
//            hashmap_unsafe_update(map, key, data :: List())
//          }
//          if (aMap.contains(key) && bMap.contains(key)) {
//            var as: Var[List[A]] = var_new(List())
//            as = as ++ aMap(key)
//            val bsFull = bMap(key)
//            var bs: Var[List[B]] = var_new(List())
//            val nextAgg = new RepAggregatorOp[(A, B)](next)
//            while (!as.isEmpty) {
//              bs = bs ++ bsFull
//              while (!bs.isEmpty) {
//                nextAgg.onData((as.head, bs.head))
//                bs = bs.tail
//              }
//              as = as.tail
//            }
//
//          }
//        }
//        
//        def flush = {
////          map.clear()
//          next.flush // Will flush next twice if both input streams are flushed
//        }
//      }
//      
//      (new JoinOp(aMap, keyFunA), new JoinOp(bMap, keyFunB))
//    }
    
    // TODO state?
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
  
  // ---------- Input/Output ----------
*/
  class RepPrintOp[A: Manifest]()(implicit pos: SourceContext) extends RepStreamOp[A] {
    def onData(data: Rep[A]) = println(data)
    def flush = println(unit("flush"))
    def collectState(): Rep[List[Any]] = List()
    def setState(l: Rep[List[Any]]) = {}
  }

  class RepListInput[A: Manifest](list: Rep[List[A]], stream: RepStreamOp[A]) {
    list.map[A]({x => stream.onData(x); x})
  } 
  
  // API
  
  object RepStream {
    def apply[A: Manifest] = new RepStream[A,A] {
      def into(out: RepStreamOp[A]): RepStreamOp[A] = out
    }
  }
  
  abstract class RepStream[A: Manifest, B: Manifest] { self =>
    def into(out: RepStreamOp[B]): RepStreamOp[A]
    def into[C: Manifest](next: RepStream[B, C]): RepStream[A, C] = new RepStream[A, C] {
      def into(out: RepStreamOp[C]) = self.into(next.into(out))
    }
    
    def print = into(new RepPrintOp[B]())
    
    def aggregate() = new RepStream[A,List[B]] {
      def into(out: RepStreamOp[List[B]]) = self.into(new RepAggregatorOp[B](out))
    }
    def drop(n: Int) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepDropOp(n, out))
    }
    def dropWhile(p: Rep[B] => Rep[Boolean]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepDropWhileOp(p, out))
    }
    def filter(p: Rep[B] => Rep[Boolean]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepFilterOp(p, out))
    }
    def flatMap[C: Manifest](f: Rep[B] => List[Rep[C]]) = new RepStream[A,C] {
      def into(out: RepStreamOp[C]) = self.into(new RepFlatMapOp(f, out))
    }
    def fold[C: Manifest](f: (Rep[B], Rep[C]) => Rep[C], z: C) = new RepStream[A,C] {
      def into(out: RepStreamOp[C]) = self.into(new RepFoldOp(f, z, out))
    }
    def groupByStream[K: Manifest](keyF: Rep[B] => Rep[K]) = new RepStream[A,HashMap[K, List[B]]] {
      def into(out: RepStreamOp[HashMap[K, List[B]]]) = self.into(new RepGroupByStreamOp[B, K](keyF, out))
    }
    def map[C: Manifest](f: Rep[B] => Rep[C]) = new RepStream[A,C] {
      def into(out: RepStreamOp[C]) = self.into(new RepMapOp(f, out))
    }
//    def multiSplit[C: Manifest](num: Int, streams: (RepStreamOp[C], Int) => RepStreamOp[B]) = new RepStream[A, List[C]] {
//      def into(out: RepStreamOp[List[C]]) = self.into(new RepMultiSplitOp(num, streams, out))
//    }
    def offset(n: Int) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepOffsetOp(n, out))
    }
    def prepend(list: List[Rep[B]]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepPrependOp(list, out))
    }
    def reduce(f: (Rep[B], Rep[B]) => Rep[B]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepReduceOp(f, out))
    }
//    def splitMerge[C: Manifest, D: Manifest](first: RepStream[B,C], second: RepStream[B,D]) = new RepStream[A,Pair[C,D]] {
//      def into(out: RepStreamOp[Pair[C,D]]) = self.into(
//          new RepSplitMergeOp({x: RepStreamOp[C] => first into x}, {y: RepStreamOp[D] => second into y}, out))
//    }
    def take(n: Int) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepTakeOp(n, out))
    }
    def takeWhile(p: Rep[B] => Rep[Boolean]) = new RepStream[A,B] {
      def into(out: RepStreamOp[B]) = self.into(new RepTakeWhileOp(p, out))
    }
        
    // special functions: Those are not simple Streams because they have
    // multiple in- or output streams
    
    def duplicate(first: RepStreamOp[B], second: RepStreamOp[B]) = {
      self.into(new RepDuplicateOp(first, second))
    }
    // TODO implement RepStreamOp and then APi
//    def equiJoin[C, D, K](other: Stream[C,D], keyFunThis: B => K, keyFunOther: D => K, next: StreamOp[List[(B,D)]]) = {
//      val (a, b) = StreamFunctions.equiJoin(keyFunThis, keyFunOther, next)
//      (self.into(a), other.into(b))
//    }
    def groupBy[K: Manifest](keyF: Rep[B] => Rep[K], streamF: Rep[K] => RepStreamOp[B]) = {
      self.into(new RepGroupByOp(keyF, streamF))
    }
//    def multiZipWith(num: Int, others: scala.collection.immutable.List[RepStream[A,B]], next: RepStreamOp[List[B]]) = {
//      val list = RepStreamFunctions.multiZipWith(num, next)
//      self.into(list(0)) :: others.zip(list.drop(1)).map({x => x._1.into(x._2)})
//    }
//    def zipWith[C,D: Manifest](other: RepStream[C,D], next: RepStreamOp[Pair[B, D]]) = {
//      val (a, b) = RepStreamFunctions.zipWith(next)
//      (self.into(a), other.into(b))
//    }
  }
  
  def hashmap_unsafe_clear[K: Manifest, V:Manifest](m: Rep[HashMap[K,V]])(implicit pos: SourceContext): Rep[Unit]
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
  override def list_tail[A:Manifest](xs: Exp[List[A]])(implicit pos: SourceContext): Exp[List[A]] = xs match {
    case Def(ListNew(Seq(y, ys@_*))) => list_new(ys)
    case _ => super.list_tail(xs)
  } 
}

trait RepStreamOpsExp extends RepStreamOps 
    with IfThenElseExp with IfThenElseExpOpt with BooleanOpsExp with BooleanOpsExpOpt with EqualExp with EqualExpOpt
    with MiscOpsExp with ListOpsExp with ListOpsExpOpt with ListOpsExpOpt2 with TupleOpsExp with VariablesExp with VariablesExpOpt
    with HashMapOpsExp with StaticDataExp with ArrayOpsExp with CastingOpsExp with OrderingOpsExp with NumericOpsExp
    with EffectExp with FunctionsExp with WhileExp with IterableOpsExp with MathOpsExp {
  
//  case class OnData[A](s: Exp[RepStreamOp[A]], data: Exp[A]) extends Def[RepStreamOp[A]]
//  override def rep_onData[A: Manifest](s: Exp[RepStreamOp[A]], data: Exp[A]) = OnData[A](s, data)
//
//  case class Flush[A](s: Exp[RepStreamOp[A]]) extends Def[RepStreamOp[A]]
//  override def rep_flush[A: Manifest](s: Exp[RepStreamOp[A]]) = Flush[A](s)
  
  // Use StaticData and Array to keep mutable state in the generated StreamOp.
  override def array_apply[T: Manifest](x: Exp[Array[T]], n: Exp[Int])(implicit pos: SourceContext): Exp[T] = reflectWrite(x)(ArrayApply(x, n))
  override def hashmap_unsafe_clear[K: Manifest, V:Manifest](m: Exp[HashMap[K,V]])(implicit pos: SourceContext) = reflectEffect(HashMapClear(m))
}

trait ScalaGenRepStreamOps extends ScalaGenIfThenElse with ScalaGenMiscOps with ScalaGenBooleanOps with ScalaGenEqual
    with ScalaGenListOps with ScalaGenTupleOps with ScalaGenVariables with ScalaGenHashMapOps with ScalaGenStaticData
    with ScalaGenArrayOps with ScalaGenCastingOps with ScalaGenOrderingOps with ScalaGenNumericOps with ScalaGenEffect
    with ScalaGenFunctions with ScalaGenWhile with ScalaGenIterableOps with ScalaGenMathOps
{
  val IR: RepStreamOpsExp
  import IR._

//  override def emitNode(sym: Sym[Any], node: Def[Any]): Unit = node match {
//    case OnData(s, data) => {
//      emitValDef(sym, quote(s) + ".onData(" + quote(data) + ")")
//    }
//    case Flush(s) => {
//      emitValDef(sym, quote(s) + ".flush()")
//    }
//    case _ => super.emitNode(sym, node)
//  }
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

