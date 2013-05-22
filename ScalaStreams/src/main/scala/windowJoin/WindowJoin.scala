package windowJoin

import java.lang.Thread
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ArrayBlockingQueue
import java.util.AbstractQueue
import scala.collection.mutable.Queue

object SoccerWindowJoin {
  
  abstract class LinkedThread[T, U](private val leftInQ: ConcurrentLinkedQueue[Option[T]], private val leftOutQ: ConcurrentLinkedQueue[Option[U]],
     private val rightInQ: ConcurrentLinkedQueue[Option[U]], private val rightOutQ: ConcurrentLinkedQueue[Option[T]]) extends Thread {
    
    protected def rcvLeft: Option[T] = leftInQ.poll
    protected def sendRight(data: Option[T]) = rightOutQ.add(data)

    protected def rcvRight: Option[U] = rightInQ.poll
    protected def sendLeft(data: Option[U]) = leftOutQ.add(data)

    
    def getLoad(sender: LinkedThread[T, U]): Int
  }
  
  class CoordinatorThread[T, U](leftInQ2: ConcurrentLinkedQueue[Option[T]], leftOutQ2: ConcurrentLinkedQueue[Option[U]],
     rightInQ2: ConcurrentLinkedQueue[Option[U]], rightOutQ2: ConcurrentLinkedQueue[Option[T]], windowSize: Int) 
     extends LinkedThread[T, U](leftInQ2, leftOutQ2, rightInQ2, rightOutQ2) {
    var left: LinkedThread[T, U] = null
    var right: LinkedThread[T, U] = null
    
    val waiting = new Queue[Either[T, U]]
    var inTransitL2R = 0
    var inTransitR2L = 0
    
    def leftOnData(data: T): Unit = {
      waiting.enqueue(Left(data))
    }
    def rightOnData(data: U): Unit = {
      waiting.enqueue(Right(data))
    }
    
    def getLoad(sender: LinkedThread[T, U]): Int = 0
    
    override def run = {
      while (true) {
        if (waiting.length > 0) { 
          waiting.front match {
            case Left(toRight) =>
              if (inTransitL2R == windowSize) {
                rcvRight match {
                  case Some(r) =>
                    sendRight(None)
                    sendRight(Some(toRight))
                    waiting.dequeue
                  case None => sys.error("coordinator shouldn't receive ACKs from the right.")
                  case null =>
                }
              } else {
                sendRight(Some(toRight))
                waiting.dequeue
                inTransitL2R += 1
              }

            case Right(toLeft) =>
              if (inTransitR2L == windowSize) {
                rcvLeft match {
                  case None =>
                    inTransitR2L += 1
                  case Some(l) =>
                    sendLeft(None)
                    sendLeft(Some(toLeft))
                    waiting.dequeue
                    inTransitR2L -= 1
                  case null =>
                }
              } else {
                sendLeft(Some(toLeft))
                waiting.dequeue
              }
              
            case _ =>
          }
        }
      }
    }
  }
  
  class WorkerThread[T, U](leftInQ2: ConcurrentLinkedQueue[Option[T]], leftOutQ2: ConcurrentLinkedQueue[Option[U]],
     rightInQ2: ConcurrentLinkedQueue[Option[U]], rightOutQ2: ConcurrentLinkedQueue[Option[T]],
     outQ: AbstractQueue[(T, U)], left: LinkedThread[T, U], p: (T, U) => Boolean) 
     extends LinkedThread[T, U](leftInQ2, leftOutQ2, rightInQ2, rightOutQ2) {
    
    def isRightmost = right.isInstanceOf[CoordinatorThread[T, U]]
    var right: LinkedThread[T, U] = null
    val queueL2R = new Queue[T]
    val queueR2L = new Queue[U]
    var r2lForwarded: Int = 0 // the number of last elements that are marked as fw but not confirmed yet
    var l2rForwarded: Int = 0 // the number of last elements that are marked as fw but not confirmed yet
    
    private def forwardTuples: Unit = {
      if (queueL2R.length > right.getLoad(this)) {
        if (isRightmost && queueL2R.length > l2rForwarded) {
          sendRight(queueL2R.get(l2rForwarded))
          l2rForwarded += 1
        } else {
          sendRight(Some(queueL2R.dequeue))
        }
      }
      if (queueR2L.length > left.getLoad(this) && queueR2L.length > r2lForwarded) {
        sendLeft(queueR2L.get(r2lForwarded))
        r2lForwarded += 1
      }
    }    
    
    def getLoad(sender: LinkedThread[T, U]): Int = {
      if(sender == left) {
        queueR2L.length
      } else {
        queueL2R.length
      }
    } 
    
    override def run = {
      while (true) {
        rcvLeft match {
          case None => 
            queueR2L.dequeue
            r2lForwarded -= 1
          case Some(l) => 
            queueR2L.foreach(r => if (p(l, r)) outQ.add((l, r)))
            queueL2R.enqueue(l)
          case null =>
        }
        rcvRight match {
          case None if (isRightmost) => 
            queueL2R.dequeue
            l2rForwarded -= 1
          case Some(r) =>
            queueL2R.foreach(l => if (p(l, r)) outQ.add((l, r)))
            queueR2L.enqueue(r)
            sendRight(None)
          case null =>
        }
        forwardTuples
      }
    }
  }
  
  class PrintThread[T](queue: ArrayBlockingQueue[T]) extends Thread {
    override def run = {
      while (true) {
        println(queue.take)
      }
    }
  }

  
  def main(args: Array[String]) {
    val nWorkers = 4
    val windowSize = 8
    type T = Int
    type U = Double
    def pred(t: T, u: U): Boolean = {
      t % 3 == u % 2
    } 

    val leftInQ = new ConcurrentLinkedQueue[Option[T]]()
    val leftOutQ = new ConcurrentLinkedQueue[Option[U]]()
    val rightInQ = new ConcurrentLinkedQueue[Option[U]]()
    val rightOutQ = new ConcurrentLinkedQueue[Option[T]]()
    val outQ = new ArrayBlockingQueue[(T,U)](100)
    
    
    val coordinator = new CoordinatorThread[T, U](leftInQ, leftOutQ, rightInQ, rightOutQ, windowSize)
    coordinator.right = linkWorkers[T, U](nWorkers, rightOutQ, rightInQ, leftOutQ, leftInQ, outQ, coordinator, coordinator, pred)
    coordinator.start
    new PrintThread[(T, U)](outQ).start()
    
    List.range(0, 50, 1).foreach{n =>
      coordinator.leftOnData(n)
      coordinator.rightOnData(n)
    }
  }
  
  def linkWorkers[T, U](nWorkers: Int, leftInQ: ConcurrentLinkedQueue[Option[T]], leftOutQ: ConcurrentLinkedQueue[Option[U]],
     rightInQ: ConcurrentLinkedQueue[Option[U]], rightOutQ: ConcurrentLinkedQueue[Option[T]], 
     outQ: AbstractQueue[(T, U)], prev: LinkedThread[T, U], coordinator: CoordinatorThread[T, U],
     p: (T, U) => Boolean): LinkedThread[T, U] = nWorkers match {
    case 0 => 
      val t = new WorkerThread[T, U](leftInQ, leftOutQ, rightInQ, rightOutQ, outQ, prev, p)
      t.right = coordinator
      coordinator.left = t
      t.start
      t
    case n if n > 0 => 
      val nRightInQ = new ConcurrentLinkedQueue[Option[U]]()
      val nRightOutQ = new ConcurrentLinkedQueue[Option[T]]()
      val t = new WorkerThread[T, U](leftInQ, leftOutQ, nRightInQ, nRightOutQ, outQ, prev, p)
      t.right = linkWorkers[T, U](n - 1, nRightOutQ, nRightInQ, rightInQ, rightOutQ, outQ, t, coordinator, p)
      t.start
      t
    case _ => throw new IllegalArgumentException("Cannot link a negative number of workers for windowjoin.")
  }

}

class NaiveWindowJoin[T, U](leftInQ: ConcurrentLinkedQueue[T], leftOutQ: ConcurrentLinkedQueue[U],
     rightInQ: ConcurrentLinkedQueue[U], rightOutQ: ConcurrentLinkedQueue[T],
     outQ: AbstractQueue[(T, U)], p: (T, U) => Boolean, windowSize: Int) {
  
  
  
  
}

class NaiveJoinSemantics[T, U] {
  def join[T, U](l1: List[T], l2: List[U], p: ((T, U)) => Boolean, outQ: AbstractQueue[(T, U)]): Unit = {
    (for(i <- l1; j <- l2) yield (i, j)) filter(p) foreach (outQ.add)
  }
}