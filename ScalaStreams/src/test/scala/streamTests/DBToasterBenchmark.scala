package streamTests

import org.scalameter.api._
import streams.DBToaster
import org.dbtoaster.dbtoasterlib.K3Collection._
import scala.collection.mutable.Map;
import java.io._


object DBToasterBenchmark extends PerformanceTest.Quickbenchmark {
  val filenames = Gen.enumeration("filename")("lineitem_tiny.csv", "lineitem_standard.csv")

  val toasts = for {
    filename <- filenames
  } yield (DBToaster.toast(Right(filename))(_))

  val printIntermediate = false
  
  performance of "Toast" in {
    measure method "toastUpdateOnNew" in {
      using(toasts) in {
        toast => toast(DBToaster.toastUpdateOnNew(printIntermediate))
      }
    }
    measure method "toastUpdateMapReduce" in {
      using(toasts) in {
        toast => toast(DBToaster.toastUpdateMapReduce(printIntermediate))
      }
    }
    measure method "toastUpdateSplit" in {
      using(toasts) in {
        toast => toast(DBToaster.toastUpdateSplit(printIntermediate))
      }
    }
    measure method "toastAggregateAndRecompute" in {
      using(toasts) in {
        toast => toast(DBToaster.toastAggregateAndRecompute(printIntermediate))
      }
    }
    measure method "originalDBToaster" in {
      using(filenames) in {
        filename => new dbtoaster.Query1(filename).run(_ => ())
      }
    }
  }
}


// Benchmark with buffered files (benchmark doesn't include file IO)
//::Benchmark Toast.toastUpdateOnNew::
//Parameters(filename -> lineitem_tiny.csv): 0.125
//Parameters(filename -> lineitem_standard.csv): 1.126
//::Benchmark Toast.toastUpdateMapReduce::
//Parameters(filename -> lineitem_tiny.csv): 0.129
//Parameters(filename -> lineitem_standard.csv): 1.108
//::Benchmark Toast.toastUpdateSplit::
//Parameters(filename -> lineitem_tiny.csv): 0.762
//Parameters(filename -> lineitem_standard.csv): 6.44
//::Benchmark Toast.toastAggregateAndRecompute::
//Parameters(filename -> lineitem_tiny.csv): 11.33
//Parameters(filename -> lineitem_standard.csv): 1124.761