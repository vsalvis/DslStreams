package streamTests

import org.scalameter.api._
import streams.StreamDBToaster._
import scala.collection.mutable.Map;
import java.io._


object DBToasterBenchmark extends PerformanceTest.Quickbenchmark {
  val filenames = Gen.enumeration("filename")("lineitem_0200.csv", "lineitem_0400.csv", "lineitem_0600.csv", "lineitem_0800.csv",
      "lineitem_1000.csv", "lineitem_2000.csv", "lineitem_3000.csv", "lineitem_4000.csv", "lineitem_5000.csv", "lineitem_6000.csv")    
      //"lineitem_tiny.csv", "lineitem_standard.csv", "lineitem_big.csv")

  val toasts = for {
    filename <- filenames
  } yield (toast(Right(filename))(_))

  val printIntermediate = false
  
  performance of "Toast" in {
    measure method "toastUpdateOnNew" in {
      using(toasts) in {
        toast => toast(toastUpdateOnNew(printIntermediate))
      }
    }
    measure method "toastUpdateMapReduce" in {
      using(toasts) in {
        toast => toast(toastUpdateMapReduce(printIntermediate))
      }
    }
    measure method "toastUpdateSplit" in {
      using(toasts) in {
        toast => toast(toastUpdateSplit(printIntermediate))
      }
    }
    measure method "toastAggregateAndRecompute" in {
      using(toasts) in {
        toast => toast(toastAggregateAndRecompute(printIntermediate))
      }
    }
    measure method "originalDBToaster" in {
      using(filenames) in {
        filename => new dbtoaster.DBToasterQuery1(filename).run(_ => ())
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
//
// Real benchmark including file IO
//::Benchmark Toast.toastUpdateOnNew::
//Parameters(filename -> lineitem_tiny.csv): 2.256
//Parameters(filename -> lineitem_standard.csv): 22.992
//::Benchmark Toast.toastUpdateMapReduce::
//Parameters(filename -> lineitem_tiny.csv): 2.269
//Parameters(filename -> lineitem_standard.csv): 23.085
//::Benchmark Toast.toastUpdateSplit::
//Parameters(filename -> lineitem_tiny.csv): 3.001
//Parameters(filename -> lineitem_standard.csv): 29.2
//::Benchmark Toast.toastAggregateAndRecompute::
//Parameters(filename -> lineitem_tiny.csv): 14.38
//Parameters(filename -> lineitem_standard.csv): 1319.169
//::Benchmark Toast.originalDBToaster::
//Parameters(filename -> lineitem_tiny.csv): 47.592
//Parameters(filename -> lineitem_standard.csv): 500.104