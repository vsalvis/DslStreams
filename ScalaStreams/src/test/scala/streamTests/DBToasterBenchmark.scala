package streamTests

import org.scalameter.api._
import streams.DBToaster

object DBToasterBenchmark extends PerformanceTest.Quickbenchmark {
  val filenames = Gen.enumeration("filename")("lineitem_tiny.csv", "lineitem_standard.csv")

  val lineitems = for {
    filename <- filenames
  } yield DBToaster.generateLineItemsFromFile(filename)

  val printIntermediate = false
  
  performance of "Toast" in {
    measure method "toastUpdateOnNew" in {
      using(lineitems) in {
        l => DBToaster.toastUpdateOnNew(printIntermediate, l)
      }
    }
    measure method "toastUpdateMapReduce" in {
      using(lineitems) in {
        l => DBToaster.toastUpdateMapReduce(printIntermediate, l)
      }
    }
    measure method "toastUpdateSplit" in {
      using(lineitems) in {
        l => DBToaster.toastUpdateSplit(printIntermediate, l)
      }
    }
    measure method "toastAggregateAndRecompute" in {
      using(lineitems) in {
        l => DBToaster.toastAggregateAndRecompute(printIntermediate, l)
      }
    }
  }
}

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