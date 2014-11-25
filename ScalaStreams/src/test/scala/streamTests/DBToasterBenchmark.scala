package streamTests

import org.scalameter.api._
import streams.StreamDBToaster._
import scala.collection.mutable.Map;
import java.io._


object DBToasterBenchmark extends PerformanceTest.Quickbenchmark {
  val filenames = Gen.enumeration("filename")("lineitem_0200.csv", "lineitem_0400.csv", "lineitem_0600.csv", "lineitem_0800.csv",
      "lineitem_1000.csv", "lineitem_2000.csv", "lineitem_3000.csv", "lineitem_4000.csv", "lineitem_5000.csv", "lineitem_6000.csv",    
      "lineitem_7000.csv", "lineitem_8000.csv", "lineitem_9000.csv")    
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
    
//    measure method "toastAggregateAndRecompute" in {
//      using(toasts) in {
//        toast => toast(toastAggregateAndRecompute(printIntermediate))
//      }
//    }
//    measure method "originalDBToaster" in {
//      using(filenames) in {
//        filename => new dbtoaster.DBToasterQuery1(filename).run(_ => ())
//      }
//    }
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

//toastUpdateOnNew
//0.833
//1.645
//2.446
//3.252
//4.012
//7.979
//12.079
//15.999
//19.766
//24.0
//28.224
//32.227
//36.422
//
//toastUpdateMapReduce
//0.827
//1.625
//2.413
//3.198
//3.961
//7.95
//11.851
//15.932
//19.89
//23.692
//28.231
//32.334
//36.397
//
//toastUpdateSplit
//1.11
//2.176
//3.145
//4.152
//5.074
//9.97
//15.133
//20.13
//24.882
//29.851
//35.164
//39.933
//45.189
//
//toastAggregateAndRecompute
//2.177
//7.427
//15.365
//25.143
//37.992
//139.442
//332.16
//602.119
//963.614
//1353.689
//1840.775
//2422.789
//3047.085
//
//originalDBToaster
//17.273
//36.533
//55.909
//74.302
//93.283
//189.469
//281.923
//381.895
//476.027
//576.171
//673.816
//768.207
//861.166