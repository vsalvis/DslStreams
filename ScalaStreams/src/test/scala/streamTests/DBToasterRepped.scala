//package streamTests
//
//import dsl._
//import streams._
//import scala.virtualization.lms.common._
//import scala.virtualization.lms.common.Functions
//import scala.virtualization.lms.internal.ScalaCompile
//import scala.virtualization.lms.util.OverloadHack
//import java.io.PrintWriter
//
//trait DBToasterRepStreamProg extends RepStreamOps {
//  def toastUpdateOnNew(): RepStreamOp[Int] = {
//    new RepReduceOp[Int]({(x, y) => x + y}, new RepPrintOp[Int])
//  }
//  def test2(): RepStreamOp[Int] = {
//    new RepMapOp[Int, Int]({x: Rep[Int] => x * unit(2)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepMapOp[Int, Int]({x: Rep[Int] => x + unit(1)}, new RepFilterOp[Int]({x: Rep[Int] => x > unit(4)}, new RepPrintOp[Int]))))
//  }
//}
//
//class TestRepStreamOps2 {
//  def repToastUpdateOnNew(): StreamOp[Int] = {
//    new RepStreamProg2 with RepStreamCompile { self =>
//      compileStream(toastUpdateOnNew())
//    }
//  }
//  def repToastUpdateMapReduce(): StreamOp[Int] = {
//    new RepStreamProg2 with RepStreamCompile { self =>
//      compileStream(toastUpdateMapReduce())
//    }
//  }
//  def repToastUpdateSplit(): StreamOp[Int] = {
//    new RepStreamProg2 with RepStreamCompile { self =>
//      compileStream(toastUpdateSplit())
//    }
//  }
//}
//
//object DBToasterRepped {
//
//  def toastUpdateOnNew(printIntermediate: Boolean) = {
//    new FilterOp((x: LineItem) => x.shipdate <= new Date(1997, 9, 1), 
//        new GroupByOp((x: LineItem) => (x.returnflag, x.linestatus), (x: Pair[Char, Char]) => 
//          new LineItemToResultOp(
//              new ResultAggregatorOp(
//                  new ResultOutput(printIntermediate)))))
//  }
//    
//  
//  def toastUpdateMapReduce(printIntermediate: Boolean) = {
//    new FilterOp((x: LineItem) => x.shipdate <= new Date(1997, 9, 1), 
//      new GroupByOp((x: LineItem) => (x.returnflag, x.linestatus), (x: Pair[Char, Char]) => 
//        new MapOp[LineItem, Result]((data: LineItem) => new Result(data.returnflag, data.linestatus, 
//            data.quantity, data.extendedprice, data.extendedprice * (1 - data.discount),
//        data.extendedprice * (1 - data.discount) * (1 + data.tax), data.quantity,
//        data.extendedprice, data.discount, 1),
//      new ReduceOp[Result]((old, data) => (new Result(old.returnflag, old.linestatus, old.sum_qty + data.sum_qty,
//            old.sum_base_price + data.sum_base_price, old.sum_disc_price + data.sum_disc_price,
//            old.sum_charge + data.sum_charge,
//            (old.avg_qty * old.count_order + data.avg_qty) / (old.count_order + 1),
//            (old.avg_price * old.count_order + data.avg_price) / (old.count_order + 1),
//            (old.avg_disc * old.count_order + data.avg_disc) / (old.count_order + 1),
//            old.count_order + 1)), 
//            new ResultOutput(printIntermediate)))))
//  }
//
//    class SumOp(next: StreamOp[Double]) extends StreamOp[Double] {
//    var sum = 0.0
//    def onData(data: Double) = {
//      sum += data
//      next.onData(sum)
//    }
//    def flush = {
//      sum = 0
//      next.flush
//    }
//  }
//  
//  class AvgOp(next: StreamOp[Double]) extends StreamOp[Double] {
//    var sum = 0.0
//    var count = 0
//    def onData(data: Double) = {
//      sum += data
//      count += 1
//      next.onData(sum / count)
//    }
//    def flush = {
//      sum = 0
//      count = 0
//      next.flush
//    }
//  }
//  
//  def toastUpdateSplit(printIntermediate: Boolean) = {
//    new FilterOp((x: LineItem) => x.shipdate <= new Date(1997, 9, 1), 
//      new GroupByOp((x: LineItem) => (x.returnflag, x.linestatus), (key: Pair[Char, Char]) => 
//        new SplitMergeOp((out: StreamOp[Pair[Char, Char]]) => new MapOp((x: LineItem) => (x.returnflag, x.linestatus), out), 
//            (zipList: StreamOp[List[Double]]) => new MultiSplitOp[LineItem, Double](8, 
//                (zipped: StreamOp[Double], i: Int) => i match {
//                  case 0 => new MapOp({_.quantity}, new SumOp(zipped))
//                  case 1 => new MapOp({_.extendedprice}, new SumOp(zipped))
//                  case 2 => new MapOp({x => x.extendedprice * (1 - x.discount)}, new SumOp(zipped))
//                  case 3 => new MapOp({x => x.extendedprice * (1 - x.discount) * (1 + x.tax)}, new SumOp(zipped))
//                  case 4 => new MapOp({_.quantity}, new AvgOp(zipped))
//                  case 5 => new MapOp({_.extendedprice}, new AvgOp(zipped))
//                  case 6 => new MapOp({_.discount}, new AvgOp(zipped))
//                  case 7 => new MapOp({x => 1.0}, new SumOp(zipped))
//                }, zipList),
//            new MapOp((x: Pair[Pair[Char, Char], List[Double]]) => new Result(x._1._1, x._1._2, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7).toInt), new ResultOutput(printIntermediate)))))
//  }
//  
//  class LineItem(val orderkey: Int, val partkey: Int, val suppkey: Int, val linenumber: Int,
//      val quantity: Double, val extendedprice: Double, val discount: Double, val tax: Double,
//      val returnflag: Char, val linestatus: Char, val shipdate: Date, val commitdate: Date,
//      val receiptdate: Date, val shipinstruct: String, val shipmode: String, val comment: String) {}
//  
//  class Date(val year: Int, val month: Int, val day: Int) {
//    def <=(other: Date) = {
//      year < other.year || (year == other.year && (month < other.month || (month == other.month && (day <= other.day))))
//    }
//  }
//  
//  class Result(val returnflag: Char, val linestatus: Char, val sum_qty: Double, val sum_base_price: Double,
//      val sum_disc_price: Double, val sum_charge: Double, val avg_qty: Double, val avg_price: Double,
//      val avg_disc: Double, val count_order: Int) {
//    override def toString() = (returnflag + ", " + linestatus + ", " + sum_qty + ", " + sum_base_price + ", " 
//        + sum_disc_price + ", " + sum_charge + ", " + avg_qty + ", " + avg_price + ", "
//        + avg_disc + ", " + count_order)
//  }
//  
//  object ResultOutput {
//    var instanceCtr = 0
//  }
//  
//  class ResultOutput(printIntermediate: Boolean) extends StreamOutput[Result] {
//    val instance = ResultOutput.instanceCtr
//    ResultOutput.instanceCtr += 1
//    var ctr = 0
//    var lastResult: Result = null
//    
//    def onData(data: Result) = {
//      if (printIntermediate) {
//        println(instance + "/" + ctr + ": " + data.returnflag + ", " + data.linestatus + ", " + data.sum_qty + ", " + data.sum_base_price + ", " 
//          + data.sum_disc_price + ", " + data.sum_charge + ", " + data.avg_qty + ", " + data.avg_price + ", "
//          + data.avg_disc + ", " + data.count_order)
//      }
//      ctr += 1
//      lastResult = data
//    }
//    
//    override def flush = {
//      if (printIntermediate) 
//        println(lastResult.toString)
//      else 
//        print(".")
//    }
//  }
//  
//  class LineItemToResultOp(next: StreamOp[Result]) extends StreamOp[LineItem] {
//    def onData(data: LineItem) = next.onData(new Result(data.returnflag, data.linestatus, data.quantity, data.extendedprice, 
//          data.extendedprice * (1-data.discount), data.extendedprice * (1-data.discount) * (1 + data.tax),
//          data.quantity, data.extendedprice, data.discount, 1))
//    def flush = next.flush
//  }
//  
//  class LineItemListToResultOp(next: StreamOp[Result]) extends StreamOp[List[LineItem]] {
//    def onData(data: List[LineItem]) = next.onData(new Result(data(0).returnflag, data(0).linestatus, 
//        data.map(_.quantity).reduce((x, y) => x + y),
//        data.map(_.extendedprice).reduce((x, y) => x + y),
//        data.map(x => x.extendedprice * (1 - x.discount)).reduce((x, y) => x + y),
//        data.map(x => x.extendedprice * (1 - x.discount) * (1 + x.tax)).reduce((x, y) => x + y),
//        data.map(_.quantity).reduce((x, y) => x + y) / data.size,
//        data.map(_.extendedprice).reduce((x, y) => x + y) / data.size,
//        data.map(_.discount).reduce((x, y) => x + y) / data.size,
//        data.size))
//    def flush = next.flush
//  }
//  
//  class ResultAggregatorOp(next: StreamOp[Result]) extends StreamOp[Result] {
//    var last: Option[Result] = None
//    def onData(data: Result) = { 
//      last match {
//        case None => last = Some(data)
//        case Some(old) => last = Some(new Result(old.returnflag, old.linestatus, old.sum_qty + data.sum_qty,
//            old.sum_base_price + data.sum_base_price, old.sum_disc_price + data.sum_disc_price,
//            old.sum_charge + data.sum_charge,
//            (old.avg_qty * old.count_order + data.avg_qty) / (old.count_order + 1),
//            (old.avg_price * old.count_order + data.avg_price) / (old.count_order + 1),
//            (old.avg_disc * old.count_order + data.avg_disc) / (old.count_order + 1),
//            old.count_order + 1))
//      }
//      next.onData(last.get)
//    }
//    def flush = next.flush
//  }