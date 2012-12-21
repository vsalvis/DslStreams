package streams

import scala.io.Source

object DBToaster {
  def main(args: Array[String]) {
    val printIntermediate = false
    val filename = "lineitem_tiny.csv"
    val numTrials = 1

    // DBToaster, update on new data, no need to store old data
    val timing1 = new Array[Long](numTrials)
    for (i <- 0 to numTrials - 1) {
	    val start = System.currentTimeMillis
	    toastUpdateOnNew(printIntermediate, filename)
	    timing1(i) = System.currentTimeMillis - start
	    println("--------" + timing1(i) + "------------")
    }

    // Naive, aggregate all and recompute on new data
    val timing2 = new Array[Long](numTrials)
    for (i <- 0 to numTrials - 1) {
	    val start2 = System.currentTimeMillis
	    toastAggregateAndRecompute(printIntermediate, filename)
	    timing2(i) = System.currentTimeMillis - start2
	    println("--------" + timing2(i) + "------------")
    }
    
    print((timing1 reduce ((x, y) => x + y)) / numTrials + ":  ")
    timing1.toList foreach (x => print(x + " "))
    println
    print((timing2 reduce ((x, y) => x + y)) / numTrials + ":  ")
    timing2.toList foreach (x => print(x + " "))
    println

    // Timing:
    // Tiny:
    // Update: 
    // 58:  208 136 47 32 72 20 17 25 18 14
    // 60:  199 137 39 38 85 25 22 21 15 19 
    // 85:  231 243 72 47 112 34 33 28 25 31 
    // Recompute:
    // 58:  167 44 58 42 40 28 33 53 56 59
    // 65:  239 48 57 46 36 28 37 49 59 58 
    // 66:  223 48 61 51 35 27 36 58 61 65 

    // Standard:
    // Update: 
    // 111:  538 174 138 55 36 36 39 35 32 33
    // 114:  543 157 173 66 31 29 31 42 34 35
    // 118:  562 172 137 68 32 36 30 38 55 53
    // Recompute:
    // 1140:  1281 1138 1223 1100 1082 1110 1088 1095 1153 1138
    // 1237:  1285 1172 1389 1409 1148 1293 1117 1245 1224 1097
    // 1449:  1301 1120 1211 1116 1102 1220 3405 1634 1096 1285

    // Big:
    // Update: 
    // 
    // 
    // 
    // Recompute:
    // 
    // 
    // 
    
    // Use MapOp and ReduceOp instead of combined LineItemListToResultOp
    val timing1b = new Array[Long](numTrials)
    for (i <- 0 to numTrials - 1) {
	    val start = System.currentTimeMillis
	    toastUpdateMapReduce(printIntermediate, filename)
	    timing1b(i) = System.currentTimeMillis - start
	    println("--------" + timing1b(i) + "------------")
    }
    println
    print((timing1b reduce ((x, y) => x + y)) / numTrials + ":  ")
    timing1b.toList foreach (x => print(x + " "))
    println
    
    // Use SplitOp instead of combined LineItemListToResultOp
    val timing1c = new Array[Long](numTrials)
    for (i <- 0 to numTrials - 1) {
	    val start = System.currentTimeMillis
	    toastUpdateSplit(printIntermediate, filename)
	    timing1c(i) = System.currentTimeMillis - start
	    println("--------" + timing1c(i) + "------------")
    }
    println
    print((timing1c reduce ((x, y) => x + y)) / numTrials + ":  ")
    timing1c.toList foreach (x => print(x + " "))
    println

    // Standard Size:
    // 1 :  129:  658 139 140 127 50 49 39 30 29 33 
    // 2 :  1099:  1261 1107 1179 1062 1054 1052 1060 1079 1064 1073 
    // 1b:  35:  60 46 33 28 31 28 31 36 29 29 
    // Run independently:
    // 1b:  129:  504 162 184 134 79 67 60 37 37 31
    //      133:  543 167 159 132 70 81 64 48 41 33
    // 1 :  117:  488 146 154 106 69 62 49 36 32 35 
    //      119:  496 152 145 118 63 66 47 38 33 32 
    // 2 :  1151:  1662 1286 1121 1070 1058 1060 1054 1073 1077 1058 
    //      1287:  1786 1286 1461 1289 1365 1199 1142 1094 1109 1141 
    /*
SELECT returnflag, linestatus, 
  SUM(quantity) AS sum_qty,
  SUM(extendedprice) AS sum_base_price,
  SUM(extendedprice * (1-discount)) AS sum_disc_price,
  SUM(extendedprice * (1-discount)*(1+tax)) AS sum_charge,
  AVG(quantity) AS avg_qty,
  AVG(extendedprice) AS avg_price,
  AVG(discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE shipdate <= DATE('1997-09-01')
GROUP BY returnflag, linestatus;
    
    CREATE STREAM LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    )
  FROM FILE '../../experiments/data/tpch/standard/lineitem.csv'
  LINE DELIMITED CSV (delimiter := '|');

 */
  }
  
  def toastUpdateOnNew(printIntermediate: Boolean, filename: String): Int = {
    val input = new LineItemInput(filename, 
	        new FilterOp(x => x.shipdate <= new Date(1997, 9, 1), 
	            new GroupByOp(x => (x.returnflag, x.linestatus), (x: Pair[Char, Char]) => 
	              new LineItemToResultOp(
	                  new ResultAggregatorOp(
	                      new ResultOutput(printIntermediate))))))
    input.flush
    42
  }
  
  def toastUpdateMapReduce(printIntermediate: Boolean, filename: String): Int = {
    val input = new LineItemInput(filename, 
	        new FilterOp(x => x.shipdate <= new Date(1997, 9, 1), 
	          new GroupByOp(x => (x.returnflag, x.linestatus), (x: Pair[Char, Char]) => 
	            new MapOp[LineItem, Result]((data: LineItem) => new Result(data.returnflag, data.linestatus, 
		            data.quantity, data.extendedprice, data.extendedprice * (1 - data.discount),
				    data.extendedprice * (1 - data.discount) * (1 + data.tax), data.quantity,
				    data.extendedprice, data.discount, 1),
				  new ReduceOp[Result]((old, data) => (new Result(old.returnflag, old.linestatus, old.sum_qty + data.sum_qty,
			          old.sum_base_price + data.sum_base_price, old.sum_disc_price + data.sum_disc_price,
			          old.sum_charge + data.sum_charge,
			          (old.avg_qty * old.count_order + data.avg_qty) / (old.count_order + 1),
			          (old.avg_price * old.count_order + data.avg_price) / (old.count_order + 1),
			          (old.avg_disc * old.count_order + data.avg_disc) / (old.count_order + 1),
			          old.count_order + 1)), 
	                new ResultOutput(printIntermediate))))))
    input.flush
    42
  }
  
  def toastAggregateAndRecompute(printIntermediate: Boolean, filename: String): Int = {
    val input2 = new LineItemInput(filename, 
        new FilterOp(x => x.shipdate <= new Date(1997, 9, 1), 
            new GroupByOp(x => (x.returnflag, x.linestatus), (x: Pair[Char, Char]) =>
              new AggregatorOp(
                  new LineItemListToResultOp(
                      new ResultOutput(printIntermediate))))))
    input2.flush
    42
  }

  class SumOp(next: StreamOp[Double]) extends StreamOp[Double] {
    var sum = 0.0
    def onData(data: Double) = {
      sum += data
      next.onData(sum)
    }
    def flush = {
      sum = 0
      next.flush
    }
  }
  
  class AvgOp(next: StreamOp[Double]) extends StreamOp[Double] {
    var sum = 0.0
    var count = 0
    def onData(data: Double) = {
      sum += data
      count += 1
      next.onData(sum / count)
    }
    def flush = {
      sum = 0
      count = 0
      next.flush
    }
  }
  
  def toastUpdateSplit(printIntermediate: Boolean, filename: String): Int = {
    val input = new LineItemInput(filename, 
	        new FilterOp(x => x.shipdate <= new Date(1997, 9, 1), 
	          new GroupByOp(x => (x.returnflag, x.linestatus), (key: Pair[Char, Char]) => 
	            new SplitMergeOp((out: StreamOp[Pair[Char, Char]]) => new MapOp((x: LineItem) => (x.returnflag, x.linestatus), out), 
	                (zipList: StreamOp[List[Double]]) => new MultiSplitOp[LineItem, Double](8, 
	                    (zipped: StreamOp[Double], i: Int) => i match {
	                      case 0 => new MapOp({_.quantity}, new SumOp(zipped))
	                      case 1 => new MapOp({_.extendedprice}, new SumOp(zipped))
	                      case 2 => new MapOp({x => x.extendedprice * (1 - x.discount)}, new SumOp(zipped))
	                      case 3 => new MapOp({x => x.extendedprice * (1 - x.discount) * (1 + x.tax)}, new SumOp(zipped))
	                      case 4 => new MapOp({_.quantity}, new AvgOp(zipped))
	                      case 5 => new MapOp({_.extendedprice}, new AvgOp(zipped))
	                      case 6 => new MapOp({_.discount}, new AvgOp(zipped))
	                      case 7 => new MapOp({x => 1.0}, new SumOp(zipped))
	                    }, zipList),
	                new MapOp((x: Pair[Pair[Char, Char], List[Double]]) => new Result(x._1._1, x._1._2, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7).toInt), new ResultOutput(printIntermediate))))))
    
    input.flush
    42
  }
  
  class LineItem(val orderkey: Int, val partkey: Int, val suppkey: Int, val linenumber: Int,
      val quantity: Double, val extendedprice: Double, val discount: Double, val tax: Double,
      val returnflag: Char, val linestatus: Char, val shipdate: Date, val commitdate: Date,
      val receiptdate: Date, val shipinstruct: String, val shipmode: String, val comment: String) {}
  
  class Date(val year: Int, val month: Int, val day: Int) {
    def <=(other: Date) = {
      year < other.year || (year == other.year && (month < other.month || (month == other.month && (day <= other.day))))
    }
  }
  
  class Result(val returnflag: Char, val linestatus: Char, val sum_qty: Double, val sum_base_price: Double,
      val sum_disc_price: Double, val sum_charge: Double, val avg_qty: Double, val avg_price: Double,
      val avg_disc: Double, val count_order: Int) {
    override def toString() = (returnflag + ", " + linestatus + ", " + sum_qty + ", " + sum_base_price + ", " 
        + sum_disc_price + ", " + sum_charge + ", " + avg_qty + ", " + avg_price + ", "
        + avg_disc + ", " + count_order)
  }

  class LineItemInput(filename: String, next: StreamOp[LineItem]) extends StreamInput[LineItem](next) {
//    next.onData(createLineItem("1|156|4|1|17|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the"))
//    next.onData(createLineItem("1|156|4|1|17|17954.55|0.04|0.02|M|O|1998-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the"))
//    next.onData(createLineItem("1|156|4|1|7777|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the"))

    Source.fromFile("data/" + filename).getLines foreach (x => next.onData(createLineItem(x))) 
    
    def createLineItem(line: String): LineItem = {
      val values = line.split('|')
      new LineItem(values(0).toInt, values(1).toInt, values(2).toInt, values(3).toInt, values(4).toDouble,
          values(5).toDouble, values(6).toDouble, values(7).toDouble, values(8)(0), values(9)(0), parseDate(values(10)),
          parseDate(values(11)), parseDate(values(12)), values(13), values(14), values(15))
    }
    
    def parseDate(date: String): Date = {
      val values = date.split("-")
      new Date(values(0).toInt, values(1).toInt, values(2).toInt)
    }
    
    def flush = next.flush
  }
  
  object ResultOutput {
    var instanceCtr = 0
  }
  
  class ResultOutput(printIntermediate: Boolean) extends StreamOutput[Result] {
    val instance = ResultOutput.instanceCtr
    ResultOutput.instanceCtr += 1
    var ctr = 0
    var lastResult: Result = null
    
    def onData(data: Result) = {
      if (printIntermediate) {
        println(instance + "/" + ctr + ": " + data.returnflag + ", " + data.linestatus + ", " + data.sum_qty + ", " + data.sum_base_price + ", " 
          + data.sum_disc_price + ", " + data.sum_charge + ", " + data.avg_qty + ", " + data.avg_price + ", "
          + data.avg_disc + ", " + data.count_order)
      }
      ctr += 1
      lastResult = data
    }
    
    override def flush = println(lastResult.toString)
  }
  
  class LineItemToResultOp(next: StreamOp[Result]) extends StreamOp[LineItem] {
    def onData(data: LineItem) = next.onData(new Result(data.returnflag, data.linestatus, data.quantity, data.extendedprice, 
          data.extendedprice * (1-data.discount), data.extendedprice * (1-data.discount) * (1 + data.tax),
          data.quantity, data.extendedprice, data.discount, 1))
    def flush = next.flush
  }
  
  class LineItemListToResultOp(next: StreamOp[Result]) extends StreamOp[List[LineItem]] {
    def onData(data: List[LineItem]) = next.onData(new Result(data(0).returnflag, data(0).linestatus, 
        data.map(_.quantity).reduce((x, y) => x + y),
        data.map(_.extendedprice).reduce((x, y) => x + y),
        data.map(x => x.extendedprice * (1 - x.discount)).reduce((x, y) => x + y),
        data.map(x => x.extendedprice * (1 - x.discount) * (1 + x.tax)).reduce((x, y) => x + y),
        data.map(_.quantity).reduce((x, y) => x + y) / data.size,
        data.map(_.extendedprice).reduce((x, y) => x + y) / data.size,
        data.map(_.discount).reduce((x, y) => x + y) / data.size,
        data.size))
    def flush = next.flush
  }
  
  
  class ResultAggregatorOp(next: StreamOp[Result]) extends StreamOp[Result] {
    var last: Option[Result] = None
    def onData(data: Result) = { 
      last match {
        case None => last = Some(data)
        case Some(old) => last = Some(new Result(old.returnflag, old.linestatus, old.sum_qty + data.sum_qty,
            old.sum_base_price + data.sum_base_price, old.sum_disc_price + data.sum_disc_price,
            old.sum_charge + data.sum_charge,
            (old.avg_qty * old.count_order + data.avg_qty) / (old.count_order + 1),
            (old.avg_price * old.count_order + data.avg_price) / (old.count_order + 1),
            (old.avg_disc * old.count_order + data.avg_disc) / (old.count_order + 1),
            old.count_order + 1))
      }
      next.onData(last.get)
    }
    def flush = next.flush
  }
}