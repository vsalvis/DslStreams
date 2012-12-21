package streams


object Applications {
  def main(args: Array[String]) {
    val filename = "lineitem_tiny.csv"
    new ListInput(DBToaster.generateLineItemsFromFile(filename),
        new MapOp[DBToaster.LineItem, Double](_.quantity,
            new FIRFilterOp(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp)
        ))
    println("----------")
    new ListInput(DBToaster.generateLineItemsFromFile(filename),
        new MapOp[DBToaster.LineItem, Double](_.quantity,
            new FIRFilterOpPrepend(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp)
        ))
    println("----------")
    new ListInput[Double](1.0 :: 1.0 :: 1.0 :: 1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil, new FIRFilterOp(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp))
    println("----------")
    new ListInput[Double](1.0 :: 1.0 :: 1.0 :: 1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil, new FIRFilterOpPrepend(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp))
    println("----------")
    new ListInput[Double](1.0 :: 1.0 :: 1.0 :: 1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil, new FIRFilterOpSplit(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp))
  }
}


class FIRFilterOp(coefficients: List[Double], next: StreamOp[Double]) extends StreamOp[Double] {
  var values: List[Double] = Nil
  def onData(data: Double) = {
    values = data :: values.take(coefficients.size - 1)
    next.onData(coefficients.zip(values).map(z => z._1 * z._2).reduce((x, y) => x + y))
  }
  
  def flush = {
    values = Nil
    next.flush
  }
}

class FIRFilterOpPrepend(coefficients: List[Double], next: StreamOp[Double]) extends StreamOp[Double] {
  val stream = split(coefficients, next)

  def split(list: List[Double], after: StreamOp[Double]): StreamOp[Double] = list match {
    case Nil => after
    case x :: Nil => new MapOp((y: Double) => y * x, after)
    case x :: xs => {
      val (a, b) = StreamFunctions.zipWith(new MapOp((x: Pair[Double, Double]) => x._1 + x._2, after))
      new DuplicateOp(new MapOp((y: Double) => y * x, a), new PrependOp(0.0 :: Nil, split(xs, b)))
    }
  }
  
  def onData(data: Double) = stream.onData(data)
  
  def flush = stream.flush
}

class FIRFilterOpSplit(coefficients: List[Double], next: StreamOp[Double]) extends StreamOp[Double] {
  val stream = split(coefficients, next)

  def split(list: List[Double], after: StreamOp[Double]): StreamOp[Double] = list match {
    case Nil => after
    case x :: Nil => new MapOp((y: Double) => y * x, after)
    case x :: xs => new SplitMergeOp[Double, Double, Double](a => new MapOp(y => y * x, a),
          b => new PrependOp(0.0 :: Nil, split(xs, b)),
          new MapOp({x => x._1 + x._2}, after))
  }
  
  def onData(data: Double) = stream.onData(data)
  
  def flush = stream.flush
}

// Filters inspired by StreamIt examples from http://groups.csail.mit.edu/cag/streamit/papers/streamit-cookbook.pdf

class LowPassFilterOp(rate: Double, cutoff: Double, taps: Int, decimation: Int, next: StreamOp[Double])
    extends FIRFilterOp(
        (for (i <- 0 to taps - 1; m = taps - 1; pi = scala.math.Pi) yield {
        	if (i == m / 2)
        	  2 * cutoff / rate
        	else
        	  scala.math.sin(2*pi*cutoff/rate*(i-m/2)) / pi / (i-m/2) * (0.54 - 0.46 * scala.math.cos(2*pi*i/m))
        }).toList, next) {}

class BandPassFilterOp(rate: Double, low: Double, high: Double, taps: Int, next: StreamOp[Double])
    extends SplitMergeOp({mergeOp: StreamOp[Double] => new LowPassFilterOp(rate, low, taps, 0, mergeOp)},
        {mergeOp: StreamOp[Double] => new LowPassFilterOp(rate, high, taps, 0, mergeOp)},
        new MapOp({(x: Pair[Double, Double]) => x._2 - x._1}, next)) {}


// ok: signal processing: lowpass etc. from streamit cookbook examples
// ok: API
// DBToaster query benchmark
// LMS
// buffer
// parallel
