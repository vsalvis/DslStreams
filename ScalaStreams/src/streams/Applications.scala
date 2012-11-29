package streams


object Applications {
  def main(args: Array[String]) {
    new DBToaster.LineItemInput(
        new MapOp[DBToaster.LineItem, Double](_.quantity,
            new FIRFilterOp(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp)
        ))
    println("----------")
    new DBToaster.LineItemInput(
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
      val (a, b) = new StreamFunctions().zipWith((y: Double, z: Double) => y + z, after)
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
    case x :: xs => new SplitOp[Double, Double, Double, Double, Double, Double](d => (d, d), 
          a => new MapOp(y => y * x, a),
          b => new PrependOp(0.0 :: Nil, split(xs, b)),
          _ + _, 
          after)
  }
  
  def onData(data: Double) = stream.onData(data)
  
  def flush = stream.flush
}

// API
// LMS
// buffer
// parallel
// signal processing: lowpass etc.
// streamit cookbook examples
// buffers
// parallel