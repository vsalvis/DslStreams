package streams


object Applications {
  def main(args: Array[String]) {
    new DBToaster.LineItemInput(
        new MapOp[DBToaster.LineItem, Double](_.quantity,
            new FIRFilterOp(1.0 :: 2.0 :: 3.0 :: Nil, new PrintlnOp)
        ))
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