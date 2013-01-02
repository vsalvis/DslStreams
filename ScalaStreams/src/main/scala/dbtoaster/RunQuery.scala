import org.dbtoaster.dbtoasterlib.K3Collection._
import scala.collection.mutable.Map;
import java.io._

package dbtoaster {
  object RunQuery {
    def main(args: Array[String]) {
      var counter = 0
      val timeStart = System.currentTimeMillis()
      val trace = args.contains("--trace")
    
      val fw = if(trace) {
        new BufferedWriter(new FileWriter("trace.txt"))
      } else null

      val fn:Unit => Unit = if(trace) {
        _ => {
          counter += 1
          if(counter % 1000 == 0) {
            fw.write(counter + ", " + (System.currentTimeMillis() - timeStart))
            fw.newLine()
            print(".")
          }
        }
      } else (_ => ())
      val query1 = new Query1("lineitem_tiny.csv")
      query1.run(fn)
      
      val runtime = (System.currentTimeMillis() - timeStart) / 1000.0
      if(args.contains("--time-only")) {
        println("" + runtime)
      }
      else {
        println("Run time: " + runtime + " s")
        query1.printResults()
      }

      if(trace) 
        fw.close();
    }
  }
}
