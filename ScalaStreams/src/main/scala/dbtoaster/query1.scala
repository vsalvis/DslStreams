
import java.io.FileInputStream;

import org.dbtoaster.dbtoasterlib.StreamAdaptor._;

import org.dbtoaster.dbtoasterlib.K3Collection._;

import org.dbtoaster.dbtoasterlib.Source._;

import org.dbtoaster.dbtoasterlib.dbtoasterExceptions._;

import org.dbtoaster.dbtoasterlib.ImplicitConversions._;

import org.dbtoaster.dbtoasterlib.StdFunctions._;

import scala.collection.mutable.Map;

import java.util.Date;

import java.util.GregorianCalendar;

import xml._;

package dbtoaster {
  class Query1(filename: String) {
    val s1 = createInputStreamSource(new FileInputStream("data/" + filename), List(new CSVAdaptor("LINEITEM", List(IntColumn,IntColumn,IntColumn,IntColumn,FloatColumn,FloatColumn,FloatColumn,FloatColumn,StringColumn,StringColumn,DateColumn,DateColumn,DateColumn,StringColumn,StringColumn,StringColumn), "", "", "", "\\|")), Delimited("\n"))


    val sources = new SourceMultiplexer(List(s1));

    var NATION = new K3PersistentCollection[Tuple4[Long,String,Long,String], Long](Map(), None) /* out */;

    var REGION = new K3PersistentCollection[Tuple3[Long,String,String], Long](Map(), None) /* out */;

    var SUM_QTY = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var SUM_BASE_PRICE = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var SUM_DISC_PRICE = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var SUM_CHARGE = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_QTY = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_QTY_mLINEITEM1_L3_1 = new K3PersistentCollection[Tuple2[String,String], Long](Map(), None) /* out */;

    var AVG_QTY_mLINEITEM5 = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_PRICE = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_PRICE_mLINEITEM5 = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_DISC = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var AVG_DISC_mLINEITEM5 = new K3PersistentCollection[Tuple2[String,String], Double](Map(), None) /* out */;

    var COUNT_ORDER = new K3PersistentCollection[Tuple2[String,String], Long](Map(), None) /* out */;

    def getSUM_QTY():K3PersistentCollection[Tuple2[String,String], Double] = {
      SUM_QTY
    };

    def getSUM_BASE_PRICE():K3PersistentCollection[Tuple2[String,String], Double] = {
      SUM_BASE_PRICE
    };

    def getSUM_DISC_PRICE():K3PersistentCollection[Tuple2[String,String], Double] = {
      SUM_DISC_PRICE
    };

    def getSUM_CHARGE():K3PersistentCollection[Tuple2[String,String], Double] = {
      SUM_CHARGE
    };

    def getAVG_QTY():K3PersistentCollection[Tuple2[String,String], Double] = {
      AVG_QTY
    };

    def getAVG_PRICE():K3PersistentCollection[Tuple2[String,String], Double] = {
      AVG_PRICE
    };

    def getAVG_DISC():K3PersistentCollection[Tuple2[String,String], Double] = {
      AVG_DISC
    };

    def getCOUNT_ORDER():K3PersistentCollection[Tuple2[String,String], Long] = {
      COUNT_ORDER
    };

    def onInsertLINEITEM(var_LINEITEM_ORDERKEY: Long,var_LINEITEM_PARTKEY: Long,var_LINEITEM_SUPPKEY: Long,var_LINEITEM_LINENUMBER: Long,var_LINEITEM_QUANTITY: Double,var_LINEITEM_EXTENDEDPRICE: Double,var_LINEITEM_DISCOUNT: Double,var_LINEITEM_TAX: Double,var_LINEITEM_RETURNFLAG: String,var_LINEITEM_LINESTATUS: String,var_LINEITEM_SHIPDATE: Date,var_LINEITEM_COMMITDATE: Date,var_LINEITEM_RECEIPTDATE: Date,var_LINEITEM_SHIPINSTRUCT: String,var_LINEITEM_SHIPMODE: String,var_LINEITEM_COMMENT: String) = {
      if((SUM_QTY).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_QUANTITY))) 
      }
      else {
        SUM_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_QUANTITY)) 
      };

      if((SUM_BASE_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_BASE_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_BASE_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        SUM_BASE_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      if((SUM_DISC_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_DISC_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_DISC_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((-1L) * (var_LINEITEM_DISCOUNT)) + (1L))) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        SUM_DISC_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((-1L) * (var_LINEITEM_DISCOUNT)) + (1L))) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      if((SUM_CHARGE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_CHARGE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_CHARGE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((-1L) * (var_LINEITEM_DISCOUNT)) + (1L))) * ((1L) + (var_LINEITEM_TAX))) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        SUM_CHARGE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((-1L) * (var_LINEITEM_DISCOUNT)) + (1L))) * ((1L) + (var_LINEITEM_TAX))) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      ({
        def tb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_7:Double = (AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_7:Double = _var___cse_var_7;
{
                val _var___cse_var_3:Double = (AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_3:Double = _var___cse_var_3;
{
                  val _var___cse_var_1:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                  val var___cse_var_1:Long = _var___cse_var_1;

                  AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_7) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__11) = {
                      val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__10) = {
                        val _var___sql_inline_average_count_1:Long = var___cse_var_1;

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                      };

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      val var___prod_ret__10:Long = _var___prod_ret__10;

                      Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__10) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__11:Double = _var___prod_ret__11;

                    var___prod_ret__11
                  }
                  ) + ({
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__13) = {
                      val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__12) = {
                        val _var___sql_inline_average_count_1:Long = (var___cse_var_1) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                      };

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      val var___prod_ret__12:Long = _var___prod_ret__12;

                      Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__12) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__13:Double = _var___prod_ret__13;

                    var___prod_ret__13
                  }
                  )) + (({
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__15) = {
                      val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__14) = {
                        val _var___sql_inline_average_count_1:Long = var___cse_var_1;

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                      };

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      val var___prod_ret__14:Long = _var___prod_ret__14;

                      Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__14) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__15:Double = _var___prod_ret__15;

                    var___prod_ret__15
                  }
                  ) * (-1L)))) * (var_LINEITEM_QUANTITY)) + ((var___cse_var_3) * (({
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__20) = {
                      val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__19) = {
                        val _var___sql_inline_average_count_1:Long = (var___cse_var_1) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                      };

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      val var___prod_ret__19:Long = _var___prod_ret__19;

                      Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__19) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__20:Double = _var___prod_ret__20;

                    var___prod_ret__20
                  }
                  ) + (({
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__22) = {
                      val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__21) = {
                        val _var___sql_inline_average_count_1:Long = var___cse_var_1;

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                      };

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      val var___prod_ret__21:Long = _var___prod_ret__21;

                      Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__21) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__22:Double = _var___prod_ret__22;

                    var___prod_ret__22
                  }
                  ) * (-1L))))))
                }
              }
            };

            def fb = {
              val _var___cse_var_7:Double = (AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_7:Double = _var___cse_var_7;
{
                val _var___cse_var_3:Double = (AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_3:Double = _var___cse_var_3;

                AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_7) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                )))))) * (var_LINEITEM_QUANTITY)) + ((var___cse_var_3) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                ))))))))
              }
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_7:Double = (AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_7:Double = _var___cse_var_7;
{
                val _var___cse_var_2:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_2:Long = _var___cse_var_2;

                AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_7) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__11) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__10) = {
                      val _var___sql_inline_average_count_1:Long = var___cse_var_2;

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__10:Long = _var___prod_ret__10;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__10) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__11:Double = _var___prod_ret__11;

                  var___prod_ret__11
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__13) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__12) = {
                      val _var___sql_inline_average_count_1:Long = (var___cse_var_2) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__12:Long = _var___prod_ret__12;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__12) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__13:Double = _var___prod_ret__13;

                  var___prod_ret__13
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__15) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__14) = {
                      val _var___sql_inline_average_count_1:Long = var___cse_var_2;

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__14:Long = _var___prod_ret__14;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__14) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__15:Double = _var___prod_ret__15;

                  var___prod_ret__15
                }
                ) * (-1L)))) * (var_LINEITEM_QUANTITY)))
              }
            };

            def fb = {
              val _var___cse_var_7:Double = (AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_7:Double = _var___cse_var_7;

              AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_7) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_QUANTITY)))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_QTY_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        def fb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_6:Double = (AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_6:Double = _var___cse_var_6;
{
                val _var___cse_var_4:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_4:Long = _var___cse_var_4;

                AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__11) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__10) = {
                      val _var___sql_inline_average_count_1:Long = var___cse_var_4;

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__10:Long = _var___prod_ret__10;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__10) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__11:Double = _var___prod_ret__11;

                  var___prod_ret__11
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__13) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__12) = {
                      val _var___sql_inline_average_count_1:Long = (var___cse_var_4) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__12:Long = _var___prod_ret__12;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__12) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__13:Double = _var___prod_ret__13;

                  var___prod_ret__13
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__15) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__14) = {
                      val _var___sql_inline_average_count_1:Long = var___cse_var_4;

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__14:Long = _var___prod_ret__14;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__14) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__15:Double = _var___prod_ret__15;

                  var___prod_ret__15
                }
                ) * (-1L)))) * (var_LINEITEM_QUANTITY)) + ((var___cse_var_6) * (({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__20) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__19) = {
                      val _var___sql_inline_average_count_1:Long = (var___cse_var_4) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__19:Long = _var___prod_ret__19;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__19) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__20:Double = _var___prod_ret__20;

                  var___prod_ret__20
                }
                ) + (({
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__22) = {
                    val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__21) = {
                      val _var___sql_inline_average_count_1:Long = var___cse_var_4;

                      val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                    };

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    val var___prod_ret__21:Long = _var___prod_ret__21;

                    Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__21) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__22:Double = _var___prod_ret__22;

                  var___prod_ret__22
                }
                ) * (-1L)))))
              }
            };

            def fb = {
              val _var___cse_var_6:Double = (AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_6:Double = _var___cse_var_6;

              AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_QUANTITY)) + ((var___cse_var_6) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_5:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_5:Long = _var___cse_var_5;

              AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__11) = {
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__10) = {
                    val _var___sql_inline_average_count_1:Long = var___cse_var_5;

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__10:Long = _var___prod_ret__10;

                  Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__10) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                val var___prod_ret__11:Double = _var___prod_ret__11;

                var___prod_ret__11
              }
              ) + ({
                val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__13) = {
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__12) = {
                    val _var___sql_inline_average_count_1:Long = (var___cse_var_5) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__12:Long = _var___prod_ret__12;

                  Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__12) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                val var___prod_ret__13:Double = _var___prod_ret__13;

                var___prod_ret__13
              }
              )) + (({
                val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__15) = {
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__14) = {
                    val _var___sql_inline_average_count_1:Long = var___cse_var_5;

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__14:Long = _var___prod_ret__14;

                  Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__14) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                val var___prod_ret__15:Double = _var___prod_ret__15;

                var___prod_ret__15
              }
              ) * (-1L)))) * (var_LINEITEM_QUANTITY))
            };

            def fb = AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
              val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

              listmax(arg._1,arg._2)
            }
            )))))) * (var_LINEITEM_QUANTITY));

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_QTY_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        if((AVG_QTY).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_QTY_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_QTY_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_QUANTITY))) 
      }
      else {
        AVG_QTY_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_QUANTITY)) 
      };

      ({
        def tb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_14:Double = (AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_14:Double = _var___cse_var_14;
{
                val _var___cse_var_10:Double = (AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_10:Double = _var___cse_var_10;
{
                  val _var___cse_var_8:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                  val var___cse_var_8:Long = _var___cse_var_8;

                  AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_14) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__27) = {
                      val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__26) = {
                        val _var___sql_inline_average_count_2:Long = var___cse_var_8;

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                      };

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      val var___prod_ret__26:Long = _var___prod_ret__26;

                      Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__26) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__27:Double = _var___prod_ret__27;

                    var___prod_ret__27
                  }
                  ) + ({
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__29) = {
                      val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__28) = {
                        val _var___sql_inline_average_count_2:Long = (var___cse_var_8) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                      };

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      val var___prod_ret__28:Long = _var___prod_ret__28;

                      Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__28) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__29:Double = _var___prod_ret__29;

                    var___prod_ret__29
                  }
                  )) + (({
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__31) = {
                      val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__30) = {
                        val _var___sql_inline_average_count_2:Long = var___cse_var_8;

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                      };

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      val var___prod_ret__30:Long = _var___prod_ret__30;

                      Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__30) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__31:Double = _var___prod_ret__31;

                    var___prod_ret__31
                  }
                  ) * (-1L)))) * (var_LINEITEM_EXTENDEDPRICE)) + ((var___cse_var_10) * (({
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__36) = {
                      val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__35) = {
                        val _var___sql_inline_average_count_2:Long = (var___cse_var_8) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                      };

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      val var___prod_ret__35:Long = _var___prod_ret__35;

                      Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__35) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__36:Double = _var___prod_ret__36;

                    var___prod_ret__36
                  }
                  ) + (({
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__38) = {
                      val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__37) = {
                        val _var___sql_inline_average_count_2:Long = var___cse_var_8;

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                      };

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      val var___prod_ret__37:Long = _var___prod_ret__37;

                      Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__37) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__38:Double = _var___prod_ret__38;

                    var___prod_ret__38
                  }
                  ) * (-1L))))))
                }
              }
            };

            def fb = {
              val _var___cse_var_14:Double = (AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_14:Double = _var___cse_var_14;
{
                val _var___cse_var_10:Double = (AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_10:Double = _var___cse_var_10;

                AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_14) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                )))))) * (var_LINEITEM_EXTENDEDPRICE)) + ((var___cse_var_10) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                ))))))))
              }
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_14:Double = (AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_14:Double = _var___cse_var_14;
{
                val _var___cse_var_9:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_9:Long = _var___cse_var_9;

                AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_14) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__27) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__26) = {
                      val _var___sql_inline_average_count_2:Long = var___cse_var_9;

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__26:Long = _var___prod_ret__26;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__26) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__27:Double = _var___prod_ret__27;

                  var___prod_ret__27
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__29) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__28) = {
                      val _var___sql_inline_average_count_2:Long = (var___cse_var_9) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__28:Long = _var___prod_ret__28;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__28) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__29:Double = _var___prod_ret__29;

                  var___prod_ret__29
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__31) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__30) = {
                      val _var___sql_inline_average_count_2:Long = var___cse_var_9;

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__30:Long = _var___prod_ret__30;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__30) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__31:Double = _var___prod_ret__31;

                  var___prod_ret__31
                }
                ) * (-1L)))) * (var_LINEITEM_EXTENDEDPRICE)))
              }
            };

            def fb = {
              val _var___cse_var_14:Double = (AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_14:Double = _var___cse_var_14;

              AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_14) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_EXTENDEDPRICE)))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_PRICE_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        def fb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_13:Double = (AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_13:Double = _var___cse_var_13;
{
                val _var___cse_var_11:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_11:Long = _var___cse_var_11;

                AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__27) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__26) = {
                      val _var___sql_inline_average_count_2:Long = var___cse_var_11;

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__26:Long = _var___prod_ret__26;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__26) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__27:Double = _var___prod_ret__27;

                  var___prod_ret__27
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__29) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__28) = {
                      val _var___sql_inline_average_count_2:Long = (var___cse_var_11) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__28:Long = _var___prod_ret__28;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__28) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__29:Double = _var___prod_ret__29;

                  var___prod_ret__29
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__31) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__30) = {
                      val _var___sql_inline_average_count_2:Long = var___cse_var_11;

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__30:Long = _var___prod_ret__30;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__30) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__31:Double = _var___prod_ret__31;

                  var___prod_ret__31
                }
                ) * (-1L)))) * (var_LINEITEM_EXTENDEDPRICE)) + ((var___cse_var_13) * (({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__36) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__35) = {
                      val _var___sql_inline_average_count_2:Long = (var___cse_var_11) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__35:Long = _var___prod_ret__35;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__35) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__36:Double = _var___prod_ret__36;

                  var___prod_ret__36
                }
                ) + (({
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__38) = {
                    val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__37) = {
                      val _var___sql_inline_average_count_2:Long = var___cse_var_11;

                      val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                    };

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    val var___prod_ret__37:Long = _var___prod_ret__37;

                    Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__37) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__38:Double = _var___prod_ret__38;

                  var___prod_ret__38
                }
                ) * (-1L)))))
              }
            };

            def fb = {
              val _var___cse_var_13:Double = (AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_13:Double = _var___cse_var_13;

              AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_EXTENDEDPRICE)) + ((var___cse_var_13) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_12:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_12:Long = _var___cse_var_12;

              AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__27) = {
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__26) = {
                    val _var___sql_inline_average_count_2:Long = var___cse_var_12;

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__26:Long = _var___prod_ret__26;

                  Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__26) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                val var___prod_ret__27:Double = _var___prod_ret__27;

                var___prod_ret__27
              }
              ) + ({
                val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__29) = {
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__28) = {
                    val _var___sql_inline_average_count_2:Long = (var___cse_var_12) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__28:Long = _var___prod_ret__28;

                  Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__28) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                val var___prod_ret__29:Double = _var___prod_ret__29;

                var___prod_ret__29
              }
              )) + (({
                val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__31) = {
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__30) = {
                    val _var___sql_inline_average_count_2:Long = var___cse_var_12;

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__30:Long = _var___prod_ret__30;

                  Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__30) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                val var___prod_ret__31:Double = _var___prod_ret__31;

                var___prod_ret__31
              }
              ) * (-1L)))) * (var_LINEITEM_EXTENDEDPRICE))
            };

            def fb = AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
              val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

              listmax(arg._1,arg._2)
            }
            )))))) * (var_LINEITEM_EXTENDEDPRICE));

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_PRICE_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        if((AVG_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_PRICE_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_PRICE_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        AVG_PRICE_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      ({
        def tb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_21:Double = (AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_21:Double = _var___cse_var_21;
{
                val _var___cse_var_17:Double = (AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_17:Double = _var___cse_var_17;
{
                  val _var___cse_var_15:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                  val var___cse_var_15:Long = _var___cse_var_15;

                  AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_21) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__43) = {
                      val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__42) = {
                        val _var___sql_inline_average_count_3:Long = var___cse_var_15;

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                      };

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      val var___prod_ret__42:Long = _var___prod_ret__42;

                      Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__42) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__43:Double = _var___prod_ret__43;

                    var___prod_ret__43
                  }
                  ) + ({
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__45) = {
                      val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__44) = {
                        val _var___sql_inline_average_count_3:Long = (var___cse_var_15) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                      };

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      val var___prod_ret__44:Long = _var___prod_ret__44;

                      Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__44) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__45:Double = _var___prod_ret__45;

                    var___prod_ret__45
                  }
                  )) + (({
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__47) = {
                      val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__46) = {
                        val _var___sql_inline_average_count_3:Long = var___cse_var_15;

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                      };

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      val var___prod_ret__46:Long = _var___prod_ret__46;

                      Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__46) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__47:Double = _var___prod_ret__47;

                    var___prod_ret__47
                  }
                  ) * (-1L)))) * (var_LINEITEM_DISCOUNT)) + ((var___cse_var_17) * (({
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__52) = {
                      val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__51) = {
                        val _var___sql_inline_average_count_3:Long = (var___cse_var_15) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                      };

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      val var___prod_ret__51:Long = _var___prod_ret__51;

                      Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__51) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__52:Double = _var___prod_ret__52;

                    var___prod_ret__52
                  }
                  ) + (({
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__54) = {
                      val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__53) = {
                        val _var___sql_inline_average_count_3:Long = var___cse_var_15;

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                      };

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      val var___prod_ret__53:Long = _var___prod_ret__53;

                      Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__53) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                        listmax(arg._1,arg._2)
                      }
                      ))))))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__54:Double = _var___prod_ret__54;

                    var___prod_ret__54
                  }
                  ) * (-1L))))))
                }
              }
            };

            def fb = {
              val _var___cse_var_21:Double = (AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_21:Double = _var___cse_var_21;
{
                val _var___cse_var_17:Double = (AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_17:Double = _var___cse_var_17;

                AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_21) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                )))))) * (var_LINEITEM_DISCOUNT)) + ((var___cse_var_17) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                  val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                  listmax(arg._1,arg._2)
                }
                ))))))))
              }
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_21:Double = (AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_21:Double = _var___cse_var_21;
{
                val _var___cse_var_16:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_16:Long = _var___cse_var_16;

                AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_21) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__43) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__42) = {
                      val _var___sql_inline_average_count_3:Long = var___cse_var_16;

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__42:Long = _var___prod_ret__42;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__42) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__43:Double = _var___prod_ret__43;

                  var___prod_ret__43
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__45) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__44) = {
                      val _var___sql_inline_average_count_3:Long = (var___cse_var_16) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__44:Long = _var___prod_ret__44;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__44) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__45:Double = _var___prod_ret__45;

                  var___prod_ret__45
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__47) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__46) = {
                      val _var___sql_inline_average_count_3:Long = var___cse_var_16;

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__46:Long = _var___prod_ret__46;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__46) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__47:Double = _var___prod_ret__47;

                  var___prod_ret__47
                }
                ) * (-1L)))) * (var_LINEITEM_DISCOUNT)))
              }
            };

            def fb = {
              val _var___cse_var_21:Double = (AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_21:Double = _var___cse_var_21;

              AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var___cse_var_21) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_DISCOUNT)))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_DISC_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        def fb = ({
          def tb = ({
            def tb = {
              val _var___cse_var_20:Double = (AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_20:Double = _var___cse_var_20;
{
                val _var___cse_var_18:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

                val var___cse_var_18:Long = _var___cse_var_18;

                AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__43) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__42) = {
                      val _var___sql_inline_average_count_3:Long = var___cse_var_18;

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__42:Long = _var___prod_ret__42;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__42) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__43:Double = _var___prod_ret__43;

                  var___prod_ret__43
                }
                ) + ({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__45) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__44) = {
                      val _var___sql_inline_average_count_3:Long = (var___cse_var_18) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__44:Long = _var___prod_ret__44;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__44) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__45:Double = _var___prod_ret__45;

                  var___prod_ret__45
                }
                )) + (({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__47) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__46) = {
                      val _var___sql_inline_average_count_3:Long = var___cse_var_18;

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__46:Long = _var___prod_ret__46;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__46) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__47:Double = _var___prod_ret__47;

                  var___prod_ret__47
                }
                ) * (-1L)))) * (var_LINEITEM_DISCOUNT)) + ((var___cse_var_20) * (({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__52) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__51) = {
                      val _var___sql_inline_average_count_3:Long = (var___cse_var_18) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__51:Long = _var___prod_ret__51;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__51) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__52:Double = _var___prod_ret__52;

                  var___prod_ret__52
                }
                ) + (({
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__54) = {
                    val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__53) = {
                      val _var___sql_inline_average_count_3:Long = var___cse_var_18;

                      val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                      Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                    };

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    val var___prod_ret__53:Long = _var___prod_ret__53;

                    Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__53) * ((div(({
                      val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                      listmax(arg._1,arg._2)
                    }
                    ))))))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__54:Double = _var___prod_ret__54;

                  var___prod_ret__54
                }
                ) * (-1L)))))
              }
            };

            def fb = {
              val _var___cse_var_20:Double = (AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_20:Double = _var___cse_var_20;

              AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_DISCOUNT)) + ((var___cse_var_20) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
                val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

                listmax(arg._1,arg._2)
              }
              )))))))
            };

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          def fb = ({
            def tb = {
              val _var___cse_var_19:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS));

              val var___cse_var_19:Long = _var___cse_var_19;

              AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((({
                val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__43) = {
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__42) = {
                    val _var___sql_inline_average_count_3:Long = var___cse_var_19;

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__42:Long = _var___prod_ret__42;

                  Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__42) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                val var___prod_ret__43:Double = _var___prod_ret__43;

                var___prod_ret__43
              }
              ) + ({
                val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__45) = {
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__44) = {
                    val _var___sql_inline_average_count_3:Long = (var___cse_var_19) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()));

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__44:Long = _var___prod_ret__44;

                  Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__44) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                val var___prod_ret__45:Double = _var___prod_ret__45;

                var___prod_ret__45
              }
              )) + (({
                val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__47) = {
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__46) = {
                    val _var___sql_inline_average_count_3:Long = var___cse_var_19;

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__46:Long = _var___prod_ret__46;

                  Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__46) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                val var___prod_ret__47:Double = _var___prod_ret__47;

                var___prod_ret__47
              }
              ) * (-1L)))) * (var_LINEITEM_DISCOUNT))
            };

            def fb = AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != ((if((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 1L else 0L))) * ((div(({
              val arg = Tuple2[Long,Boolean]((1L),((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())));

              listmax(arg._1,arg._2)
            }
            )))))) * (var_LINEITEM_DISCOUNT));

            if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
              tb 
            }
            else {
              fb 
            }
          }
          );

          if((AVG_DISC_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
            tb 
          }
          else {
            fb 
          }
        }
        );

        if((AVG_DISC).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_QTY_mLINEITEM1_L3_1.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) 
      }
      else {
        AVG_QTY_mLINEITEM1_L3_1.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 
      };

      if((AVG_DISC_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_DISC_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_DISCOUNT))) 
      }
      else {
        AVG_DISC_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_DISCOUNT)) 
      };

      if((COUNT_ORDER).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        COUNT_ORDER.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((COUNT_ORDER).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) 
      }
      else {
        COUNT_ORDER.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) 
      }
    };

    def onDeleteLINEITEM(var_LINEITEM_ORDERKEY: Long,var_LINEITEM_PARTKEY: Long,var_LINEITEM_SUPPKEY: Long,var_LINEITEM_LINENUMBER: Long,var_LINEITEM_QUANTITY: Double,var_LINEITEM_EXTENDEDPRICE: Double,var_LINEITEM_DISCOUNT: Double,var_LINEITEM_TAX: Double,var_LINEITEM_RETURNFLAG: String,var_LINEITEM_LINESTATUS: String,var_LINEITEM_SHIPDATE: Date,var_LINEITEM_COMMITDATE: Date,var_LINEITEM_RECEIPTDATE: Date,var_LINEITEM_SHIPINSTRUCT: String,var_LINEITEM_SHIPMODE: String,var_LINEITEM_COMMENT: String) = {
      if((SUM_QTY).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_QUANTITY))) 
      }
      else {
        SUM_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_QUANTITY)) 
      };

      if((SUM_BASE_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_BASE_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_BASE_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        SUM_BASE_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      if((SUM_DISC_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_DISC_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_DISC_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE)) * ((-1L) + (var_LINEITEM_DISCOUNT)))) 
      }
      else {
        SUM_DISC_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (var_LINEITEM_EXTENDEDPRICE)) * ((-1L) + (var_LINEITEM_DISCOUNT))) 
      };

      if((SUM_CHARGE).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        SUM_CHARGE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((SUM_CHARGE).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((1L) + (var_LINEITEM_TAX))) * (var_LINEITEM_EXTENDEDPRICE)) * ((-1L) + (var_LINEITEM_DISCOUNT)))) 
      }
      else {
        SUM_CHARGE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ((1L) + (var_LINEITEM_TAX))) * (var_LINEITEM_EXTENDEDPRICE)) * ((-1L) + (var_LINEITEM_DISCOUNT))) 
      };

      ({
        def tb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ({
                val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__71) = {
                  val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__70) = {
                    val _var___sql_inline_average_count_1:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                    val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                  };

                  val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                  val var___prod_ret__70:Long = _var___prod_ret__70;

                  Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__70) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                val var___prod_ret__71:Double = _var___prod_ret__71;

                var___prod_ret__71
              }
              )) * (var_LINEITEM_QUANTITY)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_QTY_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__20) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__20:Double = _var___map_ret__20;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__20) * ({
                          val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__77) = {
                            val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__76) = {
                              val _var___sql_inline_average_count_1:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                            };

                            val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                            val var___prod_ret__76:Long = _var___prod_ret__76;

                            Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__76) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                          val var___prod_ret__77:Double = _var___prod_ret__77;

                          var___prod_ret__77
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__19) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__19:Double = _var___sum_ret__19;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__19) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_QTY_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__22) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__22:Double = _var___map_ret__22;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__22) * ({
                        val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__84) = {
                          val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__83) = {
                            val _var___sql_inline_average_count_1:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                          };

                          val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                          val var___prod_ret__83:Long = _var___prod_ret__83;

                          Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__83) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        val var___prod_ret__84:Double = _var___prod_ret__84;

                        var___prod_ret__84
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__22) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__21) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__21:Double = _var___sum_ret__21;

                if((AVG_QTY).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__21)) 
                }
                else {
                  AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__21) 
                }
              }
            }
          }
        };

        def fb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_QUANTITY)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_QTY_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__20) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__20:Double = _var___map_ret__20;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__20) * ({
                          val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__77) = {
                            val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__76) = {
                              val _var___sql_inline_average_count_1:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                            };

                            val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                            val var___prod_ret__76:Long = _var___prod_ret__76;

                            Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__76) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                          val var___prod_ret__77:Double = _var___prod_ret__77;

                          var___prod_ret__77
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__19) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__19:Double = _var___sum_ret__19;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__19) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_QTY_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__22) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__22:Double = _var___map_ret__22;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__22) * ({
                        val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__84) = {
                          val Tuple2(_var___sql_inline_average_count_1,_var___prod_ret__83) = {
                            val _var___sql_inline_average_count_1:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_1),((0L) != (var___sql_inline_average_count_1)))
                          };

                          val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                          val var___prod_ret__83:Long = _var___prod_ret__83;

                          Tuple2[Long,Double]((var___sql_inline_average_count_1),((var___prod_ret__83) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_1));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_1:Long = _var___sql_inline_average_count_1;

                        val var___prod_ret__84:Double = _var___prod_ret__84;

                        var___prod_ret__84
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__22) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__21) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__21:Double = _var___sum_ret__21;

                if((AVG_QTY).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_QTY).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__21)) 
                }
                else {
                  AVG_QTY.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__21) 
                }
              }
            }
          }
        };

        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_QTY_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_QTY_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_QTY_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_QUANTITY))) 
      }
      else {
        AVG_QTY_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_QUANTITY)) 
      };

      ({
        def tb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ({
                val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__92) = {
                  val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__91) = {
                    val _var___sql_inline_average_count_2:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                    val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                  };

                  val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                  val var___prod_ret__91:Long = _var___prod_ret__91;

                  Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__91) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                val var___prod_ret__92:Double = _var___prod_ret__92;

                var___prod_ret__92
              }
              )) * (var_LINEITEM_EXTENDEDPRICE)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_PRICE_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__25) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__25:Double = _var___map_ret__25;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__25) * ({
                          val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__98) = {
                            val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__97) = {
                              val _var___sql_inline_average_count_2:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                            };

                            val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                            val var___prod_ret__97:Long = _var___prod_ret__97;

                            Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__97) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                          val var___prod_ret__98:Double = _var___prod_ret__98;

                          var___prod_ret__98
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__23) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__23:Double = _var___sum_ret__23;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__23) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_PRICE_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__27) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__27:Double = _var___map_ret__27;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__27) * ({
                        val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__105) = {
                          val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__104) = {
                            val _var___sql_inline_average_count_2:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                          };

                          val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                          val var___prod_ret__104:Long = _var___prod_ret__104;

                          Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__104) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        val var___prod_ret__105:Double = _var___prod_ret__105;

                        var___prod_ret__105
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__27) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__25) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__25:Double = _var___sum_ret__25;

                if((AVG_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__25)) 
                }
                else {
                  AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__25) 
                }
              }
            }
          }
        };

        def fb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_EXTENDEDPRICE)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_PRICE_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__25) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__25:Double = _var___map_ret__25;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__25) * ({
                          val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__98) = {
                            val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__97) = {
                              val _var___sql_inline_average_count_2:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                            };

                            val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                            val var___prod_ret__97:Long = _var___prod_ret__97;

                            Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__97) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                          val var___prod_ret__98:Double = _var___prod_ret__98;

                          var___prod_ret__98
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__23) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__23:Double = _var___sum_ret__23;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__23) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_PRICE_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__27) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__27:Double = _var___map_ret__27;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__27) * ({
                        val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__105) = {
                          val Tuple2(_var___sql_inline_average_count_2,_var___prod_ret__104) = {
                            val _var___sql_inline_average_count_2:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_2),((0L) != (var___sql_inline_average_count_2)))
                          };

                          val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                          val var___prod_ret__104:Long = _var___prod_ret__104;

                          Tuple2[Long,Double]((var___sql_inline_average_count_2),((var___prod_ret__104) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_2));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_2:Long = _var___sql_inline_average_count_2;

                        val var___prod_ret__105:Double = _var___prod_ret__105;

                        var___prod_ret__105
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__27) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__25) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__25:Double = _var___sum_ret__25;

                if((AVG_PRICE).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_PRICE).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__25)) 
                }
                else {
                  AVG_PRICE.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__25) 
                }
              }
            }
          }
        };

        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_PRICE_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_PRICE_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_PRICE_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_EXTENDEDPRICE))) 
      }
      else {
        AVG_PRICE_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_EXTENDEDPRICE)) 
      };

      ({
        def tb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * ({
                val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__113) = {
                  val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__112) = {
                    val _var___sql_inline_average_count_3:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                    val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                    Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                  };

                  val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                  val var___prod_ret__112:Long = _var___prod_ret__112;

                  Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__112) * ((div(({
                    val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                    listmax(arg._1,arg._2)
                  }
                  ))))))
                };

                val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                val var___prod_ret__113:Double = _var___prod_ret__113;

                var___prod_ret__113
              }
              )) * (var_LINEITEM_DISCOUNT)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_DISC_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__30) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__30:Double = _var___map_ret__30;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__30) * ({
                          val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__119) = {
                            val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__118) = {
                              val _var___sql_inline_average_count_3:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                            };

                            val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                            val var___prod_ret__118:Long = _var___prod_ret__118;

                            Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__118) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                          val var___prod_ret__119:Double = _var___prod_ret__119;

                          var___prod_ret__119
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__27) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__27:Double = _var___sum_ret__27;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__27) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_DISC_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__32) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__32:Double = _var___map_ret__32;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__32) * ({
                        val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__126) = {
                          val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__125) = {
                            val _var___sql_inline_average_count_3:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                          };

                          val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                          val var___prod_ret__125:Long = _var___prod_ret__125;

                          Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__125) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        val var___prod_ret__126:Double = _var___prod_ret__126;

                        var___prod_ret__126
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__32) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__29) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__29:Double = _var___sum_ret__29;

                if((AVG_DISC).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__29)) 
                }
                else {
                  AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__29) 
                }
              }
            }
          }
        };

        def fb = (({
          val result = Map[Tuple2[String,String],Double]();

          (({
            val result = Map[Tuple2[String,String],Double]();

            ({
              val v = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG),(var_LINEITEM_LINESTATUS),((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                listmax(arg._1,arg._2)
              }
              )))))) * (var_LINEITEM_DISCOUNT)));

              K3IntermediateCollection[Tuple2[String,String], Double](List(Tuple2(Tuple2[String,String](v._1,v._2),v._3))) 
            }
            ).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            (AVG_DISC_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
              val v = ({
                (x:Tuple2[Tuple2[String,String], Double]) => {
                  x match {
                    case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__30) => {
                      val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                      val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                      val var___map_ret__30:Double = _var___map_ret__30;

                      ({
                        def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__30) * ({
                          val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__119) = {
                            val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__118) = {
                              val _var___sql_inline_average_count_3:Long = (AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1));

                              val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                              Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                            };

                            val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                            val var___prod_ret__118:Long = _var___prod_ret__118;

                            Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__118) * ((div(({
                              val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                              listmax(arg._1,arg._2)
                            }
                            ))))))
                          };

                          val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                          val var___prod_ret__119:Double = _var___prod_ret__119;

                          var___prod_ret__119
                        }
                        )));

                        def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),(0.));

                        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                          tb 
                        }
                        else {
                          fb 
                        }
                      }
                      )
                    }
                  }
                }
              }
              )(y);

              Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
            }
            )).foreach {
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_v1,_v2),_v3) => {
                    val v1:String = _v1;

                    val v2:String = _v2;

                    val v3:Double = _v3;

                    val t = Tuple2(v1,v2);

                    result += ((t, (result.get(t) match {
                      case Some(v) => v + v3;

                      case _ => v3 
                    }
                    )))
                  }
                }
              }
            };

            K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
          }
          ).map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__27) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___sum_ret__27:Double = _var___sum_ret__27;

                    Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___sum_ret__27) * (-1L)))
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          (AVG_DISC_mLINEITEM5.map((y:Tuple2[Tuple2[String,String],Double]) => {
            val v = ({
              (x:Tuple2[Tuple2[String,String], Double]) => {
                x match {
                  case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___map_ret__32) => {
                    val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                    val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                    val var___map_ret__32:Double = _var___map_ret__32;

                    ({
                      def tb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__32) * ({
                        val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__126) = {
                          val Tuple2(_var___sql_inline_average_count_3,_var___prod_ret__125) = {
                            val _var___sql_inline_average_count_3:Long = ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L));

                            val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                            Tuple2[Long,Boolean]((var___sql_inline_average_count_3),((0L) != (var___sql_inline_average_count_3)))
                          };

                          val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                          val var___prod_ret__125:Long = _var___prod_ret__125;

                          Tuple2[Long,Double]((var___sql_inline_average_count_3),((var___prod_ret__125) * ((div(({
                            val arg = Tuple2[Long,Long]((1L),(var___sql_inline_average_count_3));

                            listmax(arg._1,arg._2)
                          }
                          ))))))
                        };

                        val var___sql_inline_average_count_3:Long = _var___sql_inline_average_count_3;

                        val var___prod_ret__126:Double = _var___prod_ret__126;

                        var___prod_ret__126
                      }
                      )));

                      def fb = Tuple3[String,String,Double]((var_LINEITEM_RETURNFLAG_1),(var_LINEITEM_LINESTATUS_1),((var___map_ret__32) * (((0L) != (((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L))) * ((div(({
                        val arg = Tuple2[Long,Long]((1L),(((((var_LINEITEM_RETURNFLAG) == (var_LINEITEM_RETURNFLAG_1)) * ((var_LINEITEM_LINESTATUS) == (var_LINEITEM_LINESTATUS_1))) * ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime()))) * (-1L)));

                        listmax(arg._1,arg._2)
                      }
                      )))))));

                      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                        tb 
                      }
                      else {
                        fb 
                      }
                    }
                    )
                  }
                }
              }
            }
            )(y);

            Tuple2[Tuple2[String,String],Double]((v._1,v._2), v._3) 
          }
          )).foreach {
            (x:Tuple2[Tuple2[String,String], Double]) => {
              x match {
                case Tuple2(Tuple2(_v1,_v2),_v3) => {
                  val v1:String = _v1;

                  val v2:String = _v2;

                  val v3:Double = _v3;

                  val t = Tuple2(v1,v2);

                  result += ((t, (result.get(t) match {
                    case Some(v) => v + v3;

                    case _ => v3 
                  }
                  )))
                }
              }
            }
          };

          K3IntermediateCollection[Tuple2[String,String], Double](result.toList) 
        }
        )).foreach {
          (x:Tuple2[Tuple2[String,String], Double]) => {
            x match {
              case Tuple2(Tuple2(_var_LINEITEM_RETURNFLAG_1,_var_LINEITEM_LINESTATUS_1),_var___sum_ret__29) => {
                val var_LINEITEM_RETURNFLAG_1:String = _var_LINEITEM_RETURNFLAG_1;

                val var_LINEITEM_LINESTATUS_1:String = _var_LINEITEM_LINESTATUS_1;

                val var___sum_ret__29:Double = _var___sum_ret__29;

                if((AVG_DISC).contains(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) {
                  AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), ((AVG_DISC).lookup(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1))) + (var___sum_ret__29)) 
                }
                else {
                  AVG_DISC.updateValue(Tuple2(var_LINEITEM_RETURNFLAG_1,var_LINEITEM_LINESTATUS_1), var___sum_ret__29) 
                }
              }
            }
          }
        };

        if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
          tb 
        }
        else {
          fb 
        }
      }
      );

      if((AVG_QTY_mLINEITEM1_L3_1).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_QTY_mLINEITEM1_L3_1.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_QTY_mLINEITEM1_L3_1).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L))) 
      }
      else {
        AVG_QTY_mLINEITEM1_L3_1.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) 
      };

      if((AVG_DISC_mLINEITEM5).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        AVG_DISC_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((AVG_DISC_mLINEITEM5).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + ((((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_DISCOUNT))) 
      }
      else {
        AVG_DISC_mLINEITEM5.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) * (var_LINEITEM_DISCOUNT)) 
      };

      if((COUNT_ORDER).contains(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) {
        COUNT_ORDER.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((COUNT_ORDER).lookup(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS))) + (((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L))) 
      }
      else {
        COUNT_ORDER.updateValue(Tuple2(var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS), ((var_LINEITEM_SHIPDATE.getTime()) <= (new GregorianCalendar(1997,9 - 1,1).getTime().getTime())) * (-1L)) 
      }
    };

    def onSystemInitialized() = {
    };

    def fillTables(): Unit = {;

    }
    def dispatcher(event: DBTEvent, onEventProcessedHandler: Unit => Unit): Unit = {
      event match {
        case StreamEvent(InsertTuple, o, "LINEITEM", (var_LINEITEM_ORDERKEY: Long)::(var_LINEITEM_PARTKEY: Long)::(var_LINEITEM_SUPPKEY: Long)::(var_LINEITEM_LINENUMBER: Long)::(var_LINEITEM_QUANTITY: Double)::(var_LINEITEM_EXTENDEDPRICE: Double)::(var_LINEITEM_DISCOUNT: Double)::(var_LINEITEM_TAX: Double)::(var_LINEITEM_RETURNFLAG: String)::(var_LINEITEM_LINESTATUS: String)::(var_LINEITEM_SHIPDATE: Date)::(var_LINEITEM_COMMITDATE: Date)::(var_LINEITEM_RECEIPTDATE: Date)::(var_LINEITEM_SHIPINSTRUCT: String)::(var_LINEITEM_SHIPMODE: String)::(var_LINEITEM_COMMENT: String)::Nil) => onInsertLINEITEM(var_LINEITEM_ORDERKEY,var_LINEITEM_PARTKEY,var_LINEITEM_SUPPKEY,var_LINEITEM_LINENUMBER,var_LINEITEM_QUANTITY,var_LINEITEM_EXTENDEDPRICE,var_LINEITEM_DISCOUNT,var_LINEITEM_TAX,var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS,var_LINEITEM_SHIPDATE,var_LINEITEM_COMMITDATE,var_LINEITEM_RECEIPTDATE,var_LINEITEM_SHIPINSTRUCT,var_LINEITEM_SHIPMODE,var_LINEITEM_COMMENT);

        case StreamEvent(DeleteTuple, o, "LINEITEM", (var_LINEITEM_ORDERKEY: Long)::(var_LINEITEM_PARTKEY: Long)::(var_LINEITEM_SUPPKEY: Long)::(var_LINEITEM_LINENUMBER: Long)::(var_LINEITEM_QUANTITY: Double)::(var_LINEITEM_EXTENDEDPRICE: Double)::(var_LINEITEM_DISCOUNT: Double)::(var_LINEITEM_TAX: Double)::(var_LINEITEM_RETURNFLAG: String)::(var_LINEITEM_LINESTATUS: String)::(var_LINEITEM_SHIPDATE: Date)::(var_LINEITEM_COMMITDATE: Date)::(var_LINEITEM_RECEIPTDATE: Date)::(var_LINEITEM_SHIPINSTRUCT: String)::(var_LINEITEM_SHIPMODE: String)::(var_LINEITEM_COMMENT: String)::Nil) => onDeleteLINEITEM(var_LINEITEM_ORDERKEY,var_LINEITEM_PARTKEY,var_LINEITEM_SUPPKEY,var_LINEITEM_LINENUMBER,var_LINEITEM_QUANTITY,var_LINEITEM_EXTENDEDPRICE,var_LINEITEM_DISCOUNT,var_LINEITEM_TAX,var_LINEITEM_RETURNFLAG,var_LINEITEM_LINESTATUS,var_LINEITEM_SHIPDATE,var_LINEITEM_COMMITDATE,var_LINEITEM_RECEIPTDATE,var_LINEITEM_SHIPINSTRUCT,var_LINEITEM_SHIPMODE,var_LINEITEM_COMMENT);

        case StreamEvent(SystemInitialized, o, "", Nil) => onSystemInitialized();

        case EndOfStream => ();

        case _ => throw DBTFatalError("Event could not be dispatched: " + event)
      }
      onEventProcessedHandler();

      if(sources.hasInput()) dispatcher(sources.nextInput(), onEventProcessedHandler) 
    }
    def run(onEventProcessedHandler: Unit => Unit = (_ => ())): Unit ={
      fillTables();

      dispatcher(StreamEvent(SystemInitialized, 0, "",List()), onEventProcessedHandler) 
    }
    def printResults(): Unit = {
      val pp = new PrettyPrinter(8000, 2);

      println(pp.format(<SUM_QTY>{
        getSUM_QTY().toXML() 
      }
      </SUM_QTY>));

      println(pp.format(<SUM_BASE_PRICE>{
        getSUM_BASE_PRICE().toXML() 
      }
      </SUM_BASE_PRICE>));

      println(pp.format(<SUM_DISC_PRICE>{
        getSUM_DISC_PRICE().toXML() 
      }
      </SUM_DISC_PRICE>));

      println(pp.format(<SUM_CHARGE>{
        getSUM_CHARGE().toXML() 
      }
      </SUM_CHARGE>));

      println(pp.format(<AVG_QTY>{
        getAVG_QTY().toXML() 
      }
      </AVG_QTY>));

      println(pp.format(<AVG_PRICE>{
        getAVG_PRICE().toXML() 
      }
      </AVG_PRICE>));

      println(pp.format(<AVG_DISC>{
        getAVG_DISC().toXML() 
      }
      </AVG_DISC>));

      println(pp.format(<COUNT_ORDER>{
        getCOUNT_ORDER().toXML() 
      }
      </COUNT_ORDER>));

    }
  }
}
