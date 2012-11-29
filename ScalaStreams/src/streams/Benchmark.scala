package org.example

import annotation.tailrec
import com.google.caliper.Param
import streams.DBToaster

/* To run this benchmark:
 * follow instructions at https://github.com/dcsobral/scala-foreach-benchmark to setup
 * benchmark project, then decomment and copy this file over to replace
 * src/main/scala/org/example/Benchmark.scala. Also copy all streams code and data to
 * the benchmark project template. Use sbt to compile and run.
 */

/* Results:
[info]              filename                  benchmark      ms linear runtime
[info]     lineitem_tiny.csv           ToastUpdateOnNew    2.77 =
[info]     lineitem_tiny.csv       ToastUpdateMapReduce    2.43 =
[info]     lineitem_tiny.csv           ToastUpdateSplit    3.07 =
[info]     lineitem_tiny.csv ToastAggregateAndRecompute   13.84 =
[info] lineitem_standard.csv           ToastUpdateOnNew   23.55 =
[info] lineitem_standard.csv       ToastUpdateMapReduce   23.24 =
[info] lineitem_standard.csv           ToastUpdateSplit   29.47 =
[info] lineitem_standard.csv ToastAggregateAndRecompute 1129.50 ==============================
[info] 
[info] vm: java
[info] trial: 0
[info] 
[info] Note: benchmarks printed 6815761 characters to System.out and 0 characters to System.err. Use --debug to see this output.

[info]              filename                  benchmark      ms linear runtime
[info]     lineitem_tiny.csv       ToastUpdateMapReduce    2.51 =
[info]     lineitem_tiny.csv           ToastUpdateOnNew    2.45 =
[info]     lineitem_tiny.csv           ToastUpdateSplit    6.44 =
[info]     lineitem_tiny.csv ToastAggregateAndRecompute   12.98 =
[info] lineitem_standard.csv       ToastUpdateMapReduce   24.99 =
[info] lineitem_standard.csv           ToastUpdateOnNew   23.95 =
[info] lineitem_standard.csv           ToastUpdateSplit   30.11 =
[info] lineitem_standard.csv ToastAggregateAndRecompute 1094.42 ==============================
[info] 
[info] vm: java
[info] trial: 0
[info] 
[info] Note: benchmarks printed 7567396 characters to System.out and 0 characters to System.err. Use --debug to see this output.
 * 
 */
//class Benchmark extends SimpleScalaBenchmark {
//  
//  @Param(Array("lineitem_tiny.csv", "lineitem_standard.csv"))
//  val filename: String = ""
//    
//  val printIntermediate = false
//  
//  override def setUp() {
//    // set up all your benchmark data here
//  }
//  
//  def timeToastUpdateMapReduce(reps: Int) = repeat(reps) {
//    var result = 0
//    result += DBToaster.toastUpdateMapReduce(printIntermediate, filename)
//    result
//  }
//  
//  def timeToastUpdateOnNew(reps: Int) = repeat(reps) {
//    var result = 0
//    result += DBToaster.toastUpdateOnNew(printIntermediate, filename)
//    result
//  }
//
//  def timeToastUpdateSplit(reps: Int) = repeat(reps) {
//    var result = 0
//    result += DBToaster.toastUpdateSplit(printIntermediate, filename)
//    result
//  }
//  
//  def timeToastAggregateAndRecompute(reps: Int) = repeat(reps) {
//    var result = 0
//    result += DBToaster.toastAggregateAndRecompute(printIntermediate, filename)
//    result
//  }
//  
//  override def tearDown() {
//    // clean up after yourself if required
//  }
//  
//}