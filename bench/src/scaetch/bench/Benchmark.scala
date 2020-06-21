package scaetch.bench

import net.openhft.hashing.LongHashFunction
import org.apache.commons.math3.distribution.ZipfDistribution
import org.scalameter._
import scaetch.sketch.{BufferedSketch, CountMinSketch, CountSketch, HeavyHitter, SparkCountMinSketchWrapper}
import scaetch.util.{Args, DeepSize, FileUtil, Misc, Printer}
import scaetch.sketch.hash.implicits._

import scala.collection.mutable


object Benchmark extends App {

  private def prepareSketches(depth: Int, width: Int, bufferSize: Int) = {
    List(
//      (() => CountSketch(depth, width), "CountSketch"),
      (() => CountMinSketch(depth, width), "CountMinSketch"),
//      (() => CountMinSketch(depth, width).withConservativeUpdates, "CountMinSketch with CU"),
      (() => SparkCountMinSketchWrapper(depth, width, 42), "SparkCountMinSketchWrapper"),
//      (() => new BufferedSketch(CountSketch(depth, width), bufferSize), "BufferedCountSketch"),
//      (() => new BufferedSketch(CountMinSketch(depth, width), bufferSize), "BufferedCountMinSketch")
    )
  }

  def runThroughputBenchmark(data: List[Any],
                             depths: List[Int],
                             widths: List[Int],
                             bufferSize: Int) = {

    println("----------------------------------------------")
    println("Running throughput comparison (add ops/second)")
    println("----------------------------------------------")

    val timer = config(Key.exec.benchRuns -> 5, Key.verbose -> false)
      .withWarmer { new Warmer.Default }
      .withMeasurer { new Measurer.IgnoringGC }

    val numLines = data.length

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          val t = timer.measure {
            val sk = sketch._1()
            for (elem <- data) {
              sk.add(elem, 1L)
            }
            // Estimate an arbitrary value to flush buffers
            sk.estimate(0L)
          }.value
          val throughput = numLines / t
          results.update((sketch._2, width.toString), throughput)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  def runMemoryBenchmark(data: List[Any],
                         depths: List[Int],
                         widths: List[Int],
                         bufferSize: Int) = {

    println("---------------------------------")
    println("Running memory comparison (bytes)")
    println("---------------------------------")

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          val sk = sketch._1()
          for (elem <- data) {
            sk.add(elem, 1L)
          }
          // Estimate an arbitrary value to flush buffers
          sk.estimate(0L)
          val size = DeepSize(sk)
          results.update((sketch._2, width.toString), size)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  def runPrecisionBenchmark(data: List[Any],
                            depths: List[Int],
                            widths: List[Int],
                            bufferSize: Int) = {

    println("-----------------------------------------------------")
    println("Running precision comparison (Root Mean Square Error)")
    println("-----------------------------------------------------")

    val numLines = data.length

    // Compute actual counts
    val actualCounts = data.groupBy(elem => elem).mapValues(_.length)

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          val sk = sketch._1()
          for (elem <- data) {
            sk.add(elem, 1L)
          }
          // Estimate an arbitrary value to flush buffers
          sk.estimate(0L)
          // Compute RMSE
          val rmse = Math.sqrt(actualCounts.map { case (k, v) => Math.pow(sk.estimate(k) - v, 2) }.sum/numLines)
          results.update((sketch._2, width.toString), rmse)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  val benchmarkArgs = Args.validate(args)

  val data = if (benchmarkArgs.dataType == "long") {
    FileUtil.readAs[Long](benchmarkArgs.file).toList
  } else {
    FileUtil.readAs[String](benchmarkArgs.file).toList
  }

  runThroughputBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runPrecisionBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runMemoryBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
}
