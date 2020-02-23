package bench

import sketch.{BufferedSketch, CountMinSketch, CountSketch, SparkCountMinSketchWrapper}
import util._

import scala.collection.mutable
import org.scalameter._


object Benchmark extends App {
  val SEED = 0

  private def prepareSketches(depth: Int, width: Int, bufferSize: Int) = {
    List(
      (CountSketch(depth, width, SEED), "CountSketch"),
      (CountMinSketch(depth, width, SEED), "CountMinSketch"),
      (CountMinSketch.withConservativeUpdates(depth, width, SEED), "CountMinSketch with CU"),
      (SparkCountMinSketchWrapper(depth, width, SEED), "SparkCountMinSketchWrapper"),
      (new BufferedSketch(CountSketch(depth, width, SEED), bufferSize), "BufferedCountSketch"),
      (new BufferedSketch(CountMinSketch(depth, width, SEED), bufferSize), "BufferedCountMinSketch")
    )
  }

  def runThroughputBenchmark(fileName: String,
                             depths: List[Int],
                             widths: List[Int],
                             bufferSize: Int) = {

    println("----------------------------------------------")
    println("Running throughput comparison (add ops/second)")
    println("----------------------------------------------")

    val timer = config(Key.exec.benchRuns -> 5, Key.verbose -> false)
      .withWarmer { new Warmer.Default }
      .withMeasurer { new Measurer.IgnoringGC }

    val data = File.readStrings(fileName).toList
    val numLines = data.length

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          val t = timer.measure {
            for (elem <- data) {
              sketch._1.add(elem, 1L)
            }
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

  def runMemoryBenchmark(fileName: String,
                         depths: List[Int],
                         widths: List[Int],
                         bufferSize: Int) = {

    println("---------------------------------")
    println("Running memory comparison (bytes)")
    println("---------------------------------")

    val data = File.readStrings(fileName).toList
    val numLines = data.length

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          for (elem <- data) {
            sketch._1.add(elem, 1L)
          }
          sketch._1.estimate("flush!")
          val size = DeepSize(sketch._1)
          results.update((sketch._2, width.toString), size)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  def runPrecisionBenchmark(fileName: String,
                            depths: List[Int],
                            widths: List[Int],
                            bufferSize: Int) = {

    println("-----------------------------------------------------")
    println("Running precision comparison (Root Mean Square Error)")
    println("-----------------------------------------------------")

    val data = File.readStrings(fileName).toList
    val numLines = data.length

    // Compute actual counts
    val actualCounts = data.groupBy(elem => elem).mapValues(_.length)

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          for (elem <- data) {
            sketch._1.add(elem, 1L)
          }
          // Compute RMSE
          val rmse = Math.sqrt(actualCounts.map { case (k, v) => Math.pow(sketch._1.estimate(k) - v, 2) }.sum/numLines)
          results.update((sketch._2, width.toString), rmse)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  val benchmarkArgs = Args.validate(args)
  runThroughputBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runPrecisionBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runMemoryBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
}
