package scaetch.bench

import org.scalameter._
import scaetch.sketch.hash.implicits._
import scaetch.sketch.{BufferedSketch, CountMinSketch, CountSketch, SparkCountMinSketchWrapper}
import scaetch.util._

import scala.collection.mutable


object Benchmark extends App {

  private def prepareSketches(depth: Int, width: Int, bufferSize: Int) = {
    List(
      (() => CountSketch(depth, width), "CountSketch"),
      (() => CountMinSketch(depth, width), "CountMinSketch"),
      (() => CountMinSketch(depth, width).withConservativeUpdates, "CountMinSketch with CU"),
      (() => SparkCountMinSketchWrapper(depth, width, 42), "SparkCountMinSketchWrapper"),
      (() => new BufferedSketch(CountSketch(depth, width), bufferSize), "BufferedCountSketch"),
      (() => new BufferedSketch(CountMinSketch(depth, width), bufferSize), "BufferedCountMinSketch")
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

  def runTopKComparison(data: List[Any],
                        depths: List[Int],
                        widths: List[Int],
                        bufferSize: Int,
                        k: Int) = {

    println("---------------------------------------------------")
    println(s"Running top K comparison (top $k recall/precision)")
    println("---------------------------------------------------")

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
          val estimates = actualCounts.map { case (k, _) => (k, sk.estimate(k).toInt) }
          val recall = 100 * Misc.getTopKIntersection(actualCounts, estimates, k).toDouble / k.toDouble
          results.update((sketch._2, width.toString), recall)
        }
      }
      println(s"Depth = $depth")
      Printer.printTable(results.toMap)
      println("\n")
    }
  }

  def runHeavyHitterComparison(data: List[Any],
                               depths: List[Int],
                               widths: List[Int],
                               bufferSize: Int,
                               phi: Double) = {

    println("--------------------------------------------")
    println(s"Running heavy hitter comparison (phi = $phi)")
    println("--------------------------------------------")

    // Compute actual counts
    val n = data.length
    val actualCounts = data.groupBy(elem => elem).mapValues(_.length)
    val actualFrequencies = actualCounts.mapValues(_ / n.toDouble)
    val actualHeavyHitters = actualFrequencies
      .filter { case (_, f) => f > phi }
      .keys
      .toSet

    for (depth <- depths) {
      val recallResults = mutable.Map.empty[(String, String), Double]
      val precisionResults = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth, width, bufferSize)
        for (sketch <- sketches) {
          val sk = sketch._1()
          for (elem <- data) {
            sk.add(elem, 1L)
          }
          // Estimate an arbitrary value to flush buffers
          sk.estimate(0L)
          val estimatedHeavyHitters = actualCounts
            .map { case (k, _) => (k, sk.estimate(k) / n.toDouble) }
            .filter { case (_, f) => f > phi }
            .keys
            .toSet
          val trueEstimatedHeavyHitters = 100.0d * actualHeavyHitters.intersect(estimatedHeavyHitters).size
          val recall = trueEstimatedHeavyHitters / actualHeavyHitters.size
          val precision = trueEstimatedHeavyHitters / estimatedHeavyHitters.size
          recallResults.update((sketch._2, width.toString), recall)
          precisionResults.update((sketch._2, width.toString), precision)
        }
      }
      println(s"Depth = $depth")
      println("\nRecall")
      Printer.printTable(recallResults.toMap)
      println("\nPrecision")
      Printer.printTable(precisionResults.toMap)
      println("\n")
    }
  }

  val benchmarkArgs = Args.validate(args)

  val data = if (benchmarkArgs.file == "generate") {
    val data = Data.generateZipfianLongs(1000000, 1.25, 42)
    if (benchmarkArgs.dataType == "string") {
      data.map(_.toString)
    } else {
      data
    }
  } else {
    if (benchmarkArgs.dataType == "long") {
      FileUtil.readAs[Long](benchmarkArgs.file).toList
    } else {
      FileUtil.readAs[String](benchmarkArgs.file).toList
    }
  }

  runThroughputBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runMemoryBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runPrecisionBenchmark(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize)
  runTopKComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 10)
  runTopKComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 100)
  runTopKComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 1000)
  runHeavyHitterComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 0.01)
  runHeavyHitterComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 0.05)
  runHeavyHitterComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 0.1)
  runHeavyHitterComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 0.2)
  runHeavyHitterComparison(data, benchmarkArgs.depths, benchmarkArgs.widths, benchmarkArgs.bufferSize, 0.5)
}
