import org.scalameter
import org.scalameter._
import sketch.{BufferedSketch, CountMinSketch, CountSketch, SparkCountMinSketchWrapper}
import util.{Args, DeepSize, File}

import scala.collection.mutable


object Benchmark extends App {

  val SEED = 0

  // https://stackoverflow.com/questions/55094233/scala-table-print
  def printTable(map: Map[(String, String), Double]) = {
    val (rows, cols) = {
      val (r, c) = map.keys.unzip
      (r.toList.sorted, c.toList.map(_.toLong).sorted.map(_.toString))
    }

    val firstColumnOffset = rows.map(_.length).max
    val valueOffset = map.values.map(" %.1f".format(_).length).max
    val table =
      (List(s"%${firstColumnOffset}s".format("")) ++ cols.map(s"%${valueOffset}s".format(_))).mkString + "\n" +
        rows.map { r =>
          s"%${firstColumnOffset}s".format(r) + cols.map { c =>
            s"%${valueOffset}s".format("%.1f".format(map.getOrElse((r, c), 0.0)))
          }.mkString
        }.mkString("\n")

    println(table)
  }

  def runThroughputBenchmark(fileName: String,
                             depths: List[Int],
                             widths: List[Int]) = {

    println("----------------------------------------------")
    println("Running throughput comparison (add ops/second)")
    println("----------------------------------------------")

    def prepareSketches(depth: Int)(width: Int) = {
      List(
        (CountSketch[String](depth, width, SEED), "CountSketch"),
        (new BufferedSketch(CountSketch[String](depth, width, SEED), 100), "BufferedCountSketch"),
        (CountMinSketch[String](depth, width, SEED), "CountMinSketch"),
        (new BufferedSketch(CountMinSketch[String](depth, width, SEED), 100), "BufferedCountMinSketch"),
        (CountMinSketch[String](depth, width, SEED, true), "CountMinSketch w CS"),
        (SparkCountMinSketchWrapper[String](depth, width, SEED), "SparkCountMinSketchWrapper")
      )
    }

    val timer = config(Key.exec.benchRuns -> 5, Key.verbose -> false)
      .withWarmer { new Warmer.Default }
      .withMeasurer { new Measurer.IgnoringGC }

    val data = File.readStrings(fileName).toList.take(100000)
    val numLines = data.length

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth)(width)
        for (sketch <- sketches) {
          val t = timer.measure {
            for (elem <- data) {
              sketch._1.add(elem)
            }
          }.value
          val throughput = numLines / t
          results.update((sketch._2, width.toString), throughput)
        }
      }
      println(s"Depth = $depth")
      printTable(results.toMap)
      println("\n")
    }
  }

  def runMemoryBenchmark(fileName: String,
                             depths: List[Int],
                             widths: List[Int]) = {

    println("------------------------------")
    println("Running memory comparison (Kb)")
    println("------------------------------")

    def prepareSketches(depth: Int)(width: Int) = {
      List(
        (CountSketch[String](depth, width, SEED), "CountSketch"),
        (new BufferedSketch(CountSketch[String](depth, width, SEED), 100), "BufferedCountSketch"),
        (CountMinSketch[String](depth, width, SEED), "CountMinSketch"),
        (new BufferedSketch(CountMinSketch[String](depth, width, SEED), 100), "BufferedCountMinSketch"),
        (CountMinSketch[String](depth, width, SEED, true), "CountMinSketch w CS"),
        (SparkCountMinSketchWrapper[String](depth, width, SEED), "SparkCountMinSketchWrapper")
      )
    }

    val data = File.readStrings(fileName).toList.take(100000)
    val numLines = data.length

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth)(width)
        for (sketch <- sketches) {
          for (elem <- data) {
            sketch._1.add(elem)
          }
          sketch._1.estimate("flush!")
          val size = DeepSize(sketch._1)
          results.update((sketch._2, width.toString), size)
        }
      }
      println(s"Depth = $depth")
      printTable(results.toMap)
      println("\n")
    }
  }

  def runPrecisionBenchmark(fileName: String,
                            depths: List[Int],
                            widths: List[Int]) = {

    println("-----------------------------------------------------")
    println("Running precision comparison (Root Mean Square Error)")
    println("-----------------------------------------------------")

    def prepareSketches(depth: Int)(width: Int) = {
      List(
        (CountSketch[String](depth, width, SEED), "CountSketch"),
        (CountMinSketch[String](depth, width, SEED), "CountMinSketch"),
        (CountMinSketch[String](depth, width, SEED, true), "CountMinSketch w CS"),
        (SparkCountMinSketchWrapper[String](depth, width, SEED), "SparkCountMinSketchWrapper")
      )
    }

    val data = File.readStrings(fileName).toList.take(1000000)
    val numLines = data.length

    // Compute actual counts
    val actualCounts = data.groupBy(elem => elem).mapValues(_.length)

    for (depth <- depths) {
      val results = mutable.Map.empty[(String, String), Double]
      for (width <- widths) {
        val sketches = prepareSketches(depth)(width)
        for (sketch <- sketches) {
          for (elem <- data) {
            sketch._1.add(elem)
          }
          // Compute RMSE
          val rmse = Math.sqrt(actualCounts.map { case (k, v) => Math.pow(sketch._1.estimate(k) - v, 2) }.sum/numLines)
          results.update((sketch._2, width.toString), rmse)
        }
      }
      println(s"Depth = $depth")
      printTable(results.toMap)
      println("\n")
    }
  }

  val benchmarkArgs = Args.validate(args)
  runThroughputBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths)
  runPrecisionBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths)
  runMemoryBenchmark(benchmarkArgs.file, benchmarkArgs.depths, benchmarkArgs.widths)
}
