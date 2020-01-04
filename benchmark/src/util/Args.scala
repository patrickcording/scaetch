package util

import scala.util.Try
import scalaz._
import scalaz.Scalaz._


case class BenchmarkArgs(
                        depth: Int,
                        width: Range,
                        k: Int,
                        file: String
                        )

object Args {
  val usage =
    s"""
       |Usage:
       |
       |  mill benchmark.Benchmark depth width_range k file
       |
       |Example:
       |
       |  mill benchmark.Benchmark 5 16,512 50 path/to/file
     """.stripMargin

  def validate(args: Array[String]): BenchmarkArgs = {
    if (args.length != 4) {
      throw new IllegalArgumentException(s"""Incorrect number of arguments\n\n""" + usage)
    }

    val validationResult = validateArgs(args)
    validationResult match {
      case Success(benchmarkArgs) => benchmarkArgs
      case Failure(errors) =>
        val message = "Could not parse arguments:\n\n" + errors.map(e => "- " + e).mkString("\n")
        throw new IllegalArgumentException(message)
    }
  }

  private def getInt(v: String) = Try { v.toInt }.toOption

  private def validateArgs(args: Array[String]): Validation[List[String], BenchmarkArgs] = {
    def validDepth(depth: String) = getInt(depth) match {
      case Some(v) if v > 0 => v.success
      case _ => List("depth should be a positive integer").failure
    }

    def validWidthRange(widthRange: String) = {
      val range = widthRange.split(",")
      (getInt(range(0)), if (range.length == 2) getInt(range(1)) else None) match {
        case (Some(start), Some(end)) if start > 0 & end > 0 => (start to end).success
        case _ => List("width_range should be two positive integers separated by ','").failure
      }
    }

    def validK(k: String) = getInt(k) match {
      case Some(v) if v > 0 => v.success
      case _ => List("k should be a positive integer").failure
    }

    def validFile(file: String) = {
      if (File.exists(file)) file.success
      else List("File does not exist").failure
    }

    (validDepth(args(0))
      |@| validWidthRange(args(1))
      |@| validK(args(2))
      |@| validFile(args(3)))((depth, range, k, file) => BenchmarkArgs(depth, range, k, file))
  }
}
