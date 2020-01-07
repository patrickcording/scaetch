package util

import scala.util.{Failure, Try}


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
      case Right(benchmarkArgs) => benchmarkArgs
      case Left(errors) =>
        val message = "Could not parse arguments:\n\n" + errors.map(e => "- " + e).mkString("\n")
        throw new IllegalArgumentException(message)
    }
  }

  private def getInt(v: String) = Try { v.toInt }.toOption

  private def validateArgs(args: Array[String]): Either[List[String], BenchmarkArgs] = {
    val validDepth = (depth: String) => getInt(depth) match {
      case Some(v) if v > 0 => v
      case _ => throw new IllegalArgumentException("depth should be a positive integer")
    }

    val validWidthRange = (widthRange: String) => {
      val range = widthRange.split(",")
      (getInt(range(0)), if (range.length == 2) getInt(range(1)) else None) match {
        case (Some(start), Some(end)) if start > 0 & end > 0 => (start to end)
        case _ => throw new IllegalArgumentException("width_range should be two positive integers separated by ','")
      }
    }

    val validK = (k: String) => getInt(k) match {
      case Some(v) if v > 0 => v
      case _ => throw new IllegalArgumentException("k should be a positive integer")
    }

    val validFile = (file: String) => {
      if (File.exists(file)) file
      else throw new IllegalArgumentException("File does not exist")
    }

    val validators = List(validDepth, validWidthRange, validK, validFile)
    val results = args.zip(validators).map { case (arg, validate) => Try(validate(arg)) }

    if (results.exists(_.isFailure)) {
      Left(results.filter(_.isFailure).map { case Failure(e) => e.getMessage }.toList)
    } else {
      val resultValues = results.map(_.get)
      (resultValues(0), resultValues(1), resultValues(2), resultValues(3)) match {
        case t: (Int, Range, Int, String) => Right(BenchmarkArgs.tupled(t))
      }
    }
  }
}
