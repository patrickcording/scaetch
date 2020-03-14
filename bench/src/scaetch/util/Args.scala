package scaetch.util

import scala.util.{Failure, Try}


case class BenchmarkArgs(
                        depths: List[Int],
                        widths: List[Int],
                        bufferSize: Int,
                        dataType: String,
                        file: String
                        )

object Args {
  val usage =
    s"""
       |Usage:
       |
       |  mill benchmark.Benchmark depth_range width_range buffer_size file
       |
       |Example:
       |
       |  mill benchmark.Benchmark 5,10 16,512 1000 string path/to/file
     """.stripMargin

  def validate(args: Array[String]): BenchmarkArgs = {
    if (args.length != 5) {
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
  private def isPowerOfTwo(v: Int) = (Math.log(v)/Math.log(2)).isWhole()

  private def validateArgs(args: Array[String]): Either[List[String], BenchmarkArgs] = {
    val validDepthRange = (depthRange: String) => {
      val range = depthRange.split(",")
      (getInt(range(0)), if (range.length == 2) getInt(range(1)) else None) match {
        case (Some(start), Some(end)) if start > 0 & end > 0 & end >= start =>
          (start to end).toList
        case _ =>
          throw new IllegalArgumentException("dept_range should be two positive integers that are separated by ','")
      }
    }

    val validWidthRange = (widthRange: String) => {
      val range = widthRange.split(",")
      (getInt(range(0)), if (range.length == 2) getInt(range(1)) else None) match {
        case (Some(start), Some(end)) if start > 0 & end > 0 & isPowerOfTwo(start) & isPowerOfTwo(end) =>
          ((Math.log(start)/Math.log(2)).toInt to (Math.log(end)/Math.log(2)).toInt).toList.map(v => Math.pow(2, v).toInt)
        case _ =>
          throw new IllegalArgumentException("width_range should be two positive integers that are powers of two separated by ','")
      }
    }

    val validBufferSize = (bufferSize: String) => getInt(bufferSize) match {
      case Some(v) if v > 0 => v
      case _ => throw new IllegalArgumentException("buffer_size should be a positive integer")
    }

    val validDataType = (dataType: String) => {
      if (dataType == "long" || dataType == "string") dataType
      else throw new IllegalArgumentException("datatype should be 'long' or 'string'")
    }

    val validFile = (file: String) => {
      if (FileUtil.exists(file)) file
      else throw new IllegalArgumentException("File does not exist")
    }

    val validators = List(validDepthRange, validWidthRange, validBufferSize, validDataType, validFile)
    val results = args.zip(validators).map { case (arg, validate) => Try(validate(arg)) }

    if (results.exists(_.isFailure)) {
      Left(results.filter(_.isFailure).map { case Failure(e) => e.getMessage }.toList)
    } else {
      val resultValues = results.map(_.get)
      (resultValues(0), resultValues(1), resultValues(2), resultValues(3), resultValues(4)) match {
        case t: (List[Int], List[Int], Int, String, String) => Right(BenchmarkArgs.tupled(t))
      }
    }
  }
}
