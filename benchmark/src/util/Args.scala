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

  def parse(args: Array[String]): BenchmarkArgs = {
    if (args.length != 4) {
      throw new IllegalArgumentException(s"""Incorrect number of arguments\n\n""" + usage)
    }

    val depth = Try { args(1).toInt }
      .getOrElse(throw new IllegalArgumentException("depth should be an integer.\n\n" + usage))

    val rangeArray = Try {
      val range = args(2).split(",").map(_.toInt)
      if (range.length != 2) throw Exception
      range
    }.getOrElse(throw new IllegalArgumentException("width_range should be two integers separated by ','.\n\n" + usage))
    val widthRange = rangeArray(0) to rangeArray(1)

    val k = Try { args(3).toInt }
      .getOrElse(throw new IllegalArgumentException("k should be an integer.\n\n" + usage))

    val file = if (File.exists(args(4))) args(4) else throw new IllegalArgumentException("File does not exist.\n\n" + usage)

    BenchmarkArgs(depth, widthRange, k, file)
  }

}
