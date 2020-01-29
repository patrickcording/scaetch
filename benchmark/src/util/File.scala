package util

import java.nio.file.{Files, Paths}

import scala.io.Source
import scala.util.Random

object File {
  def readLongs(path: String): Iterator[Long] = {
    Source.fromFile(path).getLines.map(_.toLong)
  }

  def readStrings(path: String): Iterator[String] = {
    Source.fromFile(path).getLines.map(_.toString)
  }

  def exists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  val r = new Random(10)
  r.nextDouble()
}
