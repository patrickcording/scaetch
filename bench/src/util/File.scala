package util

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source

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

  def writeToResources[T](fileName: String, values: Seq[T]): Unit = {
    val file = new File("bench/resources", fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    for (v <- values) {
      bw.write(v.toString)
      bw.write("\n")
    }
    bw.close()
  }
}
