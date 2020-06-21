package scaetch.util

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source

object FileUtil {
  // This stuff is here so that we can select a conversion based on the type
  // parameter given for readAs. It would not have been necessary if .asInstanceOf[Long]
  // was equivalent to .toLong, but it isn't.
  trait ReadAs[T] {
    def convert(it: Iterator[String]): Iterator[T]
  }
  class ReadAsString extends ReadAs[String] {
    override def convert(it: Iterator[String]): Iterator[String] = it
  }
  class ReadAsLong extends ReadAs[Long] {
    override def convert(it: Iterator[String]): Iterator[Long] = {
      it.map(_.toLong)
    }
  }
  implicit val readAsString = new ReadAsString
  implicit val readAsLong = new ReadAsLong

  def readAs[T](path: String)(implicit ra: ReadAs[T]): Iterator[T] = {
    val lines = Source.fromFile(path).getLines
    ra.convert(lines)
  }

  def exists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  def writeToResources[T](fileName: String, values: Seq[T]): Unit = {
    val file = new File("scaetch/bench/resources", fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    for (v <- values) {
      bw.write(v.toString)
      bw.write("\n")
    }
    bw.close()
  }
}
