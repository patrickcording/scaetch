package scaetch.util

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source

object FileUtil {
  def readAs(path: String, dataType: String): Iterator[Any] = {
    val lines = Source.fromFile(path).getLines
    if (dataType == "string") {
      lines.map(_.asInstanceOf[Any])
    } else if (dataType == "long") {
      lines.map(_.toLong.asInstanceOf[Any])
    } else {
      throw new UnsupportedOperationException(s"Can not read data as $dataType")
    }
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
