package sketch

import scala.collection.mutable

class BufferedCountSketch(k: Int, t: Int, b: Int, seed: Int) extends CountSketch(k, t, b, seed) {

  val bufferSize: Int = 100000
  var itemBuffer: mutable.Map[String, Int] = mutable.Map[String, Int]().withDefaultValue(0)

  private def flushBuffer() = {
    itemBuffer.foreach { case (key, value) => super.add(key, value) }
    itemBuffer = itemBuffer.empty
  }

  override def add(data: String): CountSketch = {
    itemBuffer.update(data, itemBuffer(data) + 1)
    if (itemBuffer.size > bufferSize) flushBuffer()
    this
  }

  override def get: Seq[String] = {
    flushBuffer()
    super.get
  }

  override def merge(other: CountSketch): CountSketch = {
    flushBuffer()
    super.merge(other)
    this
  }

}