package sketch

import scala.collection.mutable


class BufferedSketch[A <: Sketch[A]](val sketch: A, val bufferSize: Int)
  extends Sketch[BufferedSketch[A]] with Serializable {

  private val buffer = mutable.Map.empty[Any, Long]

  def flush() = {
    buffer.foreach {
      case (e, c) => e match {
        case stringElement: String => sketch.add(stringElement, c)
        case longElement: Long => sketch.add(longElement, c)
      }
    }
    buffer.clear()
  }

  private def addInternal(elem: Any, count: Long) = {
    buffer.update(elem, buffer.getOrElse(elem, 0L) + count)
    if (buffer.size > bufferSize) {
      flush ()
    }
    this
  }

  override def add(elem: String, count: Long): BufferedSketch[A] = {
    addInternal(elem, count)
  }

  override def add(elem: Long, count: Long): BufferedSketch[A] = {
    addInternal(elem, count)
  }

  override def estimate(elem: String): Long = {
    flush()
    sketch.estimate(elem)
  }

  override def estimate(elem: Long): Long = {
    flush()
    sketch.estimate(elem)
  }

  override def merge(other: BufferedSketch[A]): BufferedSketch[A] = {
    flush()
    other.flush()
    val mergedSketch = sketch.merge(other.sketch)
    new BufferedSketch(mergedSketch, Math.min(bufferSize, other.bufferSize))
  }
}