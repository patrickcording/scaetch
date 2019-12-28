package sketch

import scala.collection.mutable
import scala.reflect.ClassTag


class BufferedSketch[T <: Sketch[T]](val sketch: T, val bufferSize: Int)(implicit val tag: ClassTag[T])
  extends Sketch[BufferedSketch[T]] with Serializable {

  private val buffer = mutable.Map.empty[String, Long]

  def flush() = {
    buffer.foreach { case (e, c) => sketch.add(e, c) }
    buffer.clear()
  }

  override def add(elem: String): BufferedSketch[T] = add(elem, 1L)

  override def add(elem: String, count: Long): BufferedSketch[T] = {
    buffer.update(elem, buffer.getOrElse(elem, 0L) + count)
    if (buffer.size > bufferSize) {
      flush()
    }
    this
  }

  override def merge(other: BufferedSketch[T]): BufferedSketch[T] = {
    flush()
    other.flush()
    val mergedSketch = sketch.merge(other.sketch)
    new BufferedSketch(mergedSketch, Math.min(bufferSize, other.bufferSize))
  }

  override def estimate(elem: String): Long = {
    flush()
    sketch.estimate(elem)
  }
}