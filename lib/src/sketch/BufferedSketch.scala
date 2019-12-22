package sketch

import scala.collection.mutable

/**
  * Decorator for sketches that will buffer elements before actually updating the underlying sketch.
  * The buffer will hold a counter for each unique element. When `size` unique elements have been seen
  * the `add(elem, count)` function of the underlying sketch will be invoked. This is useful if the
  * `add` function of the underlying sketch is not O(1) time and there are relatively few unique elements
  * or elements arrive in groups.
  *
  * @param decorated the sketch instance to decorate.
  * @param size the size of the buffer.
  * @tparam T
  */
class BufferedSketch[T <: Sketch[T]](val decorated: T, size: Long)
  extends Sketch[BufferedSketch[T]] {
  private val buffer = mutable.Map.empty[String, Long]

  def flush() = {
    buffer.foreach { case (e, c) => decorated.add(e, c) }
    buffer.clear()
  }

  override def add(elem: String): BufferedSketch[T] = add(elem, 1L)

  override def add(elem: String, count: Long): BufferedSketch[T] = {
    buffer.update(elem, buffer.getOrElse(elem, 0L) + count)
    if (buffer.size > size) {
      flush()
    }
    this
  }

  override def merge(other: BufferedSketch[T]): BufferedSketch[T] = {
    flush()
    other.flush()
    new BufferedSketch[T](decorated.merge(other.decorated), size)
  }
}