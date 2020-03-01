package sketch

import ds.FixedSizeHashMap
import sketch.hash.HashFunctionSimulator


class BufferedSketch[A <: Sketch[A]](val sketch: A, val bufferSize: Int)
  extends Sketch[BufferedSketch[A]] with Serializable {

  private val buffer = new FixedSizeHashMap[(Long => A, Long)](bufferSize)

  def flush() = {
    buffer.getTable.foreach {
      case Some((_, (f, c))) => f(c)
      case None =>
    }
    buffer.clear()
  }

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): BufferedSketch[A] = {
    val sketchUpdateFunction = (c: Long) => sketch.add(elem, c)
    val hashMapUpdateFunction = (e: Option[(Long => A, Long)]) => e match {
      case Some((_, currentCount)) => (sketchUpdateFunction, count + currentCount)
      case None => (sketchUpdateFunction, count)
    }

    buffer.updateOrAdd(elem, hashMapUpdateFunction)

    if (buffer.getSize >= bufferSize/2) {
      flush()
    }

    this
  }

  override def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long = {
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