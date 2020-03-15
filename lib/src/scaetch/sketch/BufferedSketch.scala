package scaetch.sketch

import scaetch.ds.FixedSizeHashMap
import scaetch.sketch.hash.HashFunctionSimulator

/**
  * A decorator for putting an insertion buffer in front of instances of [[Sketch]].
  *
  * When inserting an element into a sketch a number of hash functions need to be
  * evaluated. Depending on the hash function, this can be costly. The sketches
  * support updating a counter by more than just +1 so for some data sources it
  * may improve performance to accumulate updates before actually pushing them to
  * the sketch.
  *
  * The buffer uses a [[FixedSizeHashMap]] to store counts for elements. When the
  * hash map has reached its capacity the elements and counts are inserted into
  * the decorated sketch.
  *
  * @param sketch     The [[Sketch]] to add a buffer to.
  * @param bufferSize The size of the buffer.
  * @tparam A         The type of the concrete implementation of this [[Sketch]].
  */
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
    // The hash function simulator associated with the current element is needed when
    // we actually insert into the sketch in the `flush` function. We cannot pass it
    // without erasing its type, so we use it here to create a new function for
    // inserting into the sketch.
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