package sketch

import hash.HashFunctionSimulator

import scala.collection.mutable

class SimpleHashMap[A](capacity: Int) {
  private val table: Array[Option[(Any, (Long => A, Long))]] = Array.fill(capacity)(None)
  private var size = 0
  private var misses = 0
  private val shift = 32 - (Math.log(capacity)/Math.log(2.0)).toInt
//  private val alive = mutable.BitSet

  def clear() = {
    var i = 0
    while (i < capacity) {
      table(i) = None
      i += 1
    }
    size = 0
  }

  def incrementOrAdd[T](key: T, sketch: Sketch[A], hash: HashFunctionSimulator[T], increment: Long) = {
    hash.set(key)
    var i = hash.hash(0) >>> shift
    var it = 0

    while(table(i).isDefined && table(i).get._1 != key) {
      i = hash.hash(it) >>> shift
      it += 1
    }

    val countFunction = (c: Long) => sketch.add(key, c)(hash)
    if (table(i).isEmpty) {
      table(i) = Some((key, (countFunction, increment)))
      size += 1
    } else {
      table(i) = Some((key, (countFunction, table(i).get._2._2 + increment)))
    }
  }

  def get() = table
  def getSize() = size
}

class BufferedSketch[A <: Sketch[A]](val sketch: A, val bufferSize: Int)
  extends Sketch[BufferedSketch[A]] with Serializable {

  private val buffer = new SimpleHashMap[A](bufferSize)

  def flush() = {
    buffer.get.foreach {
      case Some((_, (f, c))) => f(c)
      case None =>
    }
    buffer.clear()
  }

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): BufferedSketch[A] = {
    buffer.incrementOrAdd(elem, sketch, hash, 1L)
    if (buffer.getSize() >= bufferSize/2) {
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