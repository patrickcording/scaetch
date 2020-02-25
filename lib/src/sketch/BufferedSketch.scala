package sketch

import hash.HashFunctionSimulator

class SimpleHashMap[A](size: Int) {
  private val table = Array.fill(size)(List.empty[(Any, (Long => A, Long))])
  private var inserts = 0
  private var sizeHint = 0

  def clear() = {
    var i = 0
    while (i < table.size) {
      table(i) = List.empty[(Any, (Long => A, Long))]
      i += 1
    }
    inserts = 0
    sizeHint = 0
  }

  private def incrementOrAddInternal(elem: (Any, (Long => A, Long)),
                                     ch: List[(Any, (Long => A, Long))]): List[(Any, (Long => A, Long))] = ch match {
    case h :: tail if h._1 == elem._1 => (elem._1, (elem._2._1, elem._2._2 + h._2._2)) :: tail
    case h :: tail => h :: incrementOrAddInternal(elem, tail)
    case Nil => List(elem)
  }

  def incrementOrAdd(key: Any, countFunction: Long => A, increment: Long) = {
    val index = key.## % size
    val chain = table(index)
    table(index) = incrementOrAddInternal((key, (countFunction, increment)), chain)
    inserts += 1
    if (inserts > size) {
      sizeHint = table.map(_.length).sum
      inserts = 0
    }
  }

  def get() = {
    table.flatten
  }

  def getSizeHint() = sizeHint
}

class BufferedSketch[A <: Sketch[A]](val sketch: A, val bufferSize: Int)
  extends Sketch[BufferedSketch[A]] with Serializable {

  private val buffer = new SimpleHashMap[A](bufferSize/10)

  def flush() = {
    buffer.get.foreach { case (_, (f, c)) => f(c) }
    buffer.clear()
  }

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): BufferedSketch[A] = {
    buffer.incrementOrAdd(elem, (c: Long) => sketch.add(elem, c), 1L)
    if (buffer.getSizeHint() >= bufferSize) {
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