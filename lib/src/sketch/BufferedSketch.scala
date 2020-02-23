//package sketch
//
//import scala.collection.mutable
//
//
//class BufferedSketch[A[T] <: Sketch[A[T], T], T](val sketch: A[T], val bufferSize: Int)
//  extends Sketch[BufferedSketch[A, T], T] with Serializable {
//
//  private val buffer = mutable.Map.empty[T, Long]
//
//  def flush() = {
//    buffer.foreach { case (e, c) => sketch.add(e, c) }
//    buffer.clear()
//  }
//
//  override def add(elem: T): BufferedSketch[A, T] = add(elem, 1L)
//
//  override def add(elem: T, count: Long): BufferedSketch[A, T] = {
//    buffer.update(elem, buffer.getOrElse(elem, 0L) + count)
//    if (buffer.size > bufferSize) {
//      flush()
//    }
//    this
//  }
//
//  override def merge(other: BufferedSketch[A, T]): BufferedSketch[A, T] = {
//    flush()
//    other.flush()
//    val mergedSketch = sketch.merge(other.sketch)
//    new BufferedSketch(mergedSketch, Math.min(bufferSize, other.bufferSize))
//  }
//
//  override def estimate(elem: T): Long = {
//    flush()
//    sketch.estimate(elem)
//  }
//}