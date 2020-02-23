package sketch

trait SketchUpdateStateFunction[A, T] {
  def apply(elem: T, sketchInstance: A)
}

abstract class Sketch[A] extends Serializable {
  def add[T](elem: T, count: Long)(implicit updateFunction: SketchUpdateStateFunction[A, T]): A
  def estimate[T](elem: T)(implicit updateFunction: SketchUpdateStateFunction[A, T]): Long
  def merge(other: A): A

//  def add[T](elem: T)(implicit updateFunction: T => Unit): A = add(elem, 1L)
}
