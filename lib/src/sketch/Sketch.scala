package sketch

abstract class Sketch[A] extends Serializable {
  def add[T](elem: T, count: Long): A
  def estimate[T](elem: T): Long
  def merge(other: A): A
}
