package sketch

abstract class Sketch[A, T] extends Serializable {
  def add(elem: T): A
  def add(elem: T, count: Long): A
  def merge(other: A): A
  def estimate(elem: T): Long
}
