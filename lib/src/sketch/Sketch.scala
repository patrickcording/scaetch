package sketch

abstract class Sketch[T] extends Serializable {
  def add(elem: String): T
  def add(elem: String, count: Long): T
  def merge(other: T): T
  def estimate(elem: String): Long
}
