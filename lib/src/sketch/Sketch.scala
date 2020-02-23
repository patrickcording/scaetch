package sketch

abstract class Sketch[A] extends Serializable {
  def add(elem: String, count: Long): A
  def add(elem: Long, count: Long): A
  def estimate(elem: String): Long
  def estimate(elem: Long): Long
  def merge(other: A): A
}
