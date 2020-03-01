package sketch

import sketch.hash.HashFunctionSimulator

abstract class Sketch[A] extends Serializable {
  def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): A
  def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long
  def merge(other: A): A
}
