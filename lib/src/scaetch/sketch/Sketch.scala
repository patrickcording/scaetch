package scaetch.sketch

import scaetch.sketch.hash.HashFunctionSimulator

abstract class Sketch[A] extends Serializable {
  def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): A
  def add[T](elem: T)(implicit hash: HashFunctionSimulator[T]): A = add(elem, 1L)
  def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long
  def merge(other: A): A
}
