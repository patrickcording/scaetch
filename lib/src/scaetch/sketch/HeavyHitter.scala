package scaetch.sketch

import scaetch.ds.MaxList
import scaetch.sketch.hash.HashFunctionSimulator

class HeavyHitter[T, A <: Sketch[A]](k: Int, sketch: => A) {
  private val topK = new MaxList[T](k)

  def add(elem: T)(implicit hash: HashFunctionSimulator[T]): HeavyHitter[T, A] = {
    val elemCount = sketch.add(elem).estimate(elem)
    topK.add(elemCount, elem)
    this
  }

  def get: List[T] = topK.get
}
