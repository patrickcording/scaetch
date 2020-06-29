package scaetch.sketch

import scaetch.ds.MaxList
import scaetch.sketch.hash.HashFunctionSimulator


class TopK[T](k: Int, sketch: Sketch) {
  private val topK = new MaxList[T](k)

  def add(elem: T)(implicit hash: HashFunctionSimulator[T]): TopK[T] = {
    val elemCount = sketch.add(elem).estimate(elem)
    topK.add(elemCount, elem)
    this
  }

  def get: List[T] = topK.get
}
