package util

import scala.collection.mutable

class MaxList[T](size: Int) {

  private val elements = mutable.Map.empty[T, Int]

  def add(p: Int, data: T): MaxList[T] = {
    elements.update(data, p)
    if (elements.size > 2 * size) {
      val min = elements.values.toArray.sorted.apply(elements.size / 2)
      elements.retain((_, v) => v > min)
    }
    this
  }

  def get: Seq[T] = elements.toSeq.sortBy(-_._2).map(_._1).take(size)

}
object MaxList {

  def empty[T](size: Int) = new MaxList[T](size)

}
