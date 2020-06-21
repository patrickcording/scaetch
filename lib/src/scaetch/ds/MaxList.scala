package scaetch.ds

import scala.collection.mutable

/**
  * A data structure for maintaining the `k` heaviest elements.
  *
  * Elements and weights are kept in a map and when the size of the map exceeds 2*`k`, the `k` elements with
  * the lowest weight are evicted.
  *
  * @param k  The number of elements to report.
  * @tparam T The type of the elements to store.
  */
class MaxList[T](k: Int) {
  private val elements = mutable.Map.empty[T, Long]

  /**
    * Adds an element with weight `weight` to this [[MaxList]].
    *
    * @param weight The weight of the element to add. If the element already exists, the previous weight is
    *               discarded.
    * @param elem   The element to add.
    * @return       This [[MaxList]].
    */
  def add(weight: Long, elem: T): MaxList[T] = {
    elements.update(elem, weight)
    if (elements.size > 2 * k) {
      val min = elements.values.toArray.sorted.apply(elements.size / 2)
      elements.retain((_, v) => v > min)
    }
    this
  }

  /**
    * @return The `k` heaviest elements.
    */
  def get: List[T] = elements.toList.sortBy(-_._2).map(_._1).take(k)
}
