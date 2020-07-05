package scaetch.sketch.application

import scaetch.sketch.Sketch
import scaetch.sketch.hash.HashFunctionSimulator

import scala.collection.mutable

class HeavyHitter(sketchFactory: () => Sketch) {
  private val dyadicTree = Array.tabulate[Sketch](64)(_ => sketchFactory())
  private var n = 0L

  def add(elem: Long)(implicit hash: HashFunctionSimulator[Long]): HeavyHitter = {
    // The dyadic intervals are represented implicitly by their start value and their depth.
    // For each level in the dyadic tree, a start value uniquely represents the interval containing it.
    // For example, on depth 0 we have two intervals [Long.MinValue, -1] and [0, Long.MaxValue]
    // which are just represented by Long.MinValue and 0. We can always compute the end value
    // of intervals.
    var s = Long.MinValue
    for (d <- 0 to 63) {
      if (d == 63) {
        dyadicTree(d).add(elem)
      } else {
        val mid = s - (Long.MinValue / math.pow(2, d)).toLong
        if (elem >= mid) {
          s = mid
        }
        dyadicTree(d).add(s)
      }
    }
    n += 1
    this
  }

  def get(phi: Double)(implicit hash: HashFunctionSimulator[Long]): List[Long] = {
    val heavyHitters = mutable.ListBuffer[Long]()
    val queue = mutable.Queue[(Int, Long)]() // (depth, s)
    queue.enqueue((0, Long.MinValue))
    queue.enqueue((0, 0L))

    while (queue.nonEmpty) {
      val (d, s) = queue.dequeue()
      val freq = dyadicTree(d).estimate(s) / n.toDouble
      if (freq > phi) {
        if (d == 63) {
          heavyHitters += s
        } else {
          queue.enqueue((d + 1, s - (Long.MinValue / math.pow(2, d + 1)).toLong))
          queue.enqueue((d + 1, s))
        }
      }
    }

    heavyHitters.toList
  }

}
