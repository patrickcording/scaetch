package scaetch.sketch.application

import scaetch.sketch.Sketch
import scaetch.sketch.hash.HashFunctionSimulator

import scala.collection.mutable

/**
 * A data structure for finding the heavy hitters (elements that occur more than a certain threshold times)
 * in a stream. Works with any of the count sketches implemented in Scætch.
 *
 * The algorithm is taken from "What’s Hot and What’s Not: Tracking Most Frequent Items Dynamically" by
 * G. Cormode and S. Muthukrishnan (https://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CormodeM-hot.pdf).
 * Basically, the universe is split into 1, 2, 4, ..., and N ranges organized as a tree. For each level in the
 * tree we maintain a count sketch. On some level d, all elements in the same range are treated as the same
 * element. Therefore, this class must be given a function that will create instances of the same type of
 * sketch. When calling `get(phi)` we traverse the sketches where the frequency is greater than phi.
 *
 * @param sketchFactory A function returning instances of the type of sketch to be used in each level of the
 *                      heavy hitter data structure.
 */
class HeavyHitter(sketchFactory: () => Sketch) {
  private val dyadicTree = Array.tabulate[Sketch](64)(_ => sketchFactory())
  private var n = 0L

  /**
   * Adds an element to the heavy hitter data structure. This operation will update 64 sketches of the
   * type given at instantiation.
   *
   * @param elem  The element to add.
   * @param hash  The hash function simulator to use.
   * @return      This [[HeavyHitter]].
   */
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

  /**
   * Returns all elements that occur more than phi * n times among the n elements added this data structure.
   *
   * @param phi   The threshold (0.0 < `phi`` < 1.0).
   * @param hash  The hash function simulator to use.
   * @return      A list of the heavy hitters.
   */
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
