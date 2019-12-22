package sketch

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * An implementation of the CountSketch by Charikar, Chen, and Farach-Colton
  * (`https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf`).
  *
  * This implementation uses the MurmurHash3 algorithm for hashing.
  *
  * @param k the number of most frequent elements to maintain by the CountSketch.
  * @param t see algorithm description. Good values are O(log N).
  * @param b see algorithm for description. In many cases, a good value is >= 2*k. Has to be a power of two.
  * @param seed the seed for chosing the hash functions used by the algorithm.
  */
class CountSketch(k: Int, t: Int, b: Int, seed: Int) {
  require(
    (Math.log(b)/Math.log(2)) - (Math.log(b)/Math.log(2)).toInt.toDouble == 0.0,
    s"Parameter b must be a power of 2, $b is not a power of 2"
  )
  require(k >= 1)
  require(t >= 1)
  require(b >= 2)

  /**
    * Definitions of hash functions. We are using Murmur3 for both.
    */
  private val shift = (Math.log(b)/Math.log(2)).toInt
  private def h(_seed: Int)(data: String) = MurmurHash3.stringHash(data, _seed) >>> (32-shift)
  private def s(_seed: Int)(data: String) = if ((MurmurHash3.stringHash(data, _seed) & 1) == 0) -1 else 1

  /**
    * Initialise hash functions with random seeds. The seeds are based on a global seed so that sketches can be
    * merged.
    */
  private val r = new Random(seed)
  private val counterHashFunctions = (1 to t).map(_ => r.nextInt()).map(s)
  private val bucketHashFunctions = (1 to t).map(_ => r.nextInt()).map(h)

  /**
    * Internal data structure.
    */
  private val C = Array.ofDim[Long](t, b)

  /**
    * Helper functions.
    */
  private def mean(arr: Seq[Long]): Long = arr.sorted.apply(arr.length/2)

  /**
    * Estimate the frequency of an element.
    * @param elem
    * @return estimated frequency
    */
  def estimate(elem: String): Long = {
    val counterValues = (0 until t).map(idx => C(idx)(bucketHashFunctions(idx)(elem)) * counterHashFunctions(idx)(elem))
    mean(counterValues)
  }

  /**
    * Add a data point to the CountSketch.
    * @param data
    * @return this CountSketch
    */
  def add(data: String): CountSketch = {
    add(data, 1)
    this
  }

  def add(data: String, occurrences: Int): CountSketch = {
    // Update counters
    (0 until t).foreach(idx => C(idx)(bucketHashFunctions(idx)(data)) += occurrences * counterHashFunctions(idx)(data))
    this
  }

  /**
    * Merge this CountSketch with `other` CountSketch.
    * @param other
    * @return
    */
  def merge(other: CountSketch): CountSketch = {
    for (i <- 0 until t; j <- 0 until b) { C(i)(j) += other.C(i)(j) }
    this
  }

}
