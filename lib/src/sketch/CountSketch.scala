package sketch

import hash.HashFunctionSimulator

/**
  * An implementation of the CountSketch by Charikar, Chen, and Farach-Colton
  * (`https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf`).
  *
  * This implementation uses the MurmurHash3 algorithm for hashing.
  *
  * @param depth the number of hash tables. Good values are O(log N).
  * @param width the number of counters in each hash table. Has to be a power of two.
  */
class CountSketch(val depth: Int, val width: Int) extends Sketch[CountSketch] {
  require(
    (Math.log(width)/Math.log(2)).isWhole,
    s"Width must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1 && depth <= 64)
  require(width >= 2 && width <= Math.pow(2, 16).toInt)

  private val C = Array.ofDim[Long](depth, width)
  private val shift = 32-(Math.log(width)/Math.log(2)).toInt

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): CountSketch = {
    hash.set(elem)

    val counters = hash.hash(-1) >>> shift

    var i = 0
    while (i < depth) {
      val bucket = hash.hash(i) >>> shift
      if ((counters >>> depth & 1L) == 0) C(i)(bucket) += count
      else C(i)(bucket) -= count
      i += 1
    }
    this
  }

  override def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long = {
    hash.set(elem)
    val values = Array.ofDim[Long](depth)
    val counters = hash.hash(-1) >>> shift

    var i = 0
    while (i < depth) {
      val bucket = hash.hash(i) >>> shift
      if ((counters >>> depth & 1L) == 0) values(i) = C(i)(bucket)
      else values(i) = -C(i)(bucket)
      i += 1
    }
    values.sorted.apply(depth/2)
  }

  override def merge(other: CountSketch): CountSketch = {
    if (depth == other.depth && width == other.width) {
      for (i <- 0 until depth; j <- 0 until width) {
        C(i)(j) += other.C(i)(j)
      }
      var i, j = 0
      while (i < depth) {
        while (j < width) {
          C(i)(j) += other.C(i)(j)
          j += 1
        }
        i += 1
      }
      this
    } else {
      throw new Exception("Can't merge two sketches initialized with different parameters")
    }
  }
}

object CountSketch {
  def apply(depth: Int, width: Int) = new CountSketch(depth, width)
}
