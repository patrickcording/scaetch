package sketch

import scala.util.hashing.MurmurHash3

class CountMinSketch(val depth: Int, val width: Int, val seed: Int) extends Sketch[CountMinSketch] {
  require(
    (Math.log(width)/Math.log(2)).isWhole(),
    s"Parameter b must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  private val C = Array.ofDim[Long](depth, width)
  private val buckets = Array.ofDim[Int](depth)
  private val shift = 32-(Math.log(width)/Math.log(2)).toInt

  private def setBuckets(elem: String): Unit = {
    val hash1 = MurmurHash3.stringHash(elem, seed)
    val hash2 = MurmurHash3.stringHash(elem, hash1)
    for (i <- 0 until depth) {
      val h1 = hash1 + i*hash2
      buckets(i) = h1 >>> shift
    }
  }

  override def estimate(elem: String): Long = {
    setBuckets(elem)
    (0 until depth).map(i => C(i)(buckets(i))).min
  }

  override def add(data: String): CountMinSketch = {
    add(data, 1)
    this
  }

  override def add(data: String, occurrences: Long): CountMinSketch = {
    setBuckets(data)
    for (i <- 0 until depth) {
      C(i)(buckets(i)) += occurrences
    }
    this
  }

  override def merge(other: CountMinSketch): CountMinSketch = {
    if (depth == other.depth && width == other.width && seed == other.seed) {
      for (i <- 0 until depth; j <- 0 until width) {
        C(i)(j) = Math.min(C(i)(j), other.C(i)(j))
      }
      this
    } else {
      throw new Exception("Can't merge two sketches initialized with different parameters")
    }
  }
}
