package sketch

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * An implementation of the CountSketch by Charikar, Chen, and Farach-Colton
  * (`https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf`).
  *
  * This implementation uses the MurmurHash3 algorithm for hashing.
  *
  * @param depth the number of hash tables. Good values are O(log N).
  * @param width the number of counters in each hash table. Has to be a power of two.
  * @param seed the seed for chosing the hash functions used by the algorithm.
  */
abstract class CountSketch[T](val depth: Int, val width: Int, val seed: Int) extends Sketch[CountSketch[T], T] {
  require(
    (Math.log(width)/Math.log(2)).isWhole(),
    s"Parameter b must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  protected val buckets = Array.ofDim[Int](depth)
  protected val counters = Array.ofDim[Int](depth)
  protected val shift = 32-(Math.log(width)/Math.log(2)).toInt
  private val C = Array.ofDim[Long](depth, width)

  def setBucketsAndCounters(elem: T)

  /**
    * Estimate the frequency of an element.
    * @param elem
    * @return estimated frequency
    */
  override def estimate(elem: T): Long = {
    setBucketsAndCounters(elem)
    val values = (0 until depth).map(i => C(i)(buckets(i)) * counters(i))
    values.sorted.apply(depth/2)
  }

  /**
    * Add a data point to the CountSketch.
    * @param data
    * @return this CountSketch
    */
  override def add(elem: T): CountSketch[T] = {
    add(elem, 1)
    this
  }

  override def add(elem: T, occurrences: Long): CountSketch[T] = {
    // Update counters
    setBucketsAndCounters(elem)
    for (i <- 0 until depth) {
      C(i)(buckets(i)) += occurrences * counters(i)
    }
    this
  }

  /**
    * Merge this CountSketch with `other` CountSketch.
    * @param other
    * @return
    */
  override def merge(other: CountSketch[T]): CountSketch[T] = {
    if (depth == other.depth && width == other.width && seed == other.seed) {
      for (i <- 0 until depth; j <- 0 until width) {
        C(i)(j) += other.C(i)(j)
      }
      this
    } else {
      throw new Exception("Can't merge two sketches initialized with different parameters")
    }
  }
}

object CountSketch {
  def apply[T](depth: Int, width: Int, seed: Int)
              (implicit sk: (Int, Int, Int) => CountSketch[T]): CountSketch[T] = sk(depth, width, seed)

  implicit def stringCountSketch(depth: Int, width: Int, seed: Int): CountSketch[String] = {
    new CountSketch[String](depth, width, seed) with StringHashing
  }

  implicit def longCountSketch(depth: Int, width: Int, seed: Int): CountSketch[Long] = {
    new CountSketch[Long](depth, width, seed) with LongHashing
  }

  trait StringHashing extends CountSketch[String] {
    override def setBucketsAndCounters(elem: String): Unit = {
      val hash1 = MurmurHash3.stringHash(elem, seed)
      val hash2 = MurmurHash3.stringHash(elem, hash1)
      val hash3 = MurmurHash3.stringHash(elem, hash2)
      val hash4 = MurmurHash3.stringHash(elem, hash3)
      for (i <- 0 until depth) {
        val h1 = hash1 + i*hash2
        val h2 = hash3 + i*hash4
        buckets(i) = h1 >>> shift
        counters(i) = if ((h2 & 1) == 0) -1 else 1
      }
    }
  }

  trait LongHashing extends CountSketch[Long] {
    private val r = new Random(seed)
    private val A1 = Array.fill[Long](depth)(r.nextLong())
    private val B1 = Array.fill[Long](depth)(r.nextLong())
    private val A2 = Array.fill[Long](depth)(r.nextLong())
    private val B2 = Array.fill[Long](depth)(r.nextLong())

    override def setBucketsAndCounters(elem: Long): Unit = {
      for (i <- 0 until depth) {
        val h1 = A1(i)*elem + B1(i)
        val h2 = A2(i)*elem + B2(i)
        buckets(i) = h1.toInt >>> shift
        counters(i) = if ((h2 & 1) == 0) -1 else 1
      }
    }
  }
}
