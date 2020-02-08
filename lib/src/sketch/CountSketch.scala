package sketch

import net.openhft.hashing.LongHashFunction

import scala.util.Random

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
    (Math.log(width)/Math.log(2)).isWhole,
    s"Width must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1 && depth <= 64)
  require(width >= 2 && width <= Math.pow(2, 16).toInt)

  protected val buckets = Array.ofDim[Int](depth)
  protected var counters = 0L
  protected val shift = 64-(Math.log(width)/Math.log(2)).toInt
  private val C = Array.ofDim[Long](depth, width)

  def setBucketsAndCounters(elem: T)

  /**
    * Estimate the frequency of an element.
    * @param elem
    * @return estimated frequency
    */
  override def estimate(elem: T): Long = {
    setBucketsAndCounters(elem)
    val values = Array.ofDim[Long](depth)
    var i = 0
    while (i < depth) {
      if ((counters >>> depth & 1L) == 0) values(i) = C(i)(buckets(i))
      else values(i) = -C(i)(buckets(i))
      i += 1
    }
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
    setBucketsAndCounters(elem)
    var i = 0
    while (i < depth) {
      if ((counters >>> depth & 1L) == 0) C(i)(buckets(i)) += occurrences
      else C(i)(buckets(i)) -= occurrences
      i += 1
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
      val h1 = LongHashFunction.xx(seed)
      val v1 = h1.hashChars(elem)
      val h2 = LongHashFunction.xx(v1)
      val v2 = h2.hashChars(elem)
      val a = (v1 & 0xFFFFFFFFL).toInt
      val b = (v1 >>> 32).toInt

      var i = 0
      while (i < depth) {
        buckets(i) = (a*i + b) >>> shift
        i += 1
      }
      counters = v2
    }
  }

  trait LongHashing extends CountSketch[Long] {
    private val r = new Random(seed)
    private val A1 = Array.fill[Long](depth)(r.nextLong())
    private val B1 = Array.fill[Long](depth)(r.nextLong())
    private val A2 = r.nextLong()
    private val B2 = r.nextLong()

    override def setBucketsAndCounters(elem: Long): Unit = {
      var i = 0
      while (i < depth) {
        buckets(i) = ((A1(i)*elem + B1(i)) >>> shift).toInt
        i += 1
      }
      counters = A2*elem + B2
    }
  }
}
