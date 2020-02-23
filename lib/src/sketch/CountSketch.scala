//package sketch
//
//import net.openhft.hashing.LongHashFunction
//
//import scala.util.Random
//
///**
//  * An implementation of the CountSketch by Charikar, Chen, and Farach-Colton
//  * (`https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf`).
//  *
//  * This implementation uses the MurmurHash3 algorithm for hashing.
//  *
//  * @param depth the number of hash tables. Good values are O(log N).
//  * @param width the number of counters in each hash table. Has to be a power of two.
//  * @param seed the seed for chosing the hash functions used by the algorithm.
//  */
//class CountSketch(val depth: Int, val width: Int, val seed: Int) extends Sketch[CountSketch] {
//  require(
//    (Math.log(width)/Math.log(2)).isWhole,
//    s"Width must be a power of 2, $width is not a power of 2"
//  )
//  require(depth >= 1 && depth <= 64)
//  require(width >= 2 && width <= Math.pow(2, 16).toInt)
//
//  protected val buckets = Array.ofDim[Int](depth)
//  protected var counters = 0L
//  protected val shift = 64-(Math.log(width)/Math.log(2)).toInt
//  private val C = Array.ofDim[Long](depth, width)
//
//  private val r = new Random(seed)
//  private val A1 = Array.fill[Long](depth)(r.nextLong())
//  private val B1 = Array.fill[Long](depth)(r.nextLong())
//  private val A2 = r.nextLong()
//  private val B2 = r.nextLong()
//
//  private def setBucketsAndCounters(elem: String): Unit = {
//    val h1 = LongHashFunction.xx(seed)
//    val v1 = h1.hashChars(elem)
//    val h2 = LongHashFunction.xx(v1)
//    val v2 = h2.hashChars(elem)
//    val a = (v1 & 0xFFFFFFFFL).toInt
//    val b = (v1 >>> 32).toInt
//
//    var i = 0
//    while (i < depth) {
//      buckets(i) = (a*i + b) >>> shift
//      i += 1
//    }
//    counters = v2
//  }
//
//  private def setBucketsAndCounters(elem: Long): Unit = {
//    var i = 0
//    while (i < depth) {
//      buckets(i) = ((A1(i)*elem + B1(i)) >>> shift).toInt
//      i += 1
//    }
//    counters = A2*elem + B2
//  }
//
//  private def addInternal(count: Long) = {
//    var i = 0
//    while (i < depth) {
//      if ((counters >>> depth & 1L) == 0) C(i)(buckets(i)) += count
//      else C(i)(buckets(i)) -= count
//      i += 1
//    }
//  }
//
//  override def add(elem: String, count: Long): CountSketch = {
//    setBucketsAndCounters(elem)
//    addInternal(count)
//    this
//  }
//
//  override def add(elem: Long, count: Long): CountSketch = {
//    setBucketsAndCounters(elem)
//    addInternal(count)
//    this
//  }
//
//  private def estimateInternal(): Long = {
//    val values = Array.ofDim[Long](depth)
//    var i = 0
//    while (i < depth) {
//      if ((counters >>> depth & 1L) == 0) values(i) = C(i)(buckets(i))
//      else values(i) = -C(i)(buckets(i))
//      i += 1
//    }
//    values.sorted.apply(depth/2)
//  }
//
//  override def estimate(elem: String): Long = {
//    setBucketsAndCounters(elem)
//    estimateInternal()
//  }
//
//  override def estimate(elem: Long): Long = {
//    setBucketsAndCounters(elem)
//    estimateInternal()
//  }
//
//  override def merge(other: CountSketch): CountSketch = {
//    if (depth == other.depth && width == other.width && seed == other.seed) {
//      for (i <- 0 until depth; j <- 0 until width) {
//        C(i)(j) += other.C(i)(j)
//      }
//      var i, j = 0
//      while (i < depth) {
//        while (j < width) {
//          C(i)(j) += other.C(i)(j)
//          j += 1
//        }
//        i += 1
//      }
//      this
//    } else {
//      throw new Exception("Can't merge two sketches initialized with different parameters")
//    }
//  }
//}
//
//object CountSketch {
//  def apply(depth: Int, width: Int, seed: Int) = new CountSketch(depth, width, seed)
//}
