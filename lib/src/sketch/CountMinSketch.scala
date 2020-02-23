package sketch

import net.openhft.hashing.LongHashFunction

import scala.util.Random


class CountMinSketch(val depth: Int, val width: Int, val seed: Int) extends Sketch[CountMinSketch] {
  require(
    (Math.log(width)/Math.log(2)).isWhole,
    s"Width must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  protected val C = Array.ofDim[Long](depth, width)
  protected val buckets = Array.ofDim[Int](depth)
  final protected val shift = 32-(Math.log(width)/Math.log(2)).toInt

  private val h = LongHashFunction.xx(seed)
  private val r = new Random(seed)
  private val A1 = Array.fill[Long](depth)(r.nextLong())
  private val B1 = Array.fill[Long](depth)(r.nextLong())

  protected def setBuckets(elem: String) = {
    val v = h.hashChars(elem)
    val hash1 = (v & 0xFFFFFFFFL).toInt
    val hash2 = (v >>> 32).toInt

    var i = 0
    while (i < depth) {
      buckets(i) = (hash1 + i*hash2) >>> shift
      i += 1
    }
  }

  protected def setBuckets(elem: Long) = {
    var i = 0
    while (i < depth) {
      buckets(i) = (A1(i)*elem + B1(i) >>> shift).toInt
      i += 1
    }
  }

  protected def addInternal(count: Long) = {
    var i = 0
    while (i < depth) {
      C(i)(buckets(i)) += count
      i += 1
    }
  }

  override def add(elem: String, count: Long): CountMinSketch = {
    setBuckets(elem)
    addInternal(count)
    this
  }

  override def add(elem: Long, count: Long): CountMinSketch = {
    setBuckets(elem)
    addInternal(count)
    this
  }

  protected def estimateInternal() = {
    var i = 0
    var min = Long.MaxValue
    while (i < depth) {
      min = Math.min(min, C(i)(buckets(i)))
      i += 1
    }
    min
  }

  override def estimate(elem: String): Long = {
    setBuckets(elem)
    estimateInternal()
  }

  override def estimate(elem: Long): Long = {
    setBuckets(elem)
    estimateInternal()
  }

  override def merge(other: CountMinSketch): CountMinSketch = {
    if (depth == other.depth && width == other.width && seed == other.seed) {
      var i, j = 0
      while (i < depth) {
        while(j < width) {
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

object CountMinSketch {
  trait ConservativeUpdates extends CountMinSketch {
    protected override def addInternal(count: Long) = {
      val updateValue = count + super.estimateInternal()

      var i = 0
      while (i < depth) {
        C(i)(buckets(i)) = Math.max(C(i)(buckets(i)), updateValue)
        i += 1
      }
    }

    abstract override def add(elem: String, count: Long): CountMinSketch = {
      require(count >= 0, "Negative updates not allowed when conservative updating is enabled")
      setBuckets(elem)
      addInternal(count)
      this
    }

    abstract override def add(elem: Long, count: Long): CountMinSketch = {
      require(count >= 0, "Negative updates not allowed when conservative updating is enabled")
      setBuckets(elem)
      addInternal(count)
      this
    }
  }



}
