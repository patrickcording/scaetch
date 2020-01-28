package sketch

import net.openhft.hashing.LongHashFunction
import org.apache.spark.sql.catalyst.expressions.XXH64

import scala.util.Random
import scala.util.hashing.MurmurHash3


abstract class CountMinSketch[T](val depth: Int, val width: Int, val seed: Int) extends Sketch[CountMinSketch[T], T] {
  require(
    (Math.log(width)/Math.log(2)).isWhole(),
    s"Width must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  protected val C = Array.ofDim[Long](depth, width)
  protected val buckets = Array.ofDim[Int](depth)
  final protected val shift = 32-(Math.log(width)/Math.log(2)).toInt

  protected def getMin() = {
    var i = 0
    var min = Long.MaxValue
    while (i < depth) {
      min = Math.min(min, C(i)(buckets(i)))
      i += 1
    }
    min
  }

  def setBuckets(elem: T): Unit

  override def estimate(elem: T): Long = {
    setBuckets(elem)
    getMin()
  }

  override def add(elem: T): CountMinSketch[T] = {
    add(elem, 1)
    this
  }

  override def add(elem: T, occurrences: Long): CountMinSketch[T] = {
    setBuckets(elem)
    var i = 0
    while (i < depth) {
      C(i)(buckets(i)) += occurrences
      i += 1
    }
    this
  }

  override def merge(other: CountMinSketch[T]): CountMinSketch[T] = {
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
  def apply[T](depth: Int, width: Int, seed: Int)
              (implicit sk: (Int, Int, Int, Boolean) => CountMinSketch[T]): CountMinSketch[T] = sk(depth, width, seed, false)

  def apply[T](depth: Int, width: Int, seed: Int, enableConservativeUpdates: Boolean)
              (implicit sk: (Int, Int, Int, Boolean) => CountMinSketch[T]): CountMinSketch[T] = sk(depth, width, seed, enableConservativeUpdates)

  implicit def stringCountMinSketch(depth: Int, width: Int, seed: Int, consUpd: Boolean): CountMinSketch[String] = {
    if (consUpd) {
      new CountMinSketch[String](depth, width, seed) with StringHashing with ConservativeUpdates[String]
    } else {
      new CountMinSketch[String](depth, width, seed) with StringHashing
    }
  }

  implicit def longCountMinSketch(depth: Int, width: Int, seed: Int, consUpd: Boolean): CountMinSketch[Long] = {
    if (consUpd) {
      new CountMinSketch[Long](depth, width, seed) with LongHashing with ConservativeUpdates[Long]
    } else {
      new CountMinSketch[Long](depth, width, seed) with LongHashing
    }
  }

  trait ConservativeUpdates[T] extends CountMinSketch[T] {
    abstract override def add(elem: T, count: Long): CountMinSketch[T] = {
      require(count >= 0, "Negative updates not allowed when conservative updating is enabled")

      setBuckets(elem)
      val updateValue = count + super.getMin()

      var i = 0
      while (i < depth) {
        C(i)(buckets(i)) = Math.max(C(i)(buckets(i)), updateValue)
        i += 1
      }
      this
    }
  }

  trait StringHashing extends CountMinSketch[String] {
    private val h = LongHashFunction.xx(seed)

    override def setBuckets(elem: String): Unit = {
      val v = h.hashChars(elem)
      val hash1 = (v & 0xFFFFFFFFL).toInt
      val hash2 = (v >>> 32).toInt

      var i = 0
      while (i < depth) {
        buckets(i) = (hash1 + i*hash2) >>> shift
        i += 1
      }
    }
  }

  trait LongHashing extends CountMinSketch[Long] {
    private val r = new Random(seed)
    private val A1 = Array.fill[Long](depth)(r.nextLong())
    private val B1 = Array.fill[Long](depth)(r.nextLong())

    override def setBuckets(elem: Long): Unit = {
      var i = 0
      while (i < depth) {
        val h1 = A1(i)*elem + B1(i)
        buckets(i) = h1.toInt >>> shift
        i += 1
      }
    }
  }
}
