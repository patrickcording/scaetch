package sketch

import scala.util.Random
import scala.util.hashing.MurmurHash3


abstract class CountMinSketch[T](val depth: Int, val width: Int, val seed: Int) extends Sketch[CountMinSketch[T], T] {
  require(
    (Math.log(width)/Math.log(2)).isWhole(),
    s"Parameter b must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  protected val C = Array.ofDim[Long](depth, width)
  protected val buckets = Array.ofDim[Int](depth)
  protected val shift = 32-(Math.log(width)/Math.log(2)).toInt

  def setBuckets(elem: T): Unit

  override def estimate(elem: T): Long = {
    setBuckets(elem)
    (0 until depth).map(i => C(i)(buckets(i))).min
  }

  override def add(elem: T): CountMinSketch[T] = {
    add(elem, 1)
    this
  }

  override def add(elem: T, occurrences: Long): CountMinSketch[T] = {
    setBuckets(elem)
    for (i <- 0 until depth) {
      C(i)(buckets(i)) += occurrences
    }
    this
  }

  override def merge(other: CountMinSketch[T]): CountMinSketch[T] = {
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
      setBuckets(elem)
      val updateValue = count + (0 until depth).map(i => C(i)(buckets(i))).min
      for (i <- 0 until depth) {
        C(i)(buckets(i)) = Math.max(C(i)(buckets(i)), updateValue)
      }
      this
    }
  }

  trait StringHashing extends CountMinSketch[String] {
    override def setBuckets(elem: String): Unit = {
      val hash1 = MurmurHash3.stringHash(elem, seed)
      val hash2 = MurmurHash3.stringHash(elem, hash1)
      for (i <- 0 until depth) {
        buckets(i) = (hash1 + i*hash2) >>> shift
      }
    }
  }

  trait LongHashing extends CountMinSketch[Long] {
    private val r = new Random(seed)
    private val A1 = Array.fill[Long](depth)(r.nextLong())
    private val B1 = Array.fill[Long](depth)(r.nextLong())

    override def setBuckets(elem: Long): Unit = {
      for (i <- 0 until depth) {
        val h1 = A1(i)*elem + B1(i)
        buckets(i) = h1.toInt >>> shift
      }
    }
  }
}
