package sketch

import hash.HashFunctionSimulator


class CountMinSketch(val depth: Int, val width: Int) extends Sketch[CountMinSketch] {
  require(
    (Math.log(width)/Math.log(2)).isWhole,
    s"Width must be a power of 2, $width is not a power of 2"
  )
  require(depth >= 1)
  require(width >= 2)

  protected val C = Array.ofDim[Long](depth, width)
  protected val shift = 32-(Math.log(width)/Math.log(2)).toInt

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): CountMinSketch = {
    hash.set(elem)

    var i = 0
    while (i < depth) {
      C(i)(hash.hash(i) >>> shift) += count
      i += 1
    }
    this
  }

  override def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long = {
    hash.set(elem)

    var i = 0
    var min = Long.MaxValue
    while (i < depth) {
      min = Math.min(min, C(i)(hash.hash(i) >>> shift))
      i += 1
    }
    min
  }

  override def merge(other: CountMinSketch): CountMinSketch = {
    // TODO: implement a check for change in hash function
    if (depth == other.depth && width == other.width) {
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
  def apply(depth: Int, width: Int) = new CountMinSketch(depth, width)
  def withConservativeUpdates(depth: Int, width: Int) = new CountMinSketch(depth, width) with ConservativeUpdates

  trait ConservativeUpdates extends CountMinSketch {
    override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): ConservativeUpdates = {
      val updateValue = count + super.estimate(elem)

      var i = 0
      while (i < depth) {
        val bucket = hash.hash(i) >>> shift
        C(i)(bucket) = Math.max(C(i)(bucket), updateValue)
        i += 1
      }
      this
    }
  }
}
