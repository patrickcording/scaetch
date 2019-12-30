package sketch


abstract class SparkCountMinSketchWrapper[T](val depth: Int, val width: Int, val seed: Int)
  extends Sketch[SparkCountMinSketchWrapper[T], T] {
  val underlying = org.apache.spark.util.sketch.CountMinSketch.create(depth, width, seed)

  override def add(elem: T): SparkCountMinSketchWrapper[T] = add(elem, 1L)

  override def add(elem: T, count: Long): SparkCountMinSketchWrapper[T]

  override def merge(other: SparkCountMinSketchWrapper[T]): SparkCountMinSketchWrapper[T] = {
    underlying.mergeInPlace(other.underlying)
    this
  }

  override def estimate(elem: T): Long = {
    underlying.estimateCount(elem)
  }
}

object SparkCountMinSketchWrapper {
  def apply[T](depth: Int, width: Int, seed: Int)
              (implicit sk: (Int, Int, Int) => SparkCountMinSketchWrapper[T]): SparkCountMinSketchWrapper[T] = sk(depth, width, seed)

  implicit def longCountMinSketch(depth: Int, width: Int, seed: Int): SparkCountMinSketchWrapper[Long] = {
    new SparkCountMinSketchWrapper[Long](depth, width, seed) {
      override def add(elem: Long, count: Long): SparkCountMinSketchWrapper[Long] = {
        underlying.addLong(elem, count)
        this
      }
    }
  }

  implicit def stringCountMinSketch(depth: Int, width: Int, seed: Int): SparkCountMinSketchWrapper[String] = {
    new SparkCountMinSketchWrapper[String](depth, width, seed) {
      override def add(elem: String, count: Long): SparkCountMinSketchWrapper[String] = {
        underlying.addString(elem, count)
        this
      }
    }
  }
}
