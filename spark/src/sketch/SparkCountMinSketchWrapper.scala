package sketch


class SparkCountMinSketchWrapper(val depth: Int, val width: Int, val seed: Int)
  extends Sketch[SparkCountMinSketchWrapper] {
  val underlying = org.apache.spark.util.sketch.CountMinSketch.create(depth, width, seed)

  override def add(elem: String, count: Long): SparkCountMinSketchWrapper = {
    underlying.addString(elem, count)
    this
  }

  override def add(elem: Long, count: Long): SparkCountMinSketchWrapper = {
    underlying.addLong(elem, count)
    this
  }

  override def merge(other: SparkCountMinSketchWrapper): SparkCountMinSketchWrapper = {
    underlying.mergeInPlace(other.underlying)
    this
  }

  override def estimate(elem: String): Long = {
    underlying.estimateCount(elem)
  }

  override def estimate(elem: Long): Long = {
    underlying.estimateCount(elem)
  }
}

object SparkCountMinSketchWrapper {
  def apply(depth: Int, width: Int, seed: Int) = new SparkCountMinSketchWrapper(depth, width, seed)
}
