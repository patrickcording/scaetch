package scaetch.sketch

import scaetch.sketch.hash.HashFunctionSimulator

/**
  * A wrapper for the CountMin sketch implementation in Spark.
  */
class SparkCountMinSketchWrapper(val depth: Int, val width: Int, val seed: Int)
  extends Sketch with SketchLike[SparkCountMinSketchWrapper] {
  val underlying = org.apache.spark.util.sketch.CountMinSketch.create(depth, width, seed)

  override def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): SparkCountMinSketchWrapper = {
    underlying.add(elem, count)
    this
  }

  override def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long = {
    underlying.estimateCount(elem)
  }

  override def merge(other: SparkCountMinSketchWrapper): SparkCountMinSketchWrapper = {
    underlying.mergeInPlace(other.underlying)
    this
  }
}

object SparkCountMinSketchWrapper {
  def apply(depth: Int, width: Int, seed: Int) = new SparkCountMinSketchWrapper(depth, width, seed)
}
