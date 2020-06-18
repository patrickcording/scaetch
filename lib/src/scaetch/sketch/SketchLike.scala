package scaetch.sketch

import scaetch.sketch.hash.HashFunctionSimulator

/**
  * @see [[Sketch]]
  * @tparam A Type of sketch implementing this trait.
  */
trait SketchLike[A <: Sketch] extends Serializable {
  // A SketchLike implementation must be provided when extending Sketch
  this: Sketch =>

  /**
    * Adds 1 to the count of `elem`.
    *
    * @see [[Sketch.add(elem: T)]]
    */
  override def add[T](elem: T)(implicit hash: HashFunctionSimulator[T]): A = {
    this.add(elem, 1L).asInstanceOf[A]
  }

  /**
    * Merges this sketch with `other`. Extending classes must implement this function.
    *
    * @param other The sketch to merge with this sketch.
    * @return      The sketch resulting from the merge.
    */
  def merge(other: A): A
}
