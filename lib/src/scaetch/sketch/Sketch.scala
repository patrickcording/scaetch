package scaetch.sketch

import scaetch.sketch.hash.HashFunctionSimulator

/**
  * Base trait for sketch implementations. Sketches must extend this and make sure that
  * the return type for the `add` function is overridden by the type of the extending
  * class. Moreover, a [[SketchLike]] must be mixed in when extending to provide the type
  * of the extending class to `merge`. This will also provide a shorthand function for
  * `add(elem, 1)`.
  */
trait Sketch {
  /**
    * Adds `count` to the count of `elem`. Extending classes must implement this function.
    *
    * @param elem   The element.
    * @param count  The count to add.
    * @param hash   The hash function simulator to use.
    * @tparam T     The type of the element to add.
    * @return This [[SketchLike]] as an instance of `A`.
    */
  def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): Sketch

  /**
    * Adds 1 to the count of `elem`.
    *
    * @see [[Sketch.add[T](elem: T, count: Long)]]
    */
  def add[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Sketch

  /**
    * Estimates the count of `elem`. Extending classes must implement this function.
    *
    * @param elem The element.
    * @param hash The hash function simulator to use.
    * @tparam T   The type of the element to estimate.
    * @return     An approximate count for `elem`.
    */
  def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long
}
