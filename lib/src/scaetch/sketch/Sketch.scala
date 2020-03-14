package scaetch.sketch

import scaetch.sketch.hash.HashFunctionSimulator

/**
  * Base class for sketch implementations.
  *
  * @tparam A The type of the concrete implementation of this [[Sketch]].
  */
abstract class Sketch[A] extends Serializable {
  /**
    * Adds `count` to the count of `elem`. Extending classes must implement this function.
    *
    * @param elem   The element.
    * @param count  The count to add.
    * @param hash   The hash function simulator to use.
    * @tparam T     The type of the element to add.
    * @return       This [[Sketch]] as an instance of `A`.
    */
  def add[T](elem: T, count: Long)(implicit hash: HashFunctionSimulator[T]): A

  /**
    * Adds 1 to the count of `elem`.
    *
    * @see `add(elem: T, count: Long)`
    */
  def add[T](elem: T)(implicit hash: HashFunctionSimulator[T]): A = add(elem, 1L)

  /**
    * Estimates the count of `elem`. Extending classes must implement this function.
    *
    * @param elem The element.
    * @param hash The hash function simulator to use.
    * @tparam T   The type of the element to estimate.
    * @return     An approximate count for `elem`.
    */
  def estimate[T](elem: T)(implicit hash: HashFunctionSimulator[T]): Long

  /**
    * Merges this sketch with `other`. Extending classes must implement this function.
    *
    * @param other The sketch to merge with this sketch.
    * @return      The sketch resulting from the merge.
    */
  def merge(other: A): A
}
