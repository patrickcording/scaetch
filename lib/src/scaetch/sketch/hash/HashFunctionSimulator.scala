package scaetch.sketch.hash

import net.openhft.hashing.LongHashFunction

/**
  * The sketches in scaetch require a [[HashFunctionSimulator]] when an element
  * is either added or an estimation is computed.
  *
  * A hash function simulator can simulate having an arbitrary number of hash
  * functions by computing linear combinations from the hash value of one good
  * hash function. This gives faster evaluation of multiple hash function without
  * degrading the quality of the values. The idea is due to Kirsch and Mitzenmacher
  * (see https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf).
  *
  * Hashing is the bottleneck of the performance of the sketches. Therefore it is
  * decoupled from the actual sketches to allow for swapping implementations.
  * For the same reason, hash function simulators are typed to allow for specific,
  * fast implementations for specific types.
  *
  * It is technically possible to use different simulators for the same type in the
  * same sketch. This will break the sketch.
  *
  * We provide hash function simulators for three types: `Long`, `String`, and `Any`.
  * If you need for other types you can either provide your own implementation or
  * rely on the string representation of your data points.
  *
  * @tparam T The type of the elements to hash by this hash function simulator.
  */
trait HashFunctionSimulator[T] {
  protected var a: Long = 0
  protected var b: Long = 0

  /**
    * Sets the value that we want to simulate hashing of.
    *
    * @param x The value to hash.
    */
  def set(x: T): Unit

  /**
    * Computes the hash value of the `i`-th simulated hash function.
    *
    * @param i  The hash function to simulate.
    * @return   The hash value of the `i`-th simulated hash function.
    */
  def hash(i: Int): Int = ((a*(i.toLong+1) + b) >>> 32).toInt

  /**
    * @return The state of the simulator. The state is a tupled of longs.
    */
  def getState: (Long, Long) = (a, b)
}

/**
  * A hash function simulator for `Long` values.
  *
  * @param seed The seed.
  */
class LongHashFunctionSimulator(seed: Long) extends HashFunctionSimulator[Long] {
  private val h = LongHashFunction.xx(seed)
  private val A1 = h.hashLong(1) | 1L
  private val A2 = h.hashLong(2) | 1L

  override def set(x: Long): Unit = {
    a = A1*x
    b = A2*x
  }
}

/**
  * A hash function simulator for `String` values.
  *
  * @param seed The seed.
  */
class StringHashFunctionSimulator(seed: Long) extends HashFunctionSimulator[String] {
  private val h = LongHashFunction.xx(seed)
  private val A1 = h.hashLong(1) | 1L
  private val A2 = h.hashLong(2) | 1L

  override def set(x: String): Unit = {
    val v = h.hashChars(x)
    a = A1*v
    b = A2*v
  }
}

/**
  * A "catch all" hash function simulator. Checks the type of the element to hash,
  * and if it is a `Long` we use a [[LongHashFunctionSimulator]]. For any other type
  * we invoke `toString` on the element and use a [[StringHashFunctionSimulator]].
  *
  * Use this if you don't need static type checking or if you are using the sketches
  * for different types.
  *
  * @param seed The seed.
  */
class AnyHashFunctionSimulator(seed: Long) extends HashFunctionSimulator[Any] {
  private val stringHashFunctionSimulator = new StringHashFunctionSimulator(seed)
  private val longHashFunctionSimulator = new LongHashFunctionSimulator(seed)

  private def setState(state: (Long, Long)) = {
    a = state._1
    b = state._2
  }

  override def set(x: Any): Unit = {
    x match {
      case xl: Long =>
        longHashFunctionSimulator.set(xl)
        setState(longHashFunctionSimulator.getState)
      case xa =>
        stringHashFunctionSimulator.set(xa.toString)
        setState(stringHashFunctionSimulator.getState)
    }
  }
}
