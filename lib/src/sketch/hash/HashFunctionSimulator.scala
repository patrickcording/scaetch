package sketch.hash

import net.openhft.hashing.LongHashFunction

trait HashFunctionSimulator[T] {
  protected var a = 0
  protected var b = 0

  def set(x: T): Unit
  def hash(i: Int): Int = {
    a*i + b
  }
}

class LongHashFunctionSimulator(seed: Long) extends HashFunctionSimulator[Long] {
  private val h = LongHashFunction.xx(seed)
  private val A1 = h.hashLong(1)
  private val A2 = h.hashLong(2)
  private val B1 = h.hashLong(3)
  private val B2 = h.hashLong(4)

  override def set(x: Long): Unit = {
    a = (A1*x + B1).toInt
    b = (A2*x + B2).toInt
  }
}

class StringHashFunctionSimulator(seed: Long) extends HashFunctionSimulator[String] {
  private val h = LongHashFunction.xx(seed)

  override def set(x: String): Unit = {
    val v = h.hashChars(x)
    a = (v >>> 32).toInt
    b = (v & 0xFFFFFFFFL).toInt
  }
}