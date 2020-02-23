package sketch

import net.openhft.hashing.LongHashFunction

abstract class Sketch[A] extends Serializable {
  def add[T](elem: T, count: Long)(): A
  def estimate[T](elem: T): Long
  def merge(other: A): A
}

trait HashFunction[T] {
  def apply[A](instance: Sketch[A], elem: T)
}

object HashFunctions {
  implicit object SLongHashFunction extends HashFunction[Long] {
    private val h = LongHashFunction.xx(0)
    private val A1 = h.hashLong(1)
    private val A2 = h.hashLong(2)
    private val B1 = h.hashLong(3)
    private val B2 = h.hashLong(4)

    override def apply[A](instance: Sketch[A], elem: Long): Unit = {

    }
  }

}
