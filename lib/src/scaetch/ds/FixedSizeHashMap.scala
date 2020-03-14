package scaetch.ds

/**
  * An implementation of a hash map that is using linear probing and where the underlying table is of a fixed
  * size.
  *
  * This data structure is intended for being used for when you know the total number of possible keys and
  * you want to update the values associated with keys that already exists.
  *
  * @param capacity The size of the underlying table. If you try add more unique keys than this value, the
  *                 update function will enter into an infite loop.
  * @tparam V       The type of values.
  */
class FixedSizeHashMap[V](capacity: Int) {
  private val table: Array[Option[(Any, V)]] = Array.fill(capacity)(None)
  private var size = 0

  /**
    * Clears the underlying table.
    */
  def clear(): Unit = {
    var i = 0
    while (i < capacity) {
      table(i) = None
      i += 1
    }
    size = 0
  }

  /**
    * Inserts the value if `key` does not exist, otherwise update the value using the provided update function.
    *
    * The update function has to handle two cases: 1. if the key exists and 2. if the key does not exist. If the
    * key exists the argument for the update function is `Some(v)` where `v` is of type `V` and is the current
    * value. If the key does not exist in the map the argument for the update function is `None`.
    *
    * @param key            The key to insert.
    * @param updateFunction The update function.
    */
  def updateOrAdd(key: Any, updateFunction: Option[V] => V): Unit = {
    var i = Math.abs(key.hashCode()) % capacity
    while(table(i).isDefined && table(i).get._1 != key) {
      i = (i + 1) % capacity
    }

    table(i) = table(i) match {
      case Some((k, v)) =>
        Some((k, updateFunction(Some(v))))
      case None =>
        size += 1
        Some((key, updateFunction(None)))
    }
  }

  /**
    * @return The underlying table.
    */
  def getTable: Array[Option[(Any, V)]] = table

  /**
    * Gets the current size of the table. `capacity` minus this value is the number of free slots left in the
    * table.
    *
    * @return The size of the underlying table.
    */
  def getSize: Int = size
}
