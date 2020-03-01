package ds

class FixedSizeHashMap[V](capacity: Int) {
  private val table: Array[Option[(Any, V)]] = Array.fill(capacity)(None)
  private var size = 0

  def clear(): Unit = {
    var i = 0
    while (i < capacity) {
      table(i) = None
      i += 1
    }
    size = 0
  }

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

  def getTable: Array[Option[(Any, V)]] = table
  def getSize: Int = size
}
