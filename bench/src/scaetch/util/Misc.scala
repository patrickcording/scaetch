package scaetch.util

object Misc {
  def getTopKIntersection(a: Map[Any, Int], b: Map[Any, Int], k: Int): Int = {
    val aSorted = a.toList.sortBy(x => (-x._2, x.toString)).map(_._1)
    val bSorted = b.toList.sortBy(x => (-x._2, x.toString)).map(_._1)
    val intersection = aSorted.take(k).toSet intersect bSorted.take(k).toSet
    intersection.size
  }
}
