package scaetch.util

object Misc {
  def largestCommonTopK(a: Map[Any, Int], b: Map[Any, Int]): Int = {
    val aSorted = a.toList.sortBy(x => (-x._2, x.toString)).map(_._1)
    val bSorted = b.toList.sortBy(x => (-x._2, x.toString)).map(_._1)
    val maxK = math.min(aSorted.length, bSorted.length)

    for (k <- 1 to maxK) {
      val intersection = aSorted.take(k).toSet intersect bSorted.take(k).toSet
      if (intersection.size.toDouble <= k.toDouble * 0.90) {
        return k-1
      }
    }
    maxK
  }
}
