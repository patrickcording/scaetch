package scaetch.util

object Printer {
  /**
    * Print a map of doubles as a table.
    *
    * Modified slightly from https://stackoverflow.com/questions/55094233/scala-table-print.
    */
  def printTable(map: Map[(String, String), Double]) = {
    val (rows, cols) = {
      val (r, c) = map.keys.unzip
      (r.toList.sorted, c.toList.map(_.toLong).sorted.map(_.toString))
    }

    val firstColumnOffset = rows.map(_.length).max
    val valueOffset = map.values.map(" %.1f".format(_).length).max
    val table =
      (List(s"%${firstColumnOffset}s".format("")) ++ cols.map(s"%${valueOffset}s".format(_))).mkString + "\n" +
        rows.map { r =>
          s"%${firstColumnOffset}s".format(r) + cols.map { c =>
            s"%${valueOffset}s".format("%.1f".format(map.getOrElse((r, c), 0.0)))
          }.mkString
        }.mkString("\n")

    println(table)
  }
}
