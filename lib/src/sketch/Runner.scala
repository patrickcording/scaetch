package sketch

object Runner extends App {
  import NewCountSketch._

  val cs = new CountSketch(5, 1024, 37)
  cs.add("asd", 1L)
  cs.add("asd", 1L)
  cs.add(213L, 1L)
  cs.add(123.0d, 1L)


  println(cs.estimate(213L))

}

object NewCountSketch {
  implicit object DoubleCountSketchStateUpdateFunction
    extends SketchUpdateStateFunction[CountSketch, Double]{

    override def apply(elem: Double, instance: CountSketch) = {
      instance.counters(0) = 2 * 3 * elem
    }
  }
}
