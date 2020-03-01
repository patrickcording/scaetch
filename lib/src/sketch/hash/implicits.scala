package sketch.hash

object implicits {
  implicit val longHashFunction = new LongHashFunctionSimulator(42)
  implicit val stringHashFunction = new StringHashFunctionSimulator(42)
}