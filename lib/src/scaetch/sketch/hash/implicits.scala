package scaetch.sketch.hash

/**
  * Implicit instances of hash function simulators for handling `Long`s, `String`s, and
  * `Any`s. Import and use these if the seed is not important.
  */
object implicits {
  implicit val longHashFunction = new LongHashFunctionSimulator(42)
  implicit val stringHashFunction = new StringHashFunctionSimulator(42)
  implicit val anyHashFunction = new AnyHashFunctionSimulator(42)
}
