import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, TypedColumn}
import sketch.CountSketch

object CountSketchAggregator {
  def aggregate(column: String, k: Int, t: Int, b: Int, seed: Int): TypedColumn[Row, String] =
    new Aggregator[Row, CountSketch, String] with Serializable {
    override def zero: CountSketch = new CountSketch(k, t, b, seed)

    override def reduce(b: CountSketch, a: Row): CountSketch = b.add(a.getAs[String](column))

    override def merge(b1: CountSketch, b2: CountSketch): CountSketch = b1.merge(b2)

    override def finish(reduction: CountSketch): String = reduction.get.mkString(", ")

    override def bufferEncoder: Encoder[CountSketch] = Encoders.kryo[CountSketch]

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }.toColumn
}
