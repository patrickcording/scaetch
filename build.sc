import mill._, scalalib._

trait SparkSketchModule extends ScalaModule {
  def scalaVersion = "2.12.8"
}

object lib extends SparkSketchModule

object demo extends SparkSketchModule {
  override def moduleDeps = Seq(lib)

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.4.4"
  )
}
