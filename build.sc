import mill._
import mill.modules.Assembly
import scalalib._

trait SparkSketchModule extends ScalaModule {
  def scalaVersion = "2.11.12"

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.4.1"
  )

  override def assemblyRules = Assembly.defaultRules ++
    Seq("scala/.*", "org\\.apache\\.spark/.*")
      .map(Assembly.Rule.ExcludePattern.apply)
}

object lib extends SparkSketchModule

object benchmark extends SparkSketchModule {
  override def moduleDeps = Seq(lib)

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.4.1",
    ivy"org.scalaz::scalaz-core:7.2.29"
  )
}
