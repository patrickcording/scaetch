import mill._
import mill.modules.Assembly
import scalalib._
import coursier.maven.MavenRepository


trait SparkSketchModule extends ScalaModule {
  def scalaVersion = "2.11.12"

  override def ivyDeps = Agg(
    ivy"net.openhft:zero-allocation-hashing:0.10.1"
  )
}

object lib extends SparkSketchModule

object benchmark extends SparkSketchModule {
  override def moduleDeps = Seq(lib)

  override def repositories = super.repositories ++ Seq(
    MavenRepository("https://oss.sonatype.org/content/repositories/snapshots")
  )

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.4.1",
    ivy"com.storm-enroute::scalameter-core:0.19-SNAPSHOT"
  )
}
