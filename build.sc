import coursier.maven.MavenRepository
import mill._
import mill.modules.Assembly
import mill.scalalib._


trait SketchLibModule extends ScalaModule {
  def scalaVersion = "2.12.10"
}

object agent extends ScalaModule {
  def scalaVersion = "2.12.10"
}

object lib extends SketchLibModule {
  override def ivyDeps = Agg(
    ivy"net.openhft:zero-allocation-hashing:0.10.1"
  )
}

object spark extends SketchLibModule {
  override def moduleDeps = Seq(lib)

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.4.1"
  )

  override def assemblyRules = Assembly.defaultRules ++
    Seq("scala/.*", "org\\.apache\\.spark/.*")
      .map(Assembly.Rule.ExcludePattern.apply)
}

object bench extends SketchLibModule {
  override def moduleDeps = Seq(lib, agent)

  override def repositories = super.repositories ++ Seq(
    MavenRepository("https://oss.sonatype.org/content/repositories/snapshots")
  )

  override def forkArgs = Seq("-javaagent:./Agent.jar")

  override def ivyDeps = Agg(
    ivy"com.storm-enroute::scalameter-core:0.19-SNAPSHOT"
  )
}
