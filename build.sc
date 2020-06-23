import lib.ivy
import mill._
import mill.scalalib._
import mill.scalalib.publish._


trait SketchLibModule extends ScalaModule {
  def scalaVersion = "2.12.10"
  val sparkVersion = "2.4.6"
}

object agent extends ScalaModule {
  def scalaVersion = "2.12.10"
}

object lib extends SketchLibModule with PublishModule {
  override def ivyDeps = Agg(
    ivy"net.openhft:zero-allocation-hashing:0.10.1"
  )

  override def compileIvyDeps = Agg(
    ivy"org.apache.spark::spark-sql:$sparkVersion"
  )

  override def artifactName = "scaetch"
  def publishVersion = "0.0.1"

  def pomSettings = PomSettings(
    description = "Sketches for approximate counting in streams",
    organization = "com.github.patrickcording",
    url = "https://github.com/patrickcording/scaetch",
    licenses = Seq(License.MIT),
    versionControl = VersionControl.github("patrickcording", "scaetch"),
    developers = Seq(
      Developer("patrickcording", "Patrick Cording", "https://github.com/patrickcording")
    )
  )
}

object bench extends SketchLibModule {
  override def moduleDeps = Seq(lib, agent)
  override def mainClass = Some("scaetch.bench.Benchmark")

  override def forkArgs = Seq("-javaagent:./Agent.jar")

  override def ivyDeps = Agg(
    ivy"com.storm-enroute::scalameter-core:0.19",
    ivy"org.apache.commons:commons-math3:3.6.1",
    ivy"org.apache.spark::spark-sql:$sparkVersion"
  )
}
