package tools

import org.apache.commons.rng.UniformRandomProvider
import org.apache.commons.rng.core.source64.XoRoShiRo128PlusPlus
import org.apache.commons.rng.sampling.distribution.RejectionInversionZipfSampler
import util.File

import scala.util.Random

object DataGenerator extends App {

  private def intToIp(v: Int) = {
    val c1 = if ((v >>> 24) == 0) 1 else (v >>> 24) & 0xff
    val c2 = (v >>> 16) & 0xff
    val c3 = (v >>> 8) & 0xff
    val c4 = v & 0xff
    s"$c1.$c2.$c3.$c4"
  }

  val n = 100000
  val range = 1000
  val r = new Random(37)

//  val randomGaussian = (1 to n).map(_ => r.nextGaussian())
//  val max = randomGaussian.map(Math.abs(_)).max
//  val normalizedRandomGaussian = randomGaussian.map(_ / max)
//  val gaussianLongs = normalizedRandomGaussian.map(v => (v * range).toLong)
//  File.writeToResources("long_gauss", gaussianLongs)

  val rng = new XoRoShiRo128PlusPlus(Array(37, 73))
  val zipf = RejectionInversionZipfSampler.of(rng, n, 1.1)
  val randomZipf = (1 to n).map(_ => zipf.sample())
  File.writeToResources("long_zipf", randomZipf)
}
