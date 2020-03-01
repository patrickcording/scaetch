package tools

import org.apache.commons.rng.UniformRandomProvider
import org.apache.commons.rng.core.source64.XoRoShiRo128PlusPlus
import org.apache.commons.rng.sampling.distribution.RejectionInversionZipfSampler
import util.File

import scala.util.Random

object DataGenerator extends App {

  val n = 100000
  val range = 1000
  val r = new Random(37)

//  val randomGaussian = (1 to n).map(_ => r.nextGaussian())
//  val max = randomGaussian.map(Math.abs(_)).max
//  val normalizedRandomGaussian = randomGaussian.map(_ / max)
//  val gaussianLongs = normalizedRandomGaussian.map(v => (v * range).toLong)
//  File.writeToResources("long_gauss", gaussianLongs)

//  val rng = new XoRoShiRo128PlusPlus(Array(37, 73))
//  val zipf = RejectionInversionZipfSampler.of(rng, n, 1.1)
//  val randomZipf = (1 to n).map(_ => zipf.sample())
//  File.writeToResources("long_zipf", randomZipf)

  val sorted = (1 to n).map(_ => r.nextInt(range)).sorted.map(e => Array.fill(e)("a").mkString)
  File.writeToResources("long_sorted", sorted)
}
