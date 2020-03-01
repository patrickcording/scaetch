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

  val strLength = 100
  def randomString = {
    List.fill(strLength)(r.nextPrintableChar).mkString("")
  }

  val sorted = (1 to 1000).flatMap(_ => {
    val str = randomString
    List.fill(100)(str)
  }).sorted
  File.writeToResources("string_sorted", sorted)
}
