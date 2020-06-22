package scaetch.util

import net.openhft.hashing.LongHashFunction
import org.apache.commons.math3.distribution.ZipfDistribution

object Data {
  def generateZipfianLongs(n: Int, exponent: Double, seed: Int) = {
    val hash = LongHashFunction.xx(seed)
    val dist = new ZipfDistribution(n, exponent)
    dist.reseedRandomGenerator(seed)
//    dist.sample(n).map(x => hash.hashLong(x.toLong)).toList
    dist.sample(n).map(x => x.toLong).toList
  }
}
