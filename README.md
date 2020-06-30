# scaetch
**Scætch** is a Scala implementation of the [Count](https://www.cs.rutgers.edu/~farach/pubs/FrequentStream.pdf) 
and [CountMin](https://7797b024-a-62cb3a1a-s-sites.googlegroups.com/site/countminsketch/cm-latin.pdf) sketches for approximate counting in streams. Its API is strongly typed and the algorithms are implemented with performance in mind.

This repository also contains a benchmark that lets you measure throughput and precision of the sketches on a sample of your data. There is no one sketch that is always the best. The benchmark will help you finding the one you need.

## WIP
Scætch is currently **work in progress**. Some breaking bugs exist! Once things are fixed, a release will be published to Maven Central.

## Using the sketches
### Getting started
```scala
import scaetch.sketch._
import scaetch.sketch.hash._

// Create a Count sketch
val cs = CountSketch(5, 512)

// Add any type of data
cs.add(123L)
cs.add("foo")
cs.add(1.234)

// Compute estimate
cs.estimate("foo")
```

### Buffering
If some elements in your data are much more frequent than others, or if elements arrive in groups, buffering elements may speed up the processing.
```scala
val cs = CountSketch(5, 512)
val bufferSize = 1000
val bufferedSketch = new BufferedSketch(cs, bufferSize)
```

### Conservative Updates
In many cases, the precision of the CountMin sketch will improve with conservative updates. The drawback is that updates take longer time.
```scala
val cms = CountMinSketch(5, 512).withConservativeUpdates
```

### Spark API
The Spark module contains an API for using the sketches with Spark. The following will compute a CountMin sketch for each partition of `df` and combine to one resulting sketch.
```scala
import scaetch.spark.DataFrameSketchFunctions._

// Read data into DataFrame `df`
val cms = df.sketch.countMinSketch(col("colName"), 5, 1024, 42, false)
cms.estimate("foo")
```

## Applications of count sketches
Count sketches can be extended to do more than just approximate counting. They may also be used to efficiently 
approximate the top K most frequent elements and find heavy hitters in the stream.

Below are examples of how to use Scætch for this.

### Top K
```scala
import scaetch.sketch.application.TopK

val cms = CountMinSketch(5, 512)
val k = 10
val topK = new TopK[Long](10, cms)
topK.add(123)
topK.get // : List[Long]
```

### Heavy hitters
```scala
import scaetch.sketch.application.HeavyHitter

val cmsFactory = () => CountMinSketch(5, 512)
val phi = 0.05 // An element is "heavy" if it occurs more than 5% of the time
val hh = new HeavyHitter(phi, cmsFactory)
hh.add(123)
hh.get // : List[Long]
```

## Running the benchmark
You need [mill](https://github.com/lihaoyi/mill) to build the benchmark 
module.

First, an [agent for the JVM](https://www.baeldung.com/java-size-of-object) is needed for getting the size of objects. To prepare this, run:

```bash
mill agent.compile
jar cmf ./agent/META-INF/MANIFEST.MF Agent.jar ./out/agent/compile/dest/classes/agent/Agent.class
```

Now you can run the following.

```bash
mill bench.run 5,10 128,1024 1000 string path/to/file
```

This will launch the benchmark for several combinations of depth and width 
of the sketches. For the buffered sketches the buffer size is 1000.

The data file is expected to contain one element per line. In this example 
each line is interpreted as a string. You may change this to `long` if each 
element in the data file can be parsed to a 64 bit integer.

### Output
The benchmark has three tests:
1. A throughput comparison
2. A precision comparison
3. A memory usage comparison

For each test the benchmark will output several tables (depending on the supplied parameters). The following is an example of what the output of the throughput test might look like. The sketches in this test have depth 3 and widths from 128 to 1024.

```
Depth = 3
                               128     256     512    1024
    BufferedCountMinSketch  7658.8 10613.0  5306.5  9338.8
       BufferedCountSketch  7442.8  7593.6  8929.4  9732.7
            CountMinSketch 20351.3 27537.9 32125.8 26930.7
    CountMinSketch with CU 23119.2 24060.1 13885.5 11701.2
               CountSketch 14902.5 23446.1 27941.5 11788.5
SparkCountMinSketchWrapper 22118.6 21575.8 21913.2 17647.6
```
