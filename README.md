# Introduction
**scaetch** is a Scala implementation of the Count and CountMin sketches.



# Running the benchmark
You need [mill](https://github.com/lihaoyi/mill) to build the benchmark suite.

First, we need to build an agent the JVM. This is used to gather memory usage of the sketches in the benchmark.

```bash
mill agent.compile
jar cmf ./agent/META-INF/MANIFEST.MF Agent.jar ./out/agent/compile/dest/classes/agent/Agent.class
```

Then run:

```bash
mill bench.run 5,10 128,1024 1000 string path/to/file
```

This will launch the benchmark for several combinations of depth and width of the sketches. For the buffered sketches the buffer size is 1000.

The data file is expected to contain one element per line. In this example each line is interpreted as a string. You may change this to `long` if each element in the data file can be parsed to an 64 bit integer.

## Output
The benchmark has three tests:
1. A throughput comparison
2. A precision comparison
3. A memory usage comparison

### Throughput
The throughput is measured in add operations per second.

Example output (depth of the sketches is 3 and the width is in the range from 128 to 1024):
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

### Precision
The precision is measured as the root mean square error of the estimates compared to the true value.

Example output:
```bash
Depth = 3
                             128   256   512  1024
    BufferedCountMinSketch 155.3  64.4  28.2  12.3
       BufferedCountSketch 119.8  68.6  31.0  17.4
            CountMinSketch 155.3  64.4  28.2  12.3
    CountMinSketch with CU 100.5  38.9  16.9   7.6
               CountSketch 119.8  68.6  31.0  17.4
SparkCountMinSketchWrapper 146.6  62.6  27.3  11.9
```

### Memory
The memory usage is in bytes.

Example output:
```bash
Depth = 3
                               128     256     512    1024
    BufferedCountMinSketch  3328.0  6400.0 12544.0 24832.0
       BufferedCountSketch  3328.0  6400.0 12544.0 24832.0
            CountMinSketch  3184.0  6256.0 12400.0 24688.0
    CountMinSketch with CU  3184.0  6256.0 12400.0 24688.0
               CountSketch  3184.0  6256.0 12400.0 24688.0
SparkCountMinSketchWrapper  3280.0  6352.0 12496.0 24784.0
```

# Using the sketches
## Quick
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

## Seeded sketch
```scala
import scaetch.sketch._
import scaetch.sketch.hash.StringHashFunctionSimulator

val seed = 123
implicit val hfs = new StringHashFunctionSimulator(seed)
val cs = CountSketch(5, 512)

// This works because `hfs` is passed implicitly
cs.add("foo")

// This won't compile
cs.add(123L)
```

## Spark
```scala
import scaetch.spark.DataFrameSketchFunctions._

// Read data into DataFrame `df`
val cms = df.sketch.countMinSketch(col("colName"), 5, 1024, 42, false)
cms.estimate("foo")
```

## Buffering
```scala
import scaetch.sketch._
import scaetch.sketch.hash._

val cs = CountSketch(5, 512)
val bufferSize = 1000
val bufferedSketch = new BufferedSketch(cs, bufferSize)
```

## CountMin with conservative updates
```scala
val cms = CountMinSketch(5, 512).withConservativeUpdates
```

# References
[1] Count

[2] CountMin

[3] Less more

[4] Conservative updates

# Todo
[ ] Implement heavy hitter algorithm

