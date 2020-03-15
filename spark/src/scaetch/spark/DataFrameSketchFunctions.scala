package scaetch.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}
import scaetch.sketch.hash.{LongHashFunctionSimulator, StringHashFunctionSimulator}
import scaetch.sketch.{BufferedSketch, CountMinSketch, CountSketch, Sketch}

import scala.reflect.ClassTag

class DataFrameSketchFunctions(df: DataFrame) {
  def countSketch(column: Column,
                  depth: Int,
                  width: Int,
                  seed: Int): CountSketch = {
    val sketch = CountSketch(depth, width)
    doSketching(df, column, sketch, seed)
  }

  def bufferedCountSketch(column: Column,
                          depth: Int,
                          width: Int,
                          seed: Int,
                          bufferSize: Int): BufferedSketch[CountSketch] = {
    val sketch = new BufferedSketch(CountSketch(depth, width), bufferSize)
    doSketching(df, column, sketch, seed)
  }

  def countMinSketch(column: Column,
                     depth: Int,
                     width: Int,
                     seed: Int,
                     withConservativeUpdates: Boolean = false): CountMinSketch = {
    val sketch = if (withConservativeUpdates) {
      CountMinSketch(depth, width).withConservativeUpdates
    } else {
      CountMinSketch(depth, width)
    }
    doSketching(df, column, sketch, seed)
  }

  def bufferedCountMinSketch(column: Column,
                             depth: Int,
                             width: Int,
                             seed: Int,
                             bufferSize: Int,
                             withConservativeUpdates: Boolean = false): BufferedSketch[CountMinSketch] = {
    val sketch = if (withConservativeUpdates) {
      new BufferedSketch(CountMinSketch(depth, width).withConservativeUpdates, bufferSize)
    } else {
      new BufferedSketch(CountMinSketch(depth, width), bufferSize)
    }
    doSketching(df, column, sketch, seed)
  }

  private def doSketching[A <: Sketch[A]](df: DataFrame, column: Column, sketch: A, seed: Int)
                                         (implicit tag: ClassTag[A]): A = {
    val singleColumnDf = df.select(column)
    val dataType = singleColumnDf.schema.fields.head.dataType

    dataType match {
      case _: LongType =>
        val hash = new LongHashFunctionSimulator(seed)
        singleColumnDf.queryExecution.toRdd.aggregate(sketch)(
          (sketch: A, row: InternalRow) => sketch.add(row.getLong(0))(hash),
          (sketch1: A, sketch2: A) => sketch1.merge(sketch2)
        )
      case _ =>
        val hash = new StringHashFunctionSimulator(seed)
        singleColumnDf.queryExecution.toRdd.aggregate(sketch)(
          (sketch: A, row: InternalRow) => sketch.add(row.getString(0))(hash),
          (sketch1: A, sketch2: A) => sketch1.merge(sketch2)
        )
    }
  }
}

/**
  * Import this to extend a Spark [[DataFrame]] with functions for using the sketches in
  * Spark.
  *
  * Example:
  * {{{
  *   import scaetch.spark.DataFrameSketchFunctions._
  *   // Read data into DataFrame `df`
  *   val cms = df.sketch.countMinSketch(col("colName"), 5, 1024, 42, false)
  *   cms.estimate("foo")
  * }}}
  */
object DataFrameSketchFunctions {
  implicit class DataFrameExtender(val df: DataFrame) {
    def sketch = new DataFrameSketchFunctions(df)
  }
}