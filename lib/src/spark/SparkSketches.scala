package spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Column, DataFrame}
import sketch.{BufferedSketch, CountMinSketch, CountSketch, Sketch}

import scala.reflect.ClassTag

class SparkSketches(df: DataFrame) {
  def count(column: Column, depth: Int, width: Int, seed: Int): CountSketch = {
    val sketch = new CountSketch(depth, width, seed)
    doSketching(column, sketch)
  }

  def bufferedCount(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountSketch] = {
    val sketch = new BufferedSketch(new CountSketch(depth, width, seed), bufferSize)
    doSketching(column, sketch)
  }

  def countMin(column: Column, depth: Int, width: Int, seed: Int): CountMinSketch = {
    val sketch = new CountMinSketch(depth, width, seed)
    doSketching(column, sketch)
  }

  def bufferedCountMin(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountMinSketch] = {
    val sketch = new BufferedSketch(new CountMinSketch(depth, width, seed), bufferSize)
    doSketching(column, sketch)
  }

  private def doSketching[T <: Sketch[T]](column: Column, sketch: T)
                                         (implicit tag: ClassTag[T]): T = {
    val singleColumnDf = df.select(column)
    val dataType = singleColumnDf.schema.head.dataType

    singleColumnDf.queryExecution.toRdd.aggregate(sketch)(
      (sketch: T, row: InternalRow) => sketch.add(row.get(0, dataType).toString),
      (sketch1: T, sketch2: T) => sketch1.merge(sketch2)
    )
  }
}

object SparkSketches {
  implicit class DataFrameExtender(val df: DataFrame) {
    def sketch = new SparkSketches(df)
  }
}
