package spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Column, DataFrame}
import sketch.{BufferedSketch, CountMinSketch, CountSketch, Sketch}

import scala.reflect.ClassTag

class implicits(df: DataFrame) {
  def longCount(column: Column, depth: Int, width: Int, seed: Int): CountSketch[Long] = {
    val sketch = CountSketch[Long](depth, width, seed)
    doSketchingOnLongs(column, sketch)
  }

  def bufferedLongCount(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountSketch, Long] = {
    val sketch = CountSketch[Long](depth, width, seed)
    val bufferedSketch = new BufferedSketch(sketch, bufferSize)
    doSketchingOnLongs(column, bufferedSketch)
  }

  def stringCount(column: Column, depth: Int, width: Int, seed: Int): CountSketch[String] = {
    val sketch = CountSketch[String](depth, width, seed)
    doSketchingOnStrings(column, sketch)
  }

  def bufferedStringCount(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountSketch, String] = {
    val sketch = CountSketch[String](depth, width, seed)
    val bufferedSketch = new BufferedSketch(sketch, bufferSize)
    doSketchingOnStrings(column, bufferedSketch)
  }

  def longCountMin(column: Column, depth: Int, width: Int, seed: Int): CountMinSketch[Long] = {
    val sketch = CountMinSketch[Long](depth, width, seed)
    doSketchingOnLongs(column, sketch)
  }

  def bufferedLongCountMin(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountMinSketch, Long] = {
    val sketch = CountMinSketch[Long](depth, width, seed)
    val bufferedSketch = new BufferedSketch(sketch, bufferSize)
    doSketchingOnLongs(column, bufferedSketch)
  }

  def stringCountMin(column: Column, depth: Int, width: Int, seed: Int): CountMinSketch[String] = {
    val sketch = CountMinSketch[String](depth, width, seed)
    doSketchingOnStrings(column, sketch)
  }

  def bufferedStringCountMin(column: Column, depth: Int, width: Int, seed: Int, bufferSize: Int): BufferedSketch[CountMinSketch, String] = {
    val sketch = CountMinSketch[String](depth, width, seed)
    val bufferedSketch = new BufferedSketch(sketch, bufferSize)
    doSketchingOnStrings(column, bufferedSketch)
  }

  private def doSketchingOnLongs[A <: Sketch[A, Long]](column: Column, sketch: A)
                                                      (implicit tag: ClassTag[A]): A = {
    val singleColumnDf = df.select(column)
    singleColumnDf.queryExecution.toRdd.aggregate(sketch)(
      (sketch: A, row: InternalRow) => sketch.add(row.getLong(0)),
      (sketch1: A, sketch2: A) => sketch1.merge(sketch2)
    )
  }

  private def doSketchingOnStrings[A <: Sketch[A, String]](column: Column, sketch: A)
                                                          (implicit tag: ClassTag[A]): A = {
    val singleColumnDf = df.select(column)
    singleColumnDf.queryExecution.toRdd.aggregate(sketch)(
      (sketch: A, row: InternalRow) => sketch.add(row.getString(0)),
      (sketch1: A, sketch2: A) => sketch1.merge(sketch2)
    )
  }
}

object implicits {
  implicit class DataFrameExtender(val df: DataFrame) {
    def sketch = new implicits(df)
  }
}
