package spark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import sketch.CountSketch

class SparkSketches(df: DataFrame) {
  def count(column: Column, t: Int, b: Int, seed: Int): CountSketch = {
    val singleColumnDf = df.select(column)
    val dataType = singleColumnDf.schema.head.dataType
    val countSketch = new CountSketch(t, b, seed)

    singleColumnDf.queryExecution.toRdd(countSketch)(
      (sketch: CountSketch, row: InternalRow) => sketch.add(row.get(0, dataType).toString),
      (sketch1: CountSketch, sketch2: CountSketch) => sketch1.merge(sketch2)
    )
  }
}

object SparkSketches {
  implicit class DataFrameStatFunctionsExtender(val df: DataFrame) {
    def sketch = new SparkSketches(df)
  }
}
