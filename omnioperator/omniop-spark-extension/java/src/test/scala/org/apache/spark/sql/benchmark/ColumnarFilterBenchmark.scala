package org.apache.spark.sql.benchmark

object ColumnarFilterBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 20
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("filter with API") {
      spark.range(N).filter("id > 100").explain()
      columnarBenchmark(s"spark.range(${N}).filter(id > 100)", N) {
        spark.range(N).filter("id > 100").noop()
      }
    }
  }
}
