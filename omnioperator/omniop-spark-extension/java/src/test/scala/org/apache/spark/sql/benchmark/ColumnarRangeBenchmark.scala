package org.apache.spark.sql.benchmark

object ColumnarRangeBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 20
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("range with API") {
      spark.range(N).explain()
      columnarBenchmark(s"spark.range(${N})", N) {
        spark.range(N).noop()
      }
    }
  }
}
