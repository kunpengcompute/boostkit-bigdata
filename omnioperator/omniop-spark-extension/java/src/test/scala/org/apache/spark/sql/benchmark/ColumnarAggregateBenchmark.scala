package org.apache.spark.sql.benchmark


object ColumnarAggregateBenchmark extends ColumnarBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 20
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("stat functions") {
      spark.range(N).groupBy().agg("id" -> "sum").explain()
      columnarBenchmark(s"spark.range(${N}).groupBy().agg(id -> sum)", N) {
        spark.range(N).groupBy().agg("id" -> "sum").noop()
      }
    }
  }
}
