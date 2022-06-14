package org.apache.spark.sql.benchmark

object ColumnarProjectBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 18
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("project with API") {
      spark.range(N).selectExpr("id as p").explain()
      columnarBenchmark(s"spark.range(${N}).selectExpr(id as p)", N) {
        spark.range(N).selectExpr("id as p").noop()
      }
    }
  }
}
