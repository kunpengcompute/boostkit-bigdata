package org.apache.spark.sql.benchmark

object ColumnarSortBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 20
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("sort with API") {
      val value = spark.range(N)
      value.sort(value("id").desc).explain
      columnarBenchmark(s"spark.range(${N}).sort(id.desc)", N) {
        val value = spark.range(N)
        value.sort(value("id").desc).noop()
      }
    }
  }
}
