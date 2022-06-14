package org.apache.spark.sql.benchmark

object ColumnarTopNBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val N = if (mainArgs.isEmpty) {
      500L << 20
    } else {
      mainArgs(0).toLong
    }

    runBenchmark("topN with API") {
      val value = spark.range(N)
      value.sort(value("id").desc).limit(20).explain
      columnarBenchmark(s"spark.range(${N}).sort(id.desc).limit(20)", N) {
        val value = spark.range(N)
        value.sort(value("id").desc).limit(20).noop()
      }
    }
  }
}
