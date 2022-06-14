package org.apache.spark.sql.benchmark

object ColumnarUnionBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = if (mainArgs.isEmpty) {
      5L << 15
    } else {
      mainArgs(0).toLong
    }

    val M = if (mainArgs.isEmpty || mainArgs.length < 2) {
      10L << 15
    } else {
      mainArgs(1).toLong
    }

    runBenchmark("union with API") {
      val rangeM = spark.range(M)
      spark.range(N).union(rangeM).explain()
      columnarBenchmark(s"spark.range(${N}).union(spark.range(${M}))", N) {
        spark.range(N).union(rangeM).noop()
      }
    }
  }
}
