package org.apache.spark.sql.benchmark

import org.apache.spark.sql.functions._

object ColumnarJoinBenchmark extends ColumnarBasedBenchmark {
  def broadcastHashJoinLongKey(rowsA: Long): Unit = {
    val rowsB = 1 << 16
    val dim = spark.range(rowsB).selectExpr("id as k", "id as v")
    val df = spark.range(rowsA).join(dim.hint("broadcast"), (col("id") % rowsB) === col("k"))
    df.explain()
    columnarBenchmark(s"broadcastHashJoinLongKey spark.range(${rowsA}).join(spark.range(${rowsB}))", rowsA) {
      df.noop()
    }
  }

  def sortMergeJoin(rowsA: Long, rowsB: Long): Unit = {
    val df1 = spark.range(rowsA).selectExpr(s"id * 2 as k1")
    val df2 = spark.range(rowsB).selectExpr(s"id * 3 as k2")
    val df = df1.join(df2.hint("mergejoin"), col("k1") === col("k2"))
    df.explain()
    columnarBenchmark(s"sortMergeJoin spark.range(${rowsA}).join(spark.range(${rowsB}))", rowsA) {
      df.noop()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val rowsA = if (mainArgs.isEmpty) {
      20 << 20
    } else {
      mainArgs(0).toLong
    }

    val rowsB = if (mainArgs.isEmpty || mainArgs.length < 2) {
      1 << 16
    } else {
      mainArgs(1).toLong
    }

    runBenchmark("Join Benchmark") {
      broadcastHashJoinLongKey(rowsA)
      sortMergeJoin(rowsA, rowsB)
    }
  }
}
