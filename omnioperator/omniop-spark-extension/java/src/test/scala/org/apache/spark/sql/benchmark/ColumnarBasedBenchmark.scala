package org.apache.spark.sql.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * Common basic scenario to run benchmark
 */
abstract class ColumnarBasedBenchmark extends SqlBasedBenchmark {
  /** Runs function `f` with 3 scenario(spark WSCG on, off and omni-columnar processing) */
  final def columnarBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    if (getSparkSession.conf.getOption("spark.sql.extensions").isDefined)
    {
      benchmark.addCase(s"$name omniruntime wholestage off", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f
        }
      }
    }
    else
    {
      benchmark.addCase(s"$name Spark wholestage off", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f
        }
      }
      benchmark.addCase(s"$name Spark wholestage on", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
          f
        }
      }
    }

    benchmark.run()
  }
}
