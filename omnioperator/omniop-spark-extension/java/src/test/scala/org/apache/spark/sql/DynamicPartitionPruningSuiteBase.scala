/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test suite for the filtering ratio policy used to trigger dynamic partition pruning (DPP).
 */
class DynamicPartitionPruningSuite
    extends ColumnarSparkPlanTest
    with SQLTestUtils
    with GivenWhenThen
    with AdaptiveSparkPlanHelper {

  val tableFormat: String = "parquet"

  import testImplicits._

  protected def initState(): Unit = {}
  protected def runAnalyzeColumnCommands: Boolean = true

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    initState()

    val factData = Seq[(Int, Int, Int, Int)](
      (1000, 1, 1, 10),
      (1010, 2, 1, 10),
      (1020, 2, 1, 10),
      (1030, 3, 2, 10),
      (1040, 3, 2, 50),
      (1050, 3, 2, 50),
      (1060, 3, 2, 50),
      (1070, 4, 2, 10),
      (1080, 4, 3, 20),
      (1090, 4, 3, 10),
      (1100, 4, 3, 10),
      (1110, 5, 3, 10),
      (1120, 6, 4, 10),
      (1130, 7, 4, 50),
      (1140, 8, 4, 50),
      (1150, 9, 1, 20),
      (1160, 10, 1, 20),
      (1170, 11, 1, 30),
      (1180, 12, 2, 20),
      (1190, 13, 2, 20),
      (1200, 14, 3, 40),
      (1200, 15, 3, 70),
      (1210, 16, 4, 10),
      (1220, 17, 4, 20),
      (1230, 18, 4, 20),
      (1240, 19, 5, 40),
      (1250, 20, 5, 40),
      (1260, 21, 5, 40),
      (1270, 22, 5, 50),
      (1280, 23, 1, 50),
      (1290, 24, 1, 50),
      (1300, 25, 1, 50)
    )

    val storeData = Seq[(Int, String, String)](
      (1, "North-Holland", "NL"),
      (2, "South-Holland", "NL"),
      (3, "Bavaria", "DE"),
      (4, "California", "US"),
      (5, "Texas", "US"),
      (6, "Texas", "US")
    )

    val storeCode = Seq[(Int, Int)](
      (1, 10),
      (2, 20),
      (3, 30),
      (4, 40),
      (5, 50),
      (6, 60)
    )

    if (tableFormat == "hive") {
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    }

    spark.range(1000)
      .select($"id" as "product_id", ($"id" % 10) as "store_id", ($"id" + 1) as "code")
      .write
      .format(tableFormat)
      .mode("overwrite")
      .saveAsTable("product")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .format(tableFormat)
      .saveAsTable("fact_np")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("fact_sk")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("fact_stats")

    storeData.toDF("store_id", "state_province", "country")
      .write
      .format(tableFormat)
      .saveAsTable("dim_store")

    storeData.toDF("store_id", "state_province", "country")
      .write
      .format(tableFormat)
      .saveAsTable("dim_stats")

    storeCode.toDF("store_id", "code")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("code_stats")

    if (runAnalyzeColumnCommands) {
      sql("ANALYZE TABLE fact_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE dim_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE dim_store COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE code_stats COMPUTE STATISTICS FOR COLUMNS store_id")
    }
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS fact_np")
      sql("DROP TABLE IF EXISTS fact_sk")
      sql("DROP TABLE IF EXISTS product")
      sql("DROP TABLE IF EXISTS dim_store")
      sql("DROP TABLE IF EXISTS fact_stats")
      sql("DROP TABLE IF EXISTS dim_stats")
      sql("DROP TABLE IF EXISTS code_stats")
    } finally {
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
      super.afterAll()
    }
  }

  /**
   * Check if the query plan has a partition pruning filter inserted as
   * a subquery duplicate or as a custom broadcast exchange.
   */
  def checkPartitionPruningPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) => b
    }

    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(hasSubquery == withSubquery,
      s"$hasFilter trigger DPP with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast trigger DPP with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach { s =>
      s.child match {
        case _: ReusedExchangeExec => // reuse check ok.
        case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) => // reuse check ok.
        case b: BroadcastExchangeLike =>
          val hasReuse = plan.exists {
            case ReusedExchangeExec(_, e) => e eq b
            case _ => false
          }
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case a: AdaptiveSparkPlanExec =>
          val broadcastQueryStage = collectFirst(a) {
            case b: BroadcastQueryStageExec => b
          }
          val broadcastPlan = broadcastQueryStage.get.broadcast
          val hasReuse = find(plan) {
            case ReusedExchangeExec(_, e) => e eq broadcastPlan
            case b: BroadcastExchangeLike => b eq broadcastPlan
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case _ =>
          fail(s"Invalid child node found in\n$s")
      }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
      val subquery = s match {
        case r: ReusedSubqueryExec => r.child
        case o => o
      }
      assert(subquery.exists(_.isInstanceOf[AdaptiveSparkPlanExec]) == isMainQueryAdaptive)
    }
  }

  /**
   * Check if the plan has the given number of distinct broadcast exchange subqueries.
   */
  def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    df.collect()

    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) =>
        b.index
    }
    assert(buf.distinct.size == n)
  }

  /**
   * Collect the children of all correctly pushed down dynamic pruning expressions in a spark plan.
   */
  protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: ColumnarFileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case s: BatchScanExec => s.runtimeFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  /**
   * Check if the plan contains unpushed dynamic pruning filters.
   */
  def checkUnpushedFilters(df: DataFrame): Boolean = {
    find(df.queryExecution.executedPlan) {
      case FilterExec(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
      case _ => false
    }.isDefined
  }

  test("broadcast a single key in a HashedRelation") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" + 1).cast("string").as("str"))
          .write.partitionBy("one", "two", "three", "str")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          ($"id" * 10).as("prod"),
          ($"id" + 1).cast("string").as("str"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")

        // broadcast a single Long key
        val dfLong = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfLong, Row(9, 10, 11, "10") :: Nil)

        // reuse a single Byte key
        val dfByte = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.two = d.two)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfByte, Row(9, 10, 11, "10") :: Nil)

        // reuse a single String key
        val dfStr = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.str = d.str)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfStr, Row(9, 10, 11, "10") :: Nil)
      }
    }
  }

  test("broadcast multiple keys in a LongHashedRelation") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" % 10).cast("string").as("str"))
          .write.partitionBy("one", "two", "three")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          ($"id" * 10).as("prod"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")

        // broadcast multiple keys
        val dfLong = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one and f.two = d.two and f.three = d.three)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfLong, Row(9, 10, 11, "9") :: Nil)
      }
    }
  }
}


