/*
 * Copyright (C) 2024-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution.forsql

import com.huawei.boostkit.spark.ColumnarPluginConfig.OMNI_ENABLE_KEY
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.joins.{ColumnarBroadcastHashJoinExec, ColumnarSortMergeJoinExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.Row

import java.sql.Timestamp
import java.util.TimeZone

class ColumnarTimestampDataTypeSqlSuite extends ColumnarSparkPlanTest {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key, "LEGACY")
    .set(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key, "LEGACY")

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    Seq[(Int, Timestamp, Timestamp)](
      (1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02")),
      (2, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01")),
      (3, null, Timestamp.valueOf("1024-10-10 00:00:00"))
    ).toDF("int_c", "timestamp_c1", "timestamp_c2").write.format("orc").saveAsTable("timestamp_test_orc")

    Seq[(Int, Timestamp, Timestamp)](
      (1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02")),
      (2, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01")),
      (3, null, Timestamp.valueOf("1024-10-10 00:00:00"))
    ).toDF("int_c", "timestamp_c1", "timestamp_c2").write.format("parquet").saveAsTable("timestamp_test_parquet")
  }

  protected override def afterAll(): Unit = {
    spark.sql("drop table if exists timestamp_test_orc")
    spark.sql("drop table if exists timestamp_test_parquet")
    super.afterAll()
  }

  test("Test Table reader normal") {
    val orcRes = spark.sql("select * from timestamp_test_orc")
    var executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02")),
        Row(2, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01")),
        Row(3, null, Timestamp.valueOf("1024-10-10 00:00:00")))
    )

    val parquetRes = spark.sql("select * from timestamp_test_parquet")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")

    checkAnswer(
      parquetRes,
      Seq(
        Row(1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02")),
        Row(2, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01")),
        Row(3, null, Timestamp.valueOf("1024-10-10 00:00:00")))
    )
  }

  test("Test Time Zone Change") {
    val defaultTimeZone = TimeZone.getDefault.getID
    // 修改
    spark.sql("set spark.sql.session.timeZone=UTC").collect()

    spark.sql("set spark.omni.enabled=false").collect()
    val orcExpectRes = spark.sql("select * from timestamp_test_orc")
    var executedPlan = orcExpectRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[FileSourceScanExec]).isDefined, s"FileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    val orcExpect = orcExpectRes.collect().toSeq

    val parquetExpectRes = spark.sql("select * from timestamp_test_parquet")
    executedPlan = parquetExpectRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[FileSourceScanExec]).isDefined, s"FileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    val parquetExpect = parquetExpectRes.collect().toSeq

    spark.sql("set spark.omni.enabled=true").collect()
    val orcRes = spark.sql("select * from timestamp_test_orc")
    executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")

    val parquetRes = spark.sql("select * from timestamp_test_parquet")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")

    checkAnswer(orcRes, orcExpect)
    checkAnswer(parquetRes, parquetExpect)

    // 还原
    spark.sql(s"set spark.sql.session.timeZone=$defaultTimeZone").collect()
  }

  test("Test orc support expr") {
    val orcRes = spark.sql("select timestamp_c1 < timestamp_c2, " +
      "timestamp_c1 <= timestamp_c2, " +
      "timestamp_c1 > timestamp_c2, " +
      "timestamp_c1 >= timestamp_c2, " +
      "timestamp_c1 == timestamp_c2, " +
      "timestamp_c1 = timestamp_c2, " +
      "timestamp_c1 != timestamp_c2, " +
      "coalesce(timestamp_c1, timestamp_c2), " +
      "if(timestamp_c1 > timestamp_c2, timestamp_c1, timestamp_c2), " +
      "timestamp_c1 in (timestamp_c2), " +
      "isnull(timestamp_c1), " +
      "isnotnull(timestamp_c1) from timestamp_test_orc")
    val executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(true, true, false, false, false, false, true, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02"), false, false, true),
        Row(true, true, false, false, false, false, true, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01"), false, false, true),
        Row(null, null, null, null, null, null, null, Timestamp.valueOf("1024-10-10 00:00:00"), Timestamp.valueOf("1024-10-10 00:00:00"), null, true, false))
    )
  }

  test("Test orc support operator") {
    var orcRes = spark.sql("select timestamp_c1 from timestamp_test_orc where timestamp_c1 != timestamp_c2")
    var executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined, s"ColumnarConditionProjectExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")),
        Row(Timestamp.valueOf("1024-02-29 00:00:01")))
    )

    orcRes = spark.sql("select int_c, timestamp_c1, max(timestamp_c2) from timestamp_test_orc group by int_c, timestamp_c1 order by int_c limit 1")
    executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isDefined, s"ColumnarShuffleExchangeExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02"))
      )
    )

    orcRes = spark.sql("select timestamp_c1 from timestamp_test_orc as t1, (select c from values(make_timestamp('2024', '10', '10', '00', '00', '01')) as tab(c)) as t2 where t1.timestamp_c1 = t2.c")
    executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined, s"ColumnarBroadcastHashJoinExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarFilterExec]).isDefined, s"ColumnarFilterExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastExchangeExec]).isDefined, s"ColumnarBroadcastExchangeExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")))
    )

    orcRes = spark.sql("select /*+ MERGEJOIN (t2) */ t2.* from timestamp_test_orc as t1, (select c from values(make_timestamp('2024', '10', '10', '00', '00', '01')) as tab(c)) as t2 where t1.timestamp_c1 = t2.c")
    executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isDefined, s"ColumnarSortMergeJoinExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")))
    )

    orcRes = spark.sql("select timestamp_c2, count(timestamp_c1) over(PARTITION BY timestamp_c2 order by timestamp_c1) as count from timestamp_test_orc order by timestamp_c2")
    executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined, s"ColumnarSortExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(Timestamp.valueOf("1024-03-01 00:00:01"), 1),
        Row(Timestamp.valueOf("1024-10-10 00:00:00"), 0),
        Row(Timestamp.valueOf("2024-10-10 00:00:02"), 1)
      )
    )
  }

  test("Test parquet support expr") {
    val parquetRes = spark.sql("select timestamp_c1 < timestamp_c2, " +
      "timestamp_c1 <= timestamp_c2, " +
      "timestamp_c1 > timestamp_c2, " +
      "timestamp_c1 >= timestamp_c2, " +
      "timestamp_c1 == timestamp_c2, " +
      "timestamp_c1 = timestamp_c2, " +
      "timestamp_c1 != timestamp_c2, " +
      "coalesce(timestamp_c1, timestamp_c2), " +
      "if(timestamp_c1 >= timestamp_c2, timestamp_c1, timestamp_c2), " +
      "timestamp_c1 in (timestamp_c2), " +
      "isnull(timestamp_c1), " +
      "isnotnull(timestamp_c1) from timestamp_test_parquet")
    val executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(true, true, false, false, false, false, true, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02"), false, false, true),
        Row(true, true, false, false, false, false, true, Timestamp.valueOf("1024-02-29 00:00:01"), Timestamp.valueOf("1024-03-01 00:00:01"), false, false, true),
        Row(null, null, null, null, null, null, null, Timestamp.valueOf("1024-10-10 00:00:00"), Timestamp.valueOf("1024-10-10 00:00:00"), null, true, false))
    )
  }

  test("Test parquet support operator") {
    var parquetRes = spark.sql("select timestamp_c1 from timestamp_test_parquet where timestamp_c1 != timestamp_c2")
    var executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined, s"ColumnarConditionProjectExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")),
        Row(Timestamp.valueOf("1024-02-29 00:00:01")))
    )

    parquetRes = spark.sql("select int_c, timestamp_c1, max(timestamp_c2) from timestamp_test_parquet group by int_c, timestamp_c1 order by int_c limit 1")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isDefined, s"ColumnarShuffleExchangeExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(1, Timestamp.valueOf("2024-10-10 00:00:01"), Timestamp.valueOf("2024-10-10 00:00:02"))
      )
    )

    parquetRes = spark.sql("select timestamp_c1 from timestamp_test_parquet as t1, (select c from values(make_timestamp('2024', '10', '10', '00', '00', '01')) as tab(c)) as t2 where t1.timestamp_c1 = t2.c")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined, s"ColumnarBroadcastHashJoinExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarFilterExec]).isDefined, s"ColumnarFilterExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastExchangeExec]).isDefined, s"ColumnarBroadcastExchangeExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")))
    )

    parquetRes = spark.sql("select /*+ MERGEJOIN (t2) */ t2.* from timestamp_test_parquet as t1, (select c from values(make_timestamp('2024', '10', '10', '00', '00', '01')) as tab(c)) as t2 where t1.timestamp_c1 = t2.c")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isDefined, s"ColumnarSortMergeJoinExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(Timestamp.valueOf("2024-10-10 00:00:01")))
    )

    parquetRes = spark.sql("select timestamp_c2, count(timestamp_c1) over(PARTITION BY timestamp_c2 order by timestamp_c1) as count from timestamp_test_parquet order by timestamp_c2")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined, s"ColumnarSortExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      parquetRes,
      Seq(
        Row(Timestamp.valueOf("1024-03-01 00:00:01"), 1),
        Row(Timestamp.valueOf("1024-10-10 00:00:00"), 0),
        Row(Timestamp.valueOf("2024-10-10 00:00:02"), 1)
      )
    )
  }

  test("Test Table write orc unSupport") {
    val res = spark.sql("insert into table timestamp_test_orc values(4, null, null)")
    res.collect()
    val columnarDataWrite = res.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
      .commandPhysicalPlan.find({
        case _: DataWritingCommandExec => true
        case _ => false
      })
    assert(columnarDataWrite.isDefined, "use columnar data writing command")
  }
}