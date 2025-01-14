/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark

import com.huawei.boostkit.spark.util.InsertTransitions
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ColumnarBroadcastHashJoinExec}
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarProjectExec, ColumnarTakeOrderedAndProjectExec, LeafExecNode, OmniColumnarToRowExec, ProjectExec, RowToOmniColumnarExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import scala.concurrent.Future

class FallbackStrategiesSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .setAppName("test FallbackStrategiesSuite")
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "com.huawei.boostkit.spark.ColumnarPlugin")
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.OmniColumnarShuffleManager")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val employees = Seq[(String, String, Int, Int)](
      ("Lisa", "Sales", 10000, 35),
      ("Evan", "Sales", 32000, 38),
      ("Fred", "Engineering", 21000, 28),
      ("Alex", "Sales", 30000, 33),
      ("Tom", "Engineering", 23000, 33),
      ("Jane", "Marketing", 29000, 28),
      ("Jeff", "Marketing", 35000, 38),
      ("Paul", "Engineering", 29000, 23),
      ("Chloe", "Engineering", 23000, 25)
    ).toDF("name", "dept", "salary", "age")
    employees.createOrReplaceTempView("employees_for_fallback_ut_test")
  }

  test("c2r doExecuteBroadcast") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "3"),
      (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true"),
      (SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "10MB"),
      ("spark.omni.sql.columnar.broadcastJoin", "false")) {
      val df = spark.sql("select t1.age * 2, t2.salary from employees_for_fallback_ut_test t1 join employees_for_fallback_ut_test t2 on t1.age = t2.age sort by t1.age")
      val runRows = df.collect()
      val expectedRows = Seq(Row(56, 21000), Row(56, 21000),
        Row(66, 30000), Row(66, 30000),
        Row(70, 10000), Row(76, 32000),
        Row(76, 32000), Row(46, 29000),
        Row(50, 23000), Row(56, 29000),
        Row(56, 29000), Row(66, 23000),
        Row(66, 23000),Row(76, 35000),Row(76, 35000))
      assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
      val bhj = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.find({
        case _: BroadcastHashJoinExec => true
        case _ => false
      })
      assert(bhj.isDefined, "bhj is fallback")
      val c2rWithOmniBroadCast = bhj.get.children.find({
        case p: OmniColumnarToRowExec =>
          p.child.isInstanceOf[BroadcastQueryStageExec] &&
            p.child.asInstanceOf[BroadcastQueryStageExec].plan.isInstanceOf[ColumnarBroadcastExchangeExec]
        case _ => false
      })
      assert(c2rWithOmniBroadCast.isDefined, "bhj should use omni c2r to adapt to omni broadcast exchange")
    }
  }

  test("r2c doExecuteBroadcast") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "3"),
      (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true"),
      (SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "10MB"),
      ("spark.omni.sql.columnar.broadcastexchange", "false")) {
      val df = spark.sql("select t1.age * 2, t2.salary from employees_for_fallback_ut_test t1 join employees_for_fallback_ut_test t2 on t1.age = t2.age sort by t1.age")
      val runRows = df.collect()
      val expectedRows = Seq(Row(56, 21000), Row(56, 21000),
        Row(66, 30000), Row(66, 30000),
        Row(70, 10000), Row(76, 32000),
        Row(76, 32000), Row(46, 29000),
        Row(50, 23000), Row(56, 29000),
        Row(56, 29000), Row(66, 23000),
        Row(66, 23000),Row(76, 35000),Row(76, 35000))
      assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
      val omniBhj = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.find({
        case _: ColumnarBroadcastHashJoinExec => true
        case _ => false
      })
      assert(omniBhj.isDefined, "bhj should be columnar")
      val r2cWithBroadCast = omniBhj.get.children.find({
        case p: RowToOmniColumnarExec =>
          p.child.isInstanceOf[BroadcastQueryStageExec] &&
            p.child.asInstanceOf[BroadcastQueryStageExec].plan.isInstanceOf[BroadcastExchangeExec]
        case _ => false
      })
      assert(r2cWithBroadCast.isDefined, "OmniBhj should use omni r2c to adapt to broadcast exchange")
    }
  }

  test("Fall back stage contain bhj") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "3"),
      ("spark.omni.sql.columnar.project", "false"),
      (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true"),
      (SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "10MB")) {
      val df = spark.sql("select t1.age * 2, t2.salary from employees_for_fallback_ut_test t1 join employees_for_fallback_ut_test t2 on t1.age = t2.age sort by t1.age")
      val runRows = df.collect()
      val expectedRows = Seq(Row(56, 21000), Row(56, 21000),
        Row(66, 30000), Row(66, 30000),
        Row(70, 10000), Row(76, 32000),
        Row(76, 32000), Row(46, 29000),
        Row(50, 23000), Row(56, 29000),
        Row(56, 29000), Row(66, 23000),
        Row(66, 23000),Row(76, 35000),Row(76, 35000))
      assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
      val plans = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.collect({
        case plan: BroadcastHashJoinExec => plan
      })
      assert(plans.size == 1, "the last stage containing bhj should fallback")
    }
  }

  test("Fall back the last stage contains unsupported bnlj if meeting the configured threshold") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "2"),
      (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true"),("spark.omni.sql.columnar.broadcastNestedJoin","false")) {
      val df = spark.sql("select age + salary from (select age, salary from (select age from employees_for_fallback_ut_test order by age limit 1) s1," +
        " (select salary from employees_for_fallback_ut_test order by salary limit 1) s2)")
      QueryTest.checkAnswer(df, Seq(Row(10023)))
      val plans = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.collect({
        case plan: ProjectExec => plan
        case plan: TakeOrderedAndProjectExec => plan
      })
      assert(plans.count(_.isInstanceOf[ProjectExec]) == 1, "the last stage containing projectExec should fallback")
      assert(plans.count(_.isInstanceOf[TakeOrderedAndProjectExec]) == 1, "the last stage containing projectExec should fallback")
    }
  }

  test("Don't Fall back the last stage contains supported bnlj if NOT meeting the configured threshold") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "3"),
      (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")) {
      val df = spark.sql("select age + salary from (select age, salary from (select age from employees_for_fallback_ut_test order by age limit 1) s1," +
        " (select salary from employees_for_fallback_ut_test order by salary limit 1) s2)")
      QueryTest.checkAnswer(df, Seq(Row(10023)))
      val plans = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan.collect({
        case plan: ColumnarProjectExec => plan
        case plan: ColumnarTakeOrderedAndProjectExec => plan
      })
      assert(plans.count(_.isInstanceOf[ColumnarProjectExec]) == 1, "the last stage containing projectExec should not fallback")
      assert(plans.count(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]) == 1, "the last stage containing projectExec should not fallback")
    }
  }


  test("Fall back the whole query if one unsupported") {
    withSQLConf(("spark.omni.sql.columnar.query.fallback.threshold", "1")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        UnaryOp2(ColumnarUnaryOp1(UnaryOp2(ColumnarUnaryOp1(LeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      assert(outputPlan == originalPlan)
    }
  }

  test("Fall back the whole plan if meeting the configured threshold") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "1")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext()
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        UnaryOp2(ColumnarUnaryOp1(UnaryOp2(ColumnarUnaryOp1(LeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      assert(outputPlan == originalPlan)
    }
  }

  test("Don't fall back the whole plan if NOT meeting the configured threshold") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "4")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext()
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        UnaryOp2(ColumnarUnaryOp1(UnaryOp2(ColumnarUnaryOp1(LeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      val columnarUnaryOps = outputPlan.collect({
        case p: ColumnarUnaryOp1 => p
      })
      // Expect to get the plan with columnar rule applied.
      assert(columnarUnaryOps.size == 2)
      assert(outputPlan != originalPlan)
    }
  }

  test(
    "Fall back the whole plan if meeting the configured threshold (leaf node is" +
      " transformable)") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "2")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext() // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer
      // and replacing LeafOp with LeafOpTransformer.
      val planAfterPreOverride =
        UnaryOp2(ColumnarUnaryOp1(UnaryOp2(ColumnarUnaryOp1(ColumnarLeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      val columnarUnaryOps = outputPlan.collect({
        case p: ColumnarUnaryOp1 => p
      })
      assert(columnarUnaryOps.isEmpty)
      assert(outputPlan == originalPlan)
    }
  }

  test(
    "Don't Fall back the whole plan if NOT meeting the configured threshold (" +
      "leaf node is transformable)") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "3")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext()
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer
      // and replacing LeafOp with LeafOpTransformer.
      val planAfterPreOverride =
        UnaryOp2(ColumnarUnaryOp1(UnaryOp2(ColumnarUnaryOp1(ColumnarLeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to get the plan with columnar rule applied.
      val columnarUnaryOps = outputPlan.collect({
        case p: ColumnarUnaryOp1 => p
        case p: ColumnarLeafOp => p
      })
      assert(columnarUnaryOps.size == 3)
      assert(outputPlan != originalPlan)
    }
  }

  test("Don't fall back the whole query if all supported") {
    withSQLConf(("spark.omni.sql.columnar.query.fallback.threshold", "1")) {
      val originalPlan = UnaryOp1(UnaryOp1(UnaryOp1(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        ColumnarUnaryOp1(ColumnarUnaryOp1(ColumnarUnaryOp1(ColumnarUnaryOp1(ColumnarLeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to not fall back the entire plan.
      val columnPlans = outputPlan.collect({
        case p@(ColumnarExchange1(_, _) | ColumnarUnaryOp1(_, _) | ColumnarLeafOp(_)) => p
      })
      assert(columnPlans.size == 5)
      assert(outputPlan != originalPlan)
    }
  }

  test("Don't fall back the whole plan if all supported") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "1")) {
      val originalPlan = Exchange1(UnaryOp1(UnaryOp1(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext()
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        ColumnarExchange1(ColumnarUnaryOp1(ColumnarUnaryOp1(ColumnarUnaryOp1(ColumnarLeafOp()))))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to not fall back the entire plan.
      val columnPlans = outputPlan.collect({
        case p@(ColumnarExchange1(_, _) | ColumnarUnaryOp1(_, _) | ColumnarLeafOp(_)) => p
      })
      assert(columnPlans.size == 5)
      assert(outputPlan != originalPlan)
    }
  }

  test("Fall back the whole plan if one supported plan before queryStage") {
    withSQLConf(("spark.omni.sql.columnar.wholeStage.fallback.threshold", "1")) {
      val mockQueryStageExec = QueryStageExec1(LeafOp(), LeafOp())
      val originalPlan = Exchange1(UnaryOp1(UnaryOp1(mockQueryStageExec)))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext()
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        ColumnarExchange1(ColumnarUnaryOp1(UnaryOp1(mockQueryStageExec)))
      val planWithTransition = InsertTransitions.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      val columnPlans = outputPlan.collect({
        case p@(ColumnarExchange1(_, _) | ColumnarUnaryOp1(_, _) | ColumnarLeafOp(_)) => p
      })
      assert(columnPlans.isEmpty)
      assert(outputPlan == originalPlan)
    }
  }

}

case class LeafOp(override val supportsColumnar: Boolean = false) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = Seq.empty
}

case class UnaryOp1(child: SparkPlan, override val supportsColumnar: Boolean = false)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp1 =
    copy(child = newChild)
}

case class UnaryOp2(child: SparkPlan, override val supportsColumnar: Boolean = false)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp2 =
    copy(child = newChild)
}

// For replacing LeafOp.
case class ColumnarLeafOp(override val supportsColumnar: Boolean = true)
  extends LeafExecNode {

  override def nodeName: String = "OmniColumnarLeafOp"

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = Seq.empty
}

// For replacing UnaryOp1.
case class ColumnarUnaryOp1(
                             override val child: SparkPlan,
                             override val supportsColumnar: Boolean = true)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = "OmniColumnarUnaryOp1"

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarUnaryOp1 =
    copy(child = newChild)
}

case class Exchange1(
                      override val child: SparkPlan,
                      override val supportsColumnar: Boolean = false)
  extends Exchange {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): Exchange1 =
    copy(child = newChild)
}

case class ColumnarExchange1(
                              override val child: SparkPlan,
                              override val supportsColumnar: Boolean = true)
  extends Exchange {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = "OmniColumnarExchange"

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarExchange1 =
    copy(child = newChild)
}

case class QueryStageExec1(override val plan: SparkPlan,
                           override val _canonicalized: SparkPlan) extends QueryStageExec {

  override val id: Int = 0

  override def doMaterialize(): Future[Any] = throw new UnsupportedOperationException("it is mock spark plan")

  override def cancel(): Unit = {}

  override def newReuseInstance(newStageId: Int, newOutput: Seq[Attribute]): QueryStageExec = throw new UnsupportedOperationException("it is mock spark plan")

  override def getRuntimeStatistics: Statistics = throw new UnsupportedOperationException("it is mock spark plan")
}
