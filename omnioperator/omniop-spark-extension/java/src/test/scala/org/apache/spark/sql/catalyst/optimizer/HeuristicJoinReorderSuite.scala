/*
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class HeuristicJoinReorderSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
        Batch("Filter Pushdown", FixedPoint(100),
          CombineFilters,
          PushPredicateThroughNonJoin,
          BooleanSimplification,
          ReorderJoin,
          PushPredicateThroughJoin,
          ColumnPruning,
          RemoveNoopOperators,
          CollapseProject) ::
        Batch("Heuristic Join Reorder", FixedPoint(1),
          DelayCartesianProduct,
          HeuristicJoinReorder,
          PushDownPredicates,
          ColumnPruning,
          CollapseProject,
          RemoveNoopOperators) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  private val testRelation1 = LocalRelation('d.int)

  private val IOV_ALARM_DAILY = LocalRelation('DID.int, 'DATA_TIME.int)
  private val DETAILS = LocalRelation('CODE.int)
  private val IOV_BIZ_CAR_INFO_ALL2 = LocalRelation('DID.int, 'CBM_MAG_COMPANY_ID.string)
  private val IOV_BIZ_CAN_BUS_TYPE = LocalRelation('CODE.int, 'SITE.int, 'ID.int)
  private val CBM_COM_DDIC_CONTENT = LocalRelation('ID.int, 'CBM_COM_DDIC_TYPE_ID.int)
  private val CBM_COM_DDIC_TYPE = LocalRelation('ID.int, 'CODE.string)
  private val IOV_BIZ_L_OPTION_RANK_TYPE =
    LocalRelation('IOV_BIZ_CAN_BUS_TYPE_ID.int, 'CBM_COM_OPTION_RANK_ID.int)
  private val CBM_COM_OPTION_RANK = LocalRelation('ID.int, 'CBM_MAG_COMPANY_ID.int)

  test("reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze)
      comparePlans(optimized, queryAnswerPair._2.analyze)
    }
  }

  test("extract filters and joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    def testExtract(plan: LogicalPlan,
                    expected: Option[(Seq[LogicalPlan], Seq[Expression])]): Unit = {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractCheckCross(plan, expectedNoCross)
    }

    def testExtractCheckCross(plan: LogicalPlan, expected: Option[(Seq[(LogicalPlan, InnerLike)],
      Seq[Expression])]): Unit = {
      assert(
        ExtractFiltersAndInnerJoins.unapply(plan) === expected.map(e => (e._1, e._2)))
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some((Seq(x, y), Seq())))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(z), Some((Seq(x, y, z), Seq())))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some((Seq(x, y, z), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(x.join(z)), Some((Seq(x, y, x.join(z)), Seq())))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr))))

    testExtractCheckCross(x.join(y, Cross), Some((Seq((x, Cross), (y, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some((Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr))))
  }

  test("DelayCartesianProduct: beiqi scenario") {
    val T = IOV_ALARM_DAILY.subquery('T)
    val DT = DETAILS.subquery('DT)
    val C = IOV_BIZ_CAR_INFO_ALL2.subquery('C)
    val CAT = IOV_BIZ_CAN_BUS_TYPE.subquery('CAT)
    val DDIC = CBM_COM_DDIC_CONTENT.subquery('DDIC)
    val DDICT = CBM_COM_DDIC_TYPE.subquery('DDICT)
    val OPRL = IOV_BIZ_L_OPTION_RANK_TYPE.subquery('OPRL)
    val OPR = CBM_COM_OPTION_RANK.subquery('OPR)

    val query = T.join(DT, condition = None)
      .join(C, condition = Some("C.DID".attr === "T.DID".attr))
      .join(CAT, condition = Some("CAT.CODE".attr === "DT.CODE".attr))
      .join(DDIC, condition = Some("DDIC.ID".attr === "CAT.SITE".attr))
      .join(DDICT, condition = Some("DDICT.ID".attr === "DDIC.CBM_COM_DDIC_TYPE_ID".attr))
      .join(OPRL, condition = Some("OPRL.IOV_BIZ_CAN_BUS_TYPE_ID".attr === "CAT.ID".attr))
      .join(OPR, condition = Some("OPR.ID".attr === "OPRL.CBM_COM_OPTION_RANK_ID".attr))
      .where(("T.DATA_TIME".attr < 100)
        && ("C.CBM_MAG_COMPANY_ID".attr like "%500%")
        && ("OPR.CBM_MAG_COMPANY_ID".attr === -1)
        && ("DDICT.CODE".attr === "2004"))
    val optimized = Optimize.execute(query.analyze)

    val clique1 = T.where("T.DATA_TIME".attr < 100)
      .join(C.where("C.CBM_MAG_COMPANY_ID".attr like "%500%"),
        condition = Some("C.DID".attr === "T.DID".attr))
    val clique2 = DT.join(CAT, condition = Some("CAT.CODE".attr === "DT.CODE".attr))
      .join(DDIC, condition = Some("DDIC.ID".attr === "CAT.SITE".attr))
      .join(DDICT.where("DDICT.CODE".attr === "2004"),
        condition = Some("DDICT.ID".attr === "DDIC.CBM_COM_DDIC_TYPE_ID".attr))
      .join(OPRL, condition = Some("OPRL.IOV_BIZ_CAN_BUS_TYPE_ID".attr === "CAT.ID".attr))
      .join(OPR.where("OPR.CBM_MAG_COMPANY_ID".attr === -1),
        condition = Some("OPR.ID".attr === "OPRL.CBM_COM_OPTION_RANK_ID".attr))
    val expected = clique1.join(clique2, condition = None)
      .select(Seq("T.DID", "T.DATA_TIME", "DT.CODE", "C.DID", "C.CBM_MAG_COMPANY_ID", "CAT.CODE",
        "CAT.SITE", "CAT.ID", "DDIC.ID", "DDIC.CBM_COM_DDIC_TYPE_ID", "DDICT.ID", "DDICT.CODE",
        "OPRL.IOV_BIZ_CAN_BUS_TYPE_ID", "OPRL.CBM_COM_OPTION_RANK_ID", "OPR.ID",
        "OPR.CBM_MAG_COMPANY_ID").map(_.attr): _*).analyze

    comparePlans(optimized, expected)
  }

  test("DelayCartesianProduct: more than two cliques") {
    val big1 = testRelation.subquery('big1)
    val big2 = testRelation.subquery('big2)
    val big3 = testRelation.subquery('big3)
    val small1 = testRelation1.subquery('small1)
    val small2 = testRelation1.subquery('small2)
    val small3 = testRelation1.subquery('small3)
    val small4 = testRelation1.subquery('small4)

    val query = big1.join(big2, condition = None)
      .join(big3, condition = None)
      .join(small1, condition = Some("big1.a".attr === "small1.d".attr))
      .join(small2, condition = Some("big2.b".attr === "small2.d".attr))
      .join(small3, condition = Some("big3.a".attr === "small3.d".attr))
      .join(small4, condition = Some("big3.b".attr === "small4.d".attr))
    val optimized = Optimize.execute(query.analyze)

    val clique1 = big1.join(small1, condition = Some("big1.a".attr === "small1.d".attr))
    val clique2 = big2.join(small2, condition = Some("big2.b".attr === "small2.d".attr))
    val clique3 = big3.join(small3, condition = Some("big3.a".attr === "small3.d".attr))
      .join(small4, condition = Some("big3.b".attr === "small4.d".attr))
    val expected = clique1.join(clique2, condition = None)
      .join(clique3, condition = None)
      .select(Seq("big1.a", "big1.b", "big1.c", "big2.a", "big2.b", "big2.c", "big3.a",
        "big3.b", "big3.c", "small1.d", "small2.d", "small3.d", "small4.d").map(_.attr): _*)
      .analyze

    comparePlans(optimized, expected)
  }
}
