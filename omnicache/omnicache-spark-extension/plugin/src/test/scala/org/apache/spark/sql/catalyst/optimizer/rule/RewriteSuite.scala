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


package org.apache.spark.sql.catalyst.optimizer.rule

import com.huawei.boostkit.spark.util.RewriteHelper._
import java.io.File
import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{sideBySide, toPrettySQL}
import org.apache.spark.sql.types.StringType

class RewriteSuite extends SparkFunSuite with PredicateHelper {

  System.setProperty("HADOOP_USER_NAME", "root")
  lazy val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.sql.extensions", "com.huawei.boostkit.spark.OmniCache")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.ui.port", "4050")
      // .config("spark.sql.planChangeLog.level","WARN")
      .enableHiveSupport()
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  lazy val catalog: SessionCatalog = spark.sessionState.catalog

  override def beforeEach(): Unit = {
    enableCachePlugin()
  }

  def preDropTable(): Unit = {
    if (File.separatorChar == '\\') {
      return
    }
    spark.sql("DROP TABLE IF EXISTS locations").show()
    spark.sql("DROP TABLE IF EXISTS depts").show()
    spark.sql("DROP TABLE IF EXISTS emps").show()
    spark.sql("DROP TABLE IF EXISTS column_type").show()
  }

  def preCreateTable(): Unit = {
    if (catalog.tableExists(TableIdentifier("locations"))) {
      return
    }
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS locations(
        |  locationid INT,
        |  state STRING
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE locations VALUES(1,'state1');
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE locations VALUES(2,'state2');
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS depts(
        |  deptno INT,
        |  deptname STRING
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE depts VALUES(1,'deptname1');
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE depts VALUES(2,'deptname2');
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS emps(
        |  empid INT,
        |  deptno INT,
        |  locationid INT,
        |  empname STRING,
        |  salary DOUBLE
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE emps VALUES(1,1,1,'empname1',1.0);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE emps VALUES(2,2,2,'empname2',2.0);
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS column_type(
        |  empid INT,
        |  deptno INT,
        |  locationid INT,
        |  booleantype BOOLEAN,
        |  bytetype BYTE,
        |  shorttype SHORT,
        |  integertype INT,
        |  longtype LONG,
        |  floattype FLOAT,
        |  doubletype DOUBLE,
        |  datetype DATE,
        |  timestamptype TIMESTAMP,
        |  stringtype STRING,
        |  decimaltype DECIMAL
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   1,1,1,TRUE,1,1,1,1,1.0,1.0,
        |   DATE '2022-01-01',
        |   TIMESTAMP '2022-01-01',
        |   'stringtype1',1.0
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   2,2,2,TRUE,2,2,2,2,2.0,2.0,
        |   DATE '2022-02-02',
        |   TIMESTAMP '2022-02-02',
        |   'stringtype2',2.0
        |);
        |""".stripMargin
    )
  }

  preCreateTable()

  def transformAllExpressions(plan: LogicalPlan,
      rule: PartialFunction[Expression, Expression]): LogicalPlan = {
    plan.transformUp {
      case q: QueryPlan[_] => q.transformExpressions(rule)
    }
  }

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan): LogicalPlan = {
    transformAllExpressions(plan, {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name.replaceAll("#\\d+", "")
          , a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        a.child match {
          case agg@AggregateExpression(_, _, _, _, _) =>
            Alias(a.child, toPrettySQL(agg))(exprId = ExprId(0))
          case _ =>
            Alias(a.child, a.name.replaceAll("#\\d+", ""))(exprId = ExprId(0))
        }
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
      case lv: NamedLambdaVariable =>
        lv.copy(exprId = ExprId(0), value = null)
      case udf: PythonUDF =>
        udf.copy(resultId = ExprId(0))
    })
  }

  protected def rewriteNameFromAttrNullability(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case a@AttributeReference(name, _, false, _) =>
        a.copy(name = s"*$name")(exprId = a.exprId, qualifier = a.qualifier)
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   * etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   */
  protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteBinaryComparison)
            .sortBy(_.hashCode()).reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val newJoinType = joinType match {
          case ExistenceJoin(a: Attribute) =>
            val newAttr = AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
            ExistenceJoin(newAttr)
          case other => other
        }

        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteBinaryComparison)
              .sortBy(_.hashCode()).reduce(And)
        Join(left, right, newJoinType, Some(newCondition), hint)
    }
  }

  /**
   * Rewrite [[BinaryComparison]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   * 3. (a > b), (b < a)
   */
  private def rewriteBinaryComparison(condition: Expression): Expression = condition match {
    case EqualTo(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case EqualNullSafe(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
    case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)
    case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)
    case _ => condition // Don't reorder.
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      checkAnalysis: Boolean = true): Unit = {
    if (checkAnalysis) {
      // Make sure both plan pass checkAnalysis.
      SimpleAnalyzer.checkAnalysis(plan1)
      SimpleAnalyzer.checkAnalysis(plan2)
    }

    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${
          sideBySide(
            rewriteNameFromAttrNullability(normalized1).treeString,
            rewriteNameFromAttrNullability(normalized2).treeString).mkString("\n")
        }
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation()), Filter(e2, OneRowRelation()), checkAnalysis = false)
  }

  def compareRows(r1: Array[Row], r2: Array[Row]): Unit = {
    compareRows(r1, r2, noData = false)
  }

  def compareRows(r1: Array[Row], r2: Array[Row], noData: Boolean): Unit = {
    if (noData) {
      return
    }
    val sr1 = r1.sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    val sr2 = r2.sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    assert((noData || sr1.length > 0) && (sr1 sameElements sr2))
  }

  def comparePlansAndRows(sql: String, expectedPlan: LogicalPlan, noData: Boolean): Unit = {
    // 1.prepare
    val (rewritePlan, rewriteRows) = getPlanAndRows(sql)
    expectedPlan.setAnalyzed()

    // 2.compare plan
    comparePlans(rewritePlan, expectedPlan)

    // 3.compare row
    disableCachePlugin()
    val expectedRows = getRows(sql)
    compareRows(rewriteRows, expectedRows, noData)
  }

  def comparePlansAndRows(sql: String, expectedPlan: LogicalPlan): Unit = {
    comparePlansAndRows(sql, expectedPlan, noData = false)
  }

  def getStringAttr(name: String): AttributeReference = {
    AttributeReference(name, StringType)()
  }

  def getStringAttrSeq(nameSeq: Seq[String]): Seq[AttributeReference] = {
    var seq = Seq.empty[AttributeReference]
    nameSeq.foreach(n => seq :+= AttributeReference(n, StringType)())
    seq
  }

  def getRows(sql: String): Array[Row] = {
    val df = spark.sql(sql)
    val rows = getRows(df)
    rows
  }

  def getRows(df: DataFrame): Array[Row] = {
    val rows = df.collect()
    rows
  }

  def getTableRelation(name: String): LogicalPlan = {
    spark.table(name).queryExecution.optimizedPlan
  }

  def getPlanAndRows(sql: String): (LogicalPlan, Array[Row]) = {
    val df = spark.sql(sql)
    // df.explain(true)
    val rows = getRows(df)
    val plan = df.queryExecution.optimizedPlan
    (plan, rows)
  }

  def compareSql(sql1: String, sql2: String): Unit = {
    assert(
      sql1.trim.toLowerCase(Locale.ROOT) == sql2
          .trim.toLowerCase(Locale.ROOT)
    )
  }

  def compareError(errorInfo: String)(f: => Any): Any = {
    try {
      f
    } catch {
      case e: Throwable =>
        assert(e.getMessage.toLowerCase(Locale.ROOT)
            .contains(errorInfo.toLowerCase(Locale.ROOT)))
    }
  }
}
