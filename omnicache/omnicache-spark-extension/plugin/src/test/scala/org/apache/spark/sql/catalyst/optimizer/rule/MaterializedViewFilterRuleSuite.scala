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

class MaterializedViewFilterRuleSuite extends RewriteSuite {

  test("mv_filter1") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_filter1;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_filter1
        |AS
        |SELECT * FROM COLUMN_TYPE WHERE empid=1;
        |""".stripMargin
    )
  }

  test("mv_filter1_1") {
    // different column type compare predict =
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND booleantype=TRUE AND bytetype=1 AND shorttype=1 AND integertype=1
        |AND longtype=1 AND floattype=1.0 AND doubletype=1.0
        |AND datetype=DATE '2022-01-01' AND timestamptype=TIMESTAMP '2022-01-01 00:00:00.0'
        |AND stringtype='stringtype1' AND decimaltype=1.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_2") {
    // different column type compare predict >
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype>0 AND shorttype>0 AND integertype>0
        |AND longtype>0 AND floattype>0.0 AND doubletype>0.0
        |AND datetype>DATE '2021-01-01' AND timestamptype>TIMESTAMP '2021-01-01'
        |AND stringtype>'stringtype0' AND decimaltype>0.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_3") {
    // different column type compare predict >=
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype>=0 AND shorttype>=0 AND integertype>=0
        |AND longtype>=0 AND floattype>=0.0 AND doubletype>=0.0
        |AND datetype>=DATE '2021-01-01' AND timestamptype>=TIMESTAMP '2021-01-01'
        |AND stringtype>='stringtype0' AND decimaltype>=0.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_4") {
    // different column type compare predict <
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype<2 AND shorttype<2 AND integertype<2
        |AND longtype<2 AND floattype<2.0 AND doubletype<2.0
        |AND datetype<DATE '2023-01-01' AND timestamptype<TIMESTAMP '2023-01-01'
        |AND stringtype<'stringtype2' AND decimaltype<2.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_5") {
    // different column type compare predict <=
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype<=2 AND shorttype<=2 AND integertype<=2
        |AND longtype<=2 AND floattype<=2.0 AND doubletype<=2.0
        |AND datetype<=DATE '2023-01-01' AND timestamptype<=TIMESTAMP '2023-01-01'
        |AND stringtype<='stringtype2' AND decimaltype<=2.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_6") {
    // different column type residual predict !=
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype!=2 AND shorttype!=2 AND integertype!=2
        |AND longtype!=2 AND floattype!=2.0 AND doubletype!=2.0
        |AND datetype!=DATE '2023-01-01' AND timestamptype!=TIMESTAMP '2023-01-01'
        |AND stringtype!='stringtype2' AND decimaltype!=2.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_7") {
    // different column type residual predict like
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND stringtype like 'stringtype1';
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_8") {
    // different column type residual predict in
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype in (1) AND shorttype in (1) AND integertype in (1)
        |AND longtype in (1) AND floattype in (1.0) AND doubletype in (1.0)
        |AND datetype in (DATE '2022-01-01') AND timestamptype in (TIMESTAMP '2022-01-01')
        |AND stringtype in ('stringtype1') AND decimaltype in (1.0);
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_9") {
    // different column type residual predict not in
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype not in (2) AND shorttype not in (2) AND integertype not in (2)
        |AND longtype not in (2) AND floattype not in (2.0) AND doubletype not in (2.0)
        |AND datetype not in (DATE '2023-01-01') AND timestamptype not in (TIMESTAMP '2023-01-01')
        |AND stringtype not in ('stringtype2') AND decimaltype not in (2.0);
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_10") {
    // different column type residual predict is null
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype is null AND shorttype is null AND integertype is null
        |AND longtype is null AND floattype is null AND doubletype is null
        |AND datetype is null AND timestamptype is null
        |AND stringtype is null AND decimaltype is null;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_11") {
    // different column type residual predict is not null
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype is not null AND shorttype is not null AND integertype is not null
        |AND longtype is not null AND floattype is not null AND doubletype is not null
        |AND datetype is not null AND timestamptype is not null
        |AND stringtype is not null AND decimaltype is not null;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_12") {
    // different column type residual predict between and
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=1
        |AND bytetype between 1 and 2 AND shorttype between 1 and 2
        |AND integertype between 1 and 2
        |AND longtype between 1 and 2 AND floattype between 1.0 and 2.0
        |AND doubletype between 1.0 and 2.0
        |AND datetype between DATE '2022-01-01' and DATE '2023-01-01'
        |AND timestamptype between TIMESTAMP '2022-01-01' and TIMESTAMP '2023-01-01'
        |AND stringtype between 'stringtype1' and 'stringtype2'
        |AND decimaltype between 1.0 and 2.0;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter1", noData = false)
  }

  test("mv_filter1_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_filter1 DISABLE REWRITE;"
    spark.sql(sql).show()
  }

  test("mv_filter2") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_filter2;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_filter2
        |AS
        |SELECT * FROM COLUMN_TYPE WHERE empid=3
        |AND bytetype>1 AND shorttype<5 AND integertype>=1 AND longtype<=5
        |AND floattype in(3.0) AND doubletype not in(2.0)
        |AND datetype between DATE '2021-01-01' and DATE '2023-01-01'
        |AND stringtype='stringtype3'
        |AND timestamptype is not null AND decimaltype is null;
        |""".stripMargin
    )
  }

  test("mv_filter2_1") {
    // same to view
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=3
        |AND bytetype>1 AND shorttype<5 AND integertype>=1 AND longtype<=5
        |AND floattype in(3.0) AND doubletype not in(2.0)
        |AND datetype between DATE '2021-01-01' and DATE '2023-01-01'
        |AND stringtype='stringtype3'
        |AND timestamptype is not null AND decimaltype is null;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter2", noData = false)
  }

  test("mv_filter2_2") {
    // compare predicts is subset of view and residual predicts is same
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=3
        |AND bytetype>2 AND shorttype<4 AND integertype>=2 AND longtype<=4
        |AND floattype in(3.0) AND doubletype not in(2.0)
        |AND datetype between DATE '2022-01-01' and DATE '2022-04-01'
        |AND stringtype='stringtype3'
        |AND timestamptype is not null AND decimaltype is null;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_filter2", noData = false)
  }

  test("mv_filter2_3") {
    // compare predicts is not subset of view and residual predicts is same
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=3
        |AND bytetype>0 AND shorttype<6 AND integertype>=0 AND longtype<=6
        |AND floattype in(3.0) AND doubletype not in(2.0)
        |AND datetype between DATE '2020-01-01' and DATE '2024-01-01'
        |AND stringtype='stringtype3'
        |AND timestamptype is not null AND decimaltype is null;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
  }

  test("mv_filter2_4") {
    // compare predicts is subset of view and residual predicts is not same
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=3
        |AND bytetype>1 AND shorttype<5 AND integertype>=1 AND longtype<=5
        |AND floattype in(2.0) AND doubletype not in(3.0)
        |AND datetype between DATE '2021-01-01' and DATE '2023-01-01'
        |AND stringtype='stringtype3'
        |AND timestamptype is null AND decimaltype is not null;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = true)
  }

  test("mv_filter2_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_filter1 DISABLE REWRITE;"
    spark.sql(sql).show()
  }
}
