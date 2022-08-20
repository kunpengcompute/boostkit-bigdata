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
        |AND datetype=DATE '2022-01-01' AND timestamptype=TIMESTAMP '2022-01-01'
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
}
