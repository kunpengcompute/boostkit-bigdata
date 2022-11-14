/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ColumnarProjectExec, ColumnarSparkPlanTest, ProjectExec}

class ColumnarBuiltInFuncSuite extends ColumnarSparkPlanTest{
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var buildInDf: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    buildInDf = Seq[(String, String, String, String, Long, Int, String, String)](
      (null, "ChaR1    R", null, "  varchar100 ", 1001L, 1, "  中文1aA ", "varchar100_normal"),
      ("char200   ", "char2     ", "varchar2", "", 1002L, 2, "中文2bB", "varchar200_normal"),
      ("char300   ", "char3     ", "varchar3", "varchar300", 1003L, 3, "中文3cC", "varchar300_normal"),
      (null, "char4     ", "varchar4", "varchar400", 1004L, 4, null, "varchar400_normal")
    ).toDF("char_null", "char_normal", "varchar_null", "varchar_empty", "long_col", "int_col", "ch_col", "varchar_normal")
    buildInDf.createOrReplaceTempView("builtin_table")
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with normal") {
    val sql = "select lower(char_normal) from builtin_table"
    val expected = Seq(
      Row("char1    r"),
      Row("char2     "),
      Row("char3     "),
      Row("char4     ")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with null") {
    val sql = "select lower(char_null) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("char200   "),
      Row("char300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with space/empty string") {
    val sql = "select lower(varchar_empty) from builtin_table"
    val expected = Seq(
      Row("  varchar100 "),
      Row(""),
      Row("varchar300"),
      Row("varchar400")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower-lower") {
    val sql = "select lower(char_null), lower(varchar_null) from builtin_table"
    val expected = Seq(
      Row(null, null),
      Row("char200   ", "varchar2"),
      Row("char300   ", "varchar3"),
      Row(null, "varchar4"),
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower(lower())") {
    val sql = "select lower(lower(char_null)) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("char200   "),
      Row("char300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with subQuery") {
    val sql = "select lower(l) from (select lower(char_normal) as l from builtin_table)"
    val expected = Seq(
      Row("char1    r"),
      Row("char2     "),
      Row("char3     "),
      Row("char4     ")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with ch") {
    val sql = "select lower(ch_col) from builtin_table"
    val expected = Seq(
      Row("  中文1aa "),
      Row("中文2bb"),
      Row("中文3cc"),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with normal") {
    val sql = "select length(char_normal) from builtin_table"
    val expected = Seq(
      Row(10),
      Row(10),
      Row(10),
      Row(10)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with null") {
    val sql = "select length(char_null) from builtin_table"
    val expected = Seq(
      Row(null),
      Row(10),
      Row(10),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with space/empty string") {
    val sql = "select length(varchar_empty) from builtin_table"
    val expected = Seq(
      Row(13),
      Row(0),
      Row(10),
      Row(10)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with expr") {
    val sql = "select length(char_null) / 2 from builtin_table"
    val expected = Seq(
      Row(null),
      Row(5.0),
      Row(5.0),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length-length") {
    val sql = "select length(char_null),length(varchar_null) from builtin_table"
    val expected = Seq(
      Row(null, null),
      Row(10, 8),
      Row(10, 8),
      Row(null, 8)
    )
    checkResult(sql, expected)
  }

  // replace(str, search, replaceStr)
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with matched and replace str") {
    val sql = "select replace(varchar_normal,varchar_empty,char_normal) from builtin_table"
    val expected = Seq(
      Row("varchar100_normal"),
      Row("varchar200_normal"),
      Row("char3     _normal"),
      Row("char4     _normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with not matched") {
    val sql = "select replace(char_normal,varchar_normal,char_normal) from builtin_table"
    val expected = Seq(
      Row("ChaR1    R"),
      Row("char2     "),
      Row("char3     "),
      Row("char4     ")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str null") {
    val sql = "select replace(varchar_null,char_normal,varchar_normal) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("varchar2"),
      Row("varchar3"),
      Row("varchar4")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str space/empty") {
    val sql = "select replace(varchar_empty,varchar_empty,varchar_normal) from builtin_table"
    val expected = Seq(
      Row("varchar100_normal"),
      Row(""),
      Row("varchar300_normal"),
      Row("varchar400_normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with search null") {
    val sql = "select replace(varchar_normal,varchar_null,char_normal) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("char2     00_normal"),
      Row("char3     00_normal"),
      Row("char4     00_normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with search space/empty") {
    val sql = "select replace(varchar_normal,varchar_empty,char_normal) from builtin_table"
    val expected = Seq(
      Row("varchar100_normal"),
      Row("varchar200_normal"),
      Row("char3     _normal"),
      Row("char4     _normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr null") {
    val sql = "select replace(varchar_normal,varchar_empty,varchar_null) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("varchar200_normal"),
      Row("varchar3_normal"),
      Row("varchar4_normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr space/empty") {
    val sql = "select replace(varchar_normal,varchar_normal,varchar_empty) from builtin_table"
    val expected = Seq(
      Row("  varchar100 "),
      Row(""),
      Row("varchar300"),
      Row("varchar400")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str/search/replace all null") {
    val sql = "select replace(varchar_null,varchar_null,char_null) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("char200   "),
      Row("char300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr default") {
    val sql = "select replace(varchar_normal,varchar_normal) from builtin_table"
    val expected = Seq(
      Row(""),
      Row(""),
      Row(""),
      Row("")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with subReplace(normal,normal,normal)") {
    val sql = "select replace(res,'c','ccc') from (select replace(varchar_normal,varchar_empty,char_normal) as res from builtin_table)"
    val expected = Seq(
      Row("varccchar100_normal"),
      Row("varccchar200_normal"),
      Row("ccchar3     _normal"),
      Row("ccchar4     _normal")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with subReplace(null,null,null)") {
    val sql = "select replace(res,'c','ccc') from (select replace(varchar_null,varchar_null,char_null) as res from builtin_table)"
    val expected = Seq(
      Row(null),
      Row("ccchar200   "),
      Row("ccchar300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace(replace)") {
    val sql = "select replace(replace('ABCabc','AB','abc'),'abc','DEF')"
    val expected = Seq(
      Row("DEFCDEF")
    )
    checkResult(sql, expected)
  }

  // upper
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper with normal") {
    val sql = "select upper(char_normal) from builtin_table"
    val expected = Seq(
      Row("CHAR1    R"),
      Row("CHAR2     "),
      Row("CHAR3     "),
      Row("CHAR4     ")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper with null") {
    val sql = "select upper(char_null) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("CHAR200   "),
      Row("CHAR300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper with space/empty string") {
    val sql = "select upper(varchar_empty) from builtin_table"
    val expected = Seq(
      Row("  VARCHAR100 "),
      Row(""),
      Row("VARCHAR300"),
      Row("VARCHAR400")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper-upper") {
    val sql = "select upper(char_null), upper(varchar_null) from builtin_table"
    val expected = Seq(
      Row(null, null),
      Row("CHAR200   ", "VARCHAR2"),
      Row("CHAR300   ", "VARCHAR3"),
      Row(null, "VARCHAR4"),
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper(upper())") {
    val sql = "select upper(upper(char_null)) from builtin_table"
    val expected = Seq(
      Row(null),
      Row("CHAR200   "),
      Row("CHAR300   "),
      Row(null)
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper with subQuery") {
    val sql = "select upper(l) from (select upper(char_normal) as l from builtin_table)"
    val expected = Seq(
      Row("CHAR1    R"),
      Row("CHAR2     "),
      Row("CHAR3     "),
      Row("CHAR4     ")
    )
    checkResult(sql, expected)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute upper with ch") {
    val sql = "select upper(ch_col) from builtin_table"
    val expected = Seq(
      Row("  中文1AA "),
      Row("中文2BB"),
      Row("中文3CC"),
      Row(null)
    )
    checkResult(sql, expected)
  }

  def checkResult(sql: String, expected: Seq[Row], isUseOmni: Boolean = true): Unit = {
    def assertOmniProjectHappen(res: DataFrame): Unit = {
      val executedPlan = res.queryExecution.executedPlan
      assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
      assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    }
    def assertOmniProjectNotHappen(res: DataFrame): Unit = {
      val executedPlan = res.queryExecution.executedPlan
      assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty, s"ColumnarProjectExec happened, executedPlan as follows： \n$executedPlan")
      assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined, s"ProjectExec not happened, executedPlan as follows： \n$executedPlan")
    }
    val res = spark.sql(sql)
    if (isUseOmni) assertOmniProjectHappen(res) else assertOmniProjectNotHappen(res)
    checkAnswer(res, expected)
  }
}
