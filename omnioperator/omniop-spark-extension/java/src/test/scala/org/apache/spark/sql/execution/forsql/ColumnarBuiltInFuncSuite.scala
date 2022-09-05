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
      (null, "ChaR1    R", null, "  varchar100 ", 1001L, 1, "中文1", "varchar100_normal"),
      ("char200   ", "char2     ", "varchar2", "", 1002L, 2, "中文2", "varchar200_normal"),
      ("char300   ", "char3     ", "varchar3", "varchar300", 1003L, 3, "中文3", "varchar300_normal"),
      (null, "char4     ", "varchar4", "varchar400", 1004L, 4, "中文4", "varchar400_normal")
    ).toDF("char_null", "char_normal", "varchar_null", "varchar_empty", "long_col", "int_col", "ch_col", "varchar_normal")
    buildInDf.createOrReplaceTempView("builtin_table")
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with normal") {
    val res = spark.sql("select lower(char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("char1    r"),
        Row("char2     "),
        Row("char3     "),
        Row("char4     ")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with null") {
    val res = spark.sql("select lower(char_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("char200   "),
        Row("char300   "),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with space/empty string") {
    val res = spark.sql("select lower(varchar_empty) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("  varchar100 "),
        Row(""),
        Row("varchar300"),
        Row("varchar400")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower-lower") {
    val res = spark.sql("select lower(char_null), lower(varchar_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null, null),
        Row("char200   ", "varchar2"),
        Row("char300   ", "varchar3"),
        Row(null, "varchar4"),
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower(lower())") {
    val res = spark.sql("select lower(lower(char_null)) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("char200   "),
        Row("char300   "),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with subQuery") {
    val res = spark.sql("select lower(l) from (select lower(char_normal) as l from builtin_table)")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("char1    r"),
        Row("char2     "),
        Row("char3     "),
        Row("char4     ")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute lower with ch") {
    val res = spark.sql("select lower(ch_col) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("中文1"),
        Row("中文2"),
        Row("中文3"),
        Row("中文4")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with normal") {
    val res = spark.sql("select length(char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(10),
        Row(10),
        Row(10)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with null") {
    val res = spark.sql("select length(char_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(10),
        Row(10),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with space/empty string") {
    val res = spark.sql("select length(varchar_empty) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(13),
        Row(0),
        Row(10),
        Row(10)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length with expr") {
    val res = spark.sql("select length(char_null) / 2 from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(5.0),
        Row(5.0),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute length-length") {
    val res = spark.sql("select length(char_null),length(varchar_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null, null),
        Row(10, 8),
        Row(10, 8),
        Row(null, 8)
      )
    )
  }

  // replace(str, search, replaceStr)
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with matched and replace str") {
    val res = spark.sql("select replace(varchar_normal,varchar_empty,char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("varchar100_normal"),
        Row("varchar200_normal"),
        Row("char3     _normal"),
        Row("char4     _normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with not matched") {
    val res = spark.sql("select replace(char_normal,varchar_normal,char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("ChaR1    R"),
        Row("char2     "),
        Row("char3     "),
        Row("char4     ")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str null") {
    val res = spark.sql("select replace(varchar_null,char_normal,varchar_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("varchar2"),
        Row("varchar3"),
        Row("varchar4")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str space/empty") {
    val res = spark.sql("select replace(varchar_empty,varchar_empty,varchar_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("varchar100_normal"),
        Row(""),
        Row("varchar300_normal"),
        Row("varchar400_normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with search null") {
    val res = spark.sql("select replace(varchar_normal,varchar_null,char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("char2     00_normal"),
        Row("char3     00_normal"),
        Row("char4     00_normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with search space/empty") {
    val res = spark.sql("select replace(varchar_normal,varchar_empty,char_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("varchar100_normal"),
        Row("varchar200_normal"),
        Row("char3     _normal"),
        Row("char4     _normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr null") {
    val res = spark.sql("select replace(varchar_normal,varchar_empty,varchar_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("varchar200_normal"),
        Row("varchar3_normal"),
        Row("varchar4_normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr space/empty") {
    val res = spark.sql("select replace(varchar_normal,varchar_normal,varchar_empty) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("  varchar100 "),
        Row(""),
        Row("varchar300"),
        Row("varchar400")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with str/search/replace all null") {
    val res = spark.sql("select replace(varchar_null,varchar_null,char_null) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("char200   "),
        Row("char300   "),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with replaceStr default") {
    val res = spark.sql("select replace(varchar_normal,varchar_normal) from builtin_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(""),
        Row(""),
        Row(""),
        Row("")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with subReplace(normal,normal,normal)") {
    val res = spark.sql("select replace(res,'c','ccc') from (select replace(varchar_normal,varchar_empty,char_normal) as res from builtin_table)")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("varccchar100_normal"),
        Row("varccchar200_normal"),
        Row("ccchar3     _normal"),
        Row("ccchar4     _normal")
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace with subReplace(null,null,null)") {
    val res = spark.sql("select replace(res,'c','ccc') from (select replace(varchar_null,varchar_null,char_null) as res from builtin_table)")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row("ccchar200   "),
        Row("ccchar300   "),
        Row(null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when execute replace(replace)") {
    val res = spark.sql("select replace(replace('ABCabc','AB','abc'),'abc','DEF')")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row("DEFCDEF")
      )
    )
  }
}
