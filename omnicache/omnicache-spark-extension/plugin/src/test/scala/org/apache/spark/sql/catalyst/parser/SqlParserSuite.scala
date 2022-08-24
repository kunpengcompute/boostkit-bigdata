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

package org.apache.spark.sql.catalyst.parser

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import com.huawei.boostkit.spark.util.{RewriteHelper, ViewMetadata}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.optimizer.rule.RewriteSuite

class SqlParserSuite extends RewriteSuite {

  test("mv_create1") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_create1
        |  DISABLE REWRITE
        |  COMMENT 'mv_create1'
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT * FROM column_type
        |;
        |""".stripMargin
    ).show()

    val querySql = "SELECT * FROM column_type"
    val viewName = "mv_create1"

    // test metadata
    val catalogTable = catalog.getTableMetadata(TableIdentifier(viewName))
    compareSql(catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, ""), querySql.trim)
    assert(catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "").trim.equals("false"))

    // test table data
    val allColumns = catalogTable.schema.fields.map(_.name).mkString(",")
    val df1 = spark.sql(s"SELECT $allColumns FROM column_type").collect()
    val df2 = spark.sql(s"SELECT $allColumns FROM $viewName").collect()
    compareRows(df1, df2)
  }

  test("mv_create2") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS  default.mv_create2
        |AS
        |SELECT * FROM column_type
        |;
        |""".stripMargin
    ).show()

    val querySql = "SELECT * FROM column_type"
    val viewName = "mv_create2"

    // test metadata
    val catalogTable = catalog.getTableMetadata(TableIdentifier(viewName))
    compareSql(catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, ""), querySql.trim)
    assert(catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "").trim.equals("true"))

    // test table data
    val allColumns = catalogTable.schema.fields.map(_.name).mkString(",")
    val df1 = spark.sql(s"SELECT $allColumns FROM column_type").collect()
    val df2 = spark.sql(s"SELECT $allColumns FROM $viewName").collect()
    compareRows(df1, df2)
  }

  test("mv_create_join1") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_create_join1
        |  DISABLE REWRITE
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT c1.*,e1.empname,d1.deptname FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |;
        |""".stripMargin
    ).show()

    val querySql =
      """
        |SELECT c1.*,e1.empname,d1.deptname FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |""".stripMargin
    val viewName = "mv_create_join1"

    // test metadata
    val catalogTable = catalog.getTableMetadata(TableIdentifier(viewName))
    compareSql(catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, ""), querySql.trim)
    assert(catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "").trim.equals("false"))

    // test table data
    val allColumns = "%s,e1.empname,d1.deptname".format(
      catalog.getTableMetadata(TableIdentifier("column_type"))
          .schema.fields.map(f => "c1.%s".format(f.name)).mkString(",")
    )
    val sql1 =
      """
        |SELECT %s FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |""".stripMargin.format(allColumns)
    val sql2 = s"SELECT ${allColumns.replaceAll("[^,]+\\d\\.", "")}  FROM $viewName"
    val df1 = spark.sql(sql1).collect()
    val df2 = spark.sql(sql2).collect()
    compareRows(df1, df2)
  }

  test("mv_create_agg1") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_create_agg1
        |  DISABLE REWRITE
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,
        |count(1) as ct,min(c1.longtype) as _min FROM
        |column_type c1
        |GROUP BY c1.longtype,c1.doubletype,c1.datetype,c1.stringtype
        |;
        |""".stripMargin
    ).show()

    val querySql =
      """
        |SELECT c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,
        |count(1) as ct,min(c1.longtype) as _min FROM
        |column_type c1
        |GROUP BY c1.longtype,c1.doubletype,c1.datetype,c1.stringtype
        |""".stripMargin
    val viewName = "mv_create_agg1"

    // test metadata
    val catalogTable = catalog.getTableMetadata(TableIdentifier(viewName))
    compareSql(catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, ""), querySql.trim)
    assert(catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "").trim.equals("false"))

    // test table data
    val sql1 = querySql
    val sql2 =
      """
        |SELECT longtype,doubletype,datetype,stringtype,ct,_min FROM
        |mv_create_agg1
        |""".stripMargin
    val df1 = spark.sql(sql1).collect()
    val df2 = spark.sql(sql2).collect()
    compareRows(df1, df2)
  }

  test("mv_create_agg2") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_create_agg2
        |  DISABLE REWRITE
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,
        |e1.empid,d1.deptno,count(1) as ct,min(c1.longtype) as _min FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |GROUP BY c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,e1.empid,d1.deptno
        |;
        |""".stripMargin
    ).show()

    val querySql =
      """
        |SELECT c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,
        |e1.empid,d1.deptno,count(1) as ct,min(c1.longtype) as _min FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |GROUP BY c1.longtype,c1.doubletype,c1.datetype,c1.stringtype,e1.empid,d1.deptno
        |""".stripMargin
    val viewName = "mv_create_agg2"

    // test metadata
    val catalogTable = catalog.getTableMetadata(TableIdentifier(viewName))
    compareSql(catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, ""), querySql.trim)
    assert(catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "").trim.equals("false"))

    // test table data
    val sql1 = querySql
    val sql2 =
      """
        |SELECT longtype,doubletype,datetype,stringtype,
        |empid,deptno,ct,_min FROM
        |mv_create_agg2
        |""".stripMargin
    val df1 = spark.sql(sql1).collect()
    val df2 = spark.sql(sql2).collect()
    compareRows(df1, df2)
  }

  test("mv_create_error1") {
    compareError("view not found") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_create1
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
          |AS
          |SELECT * FROM column_type_xx
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_create_error2") {
    compareError("already exists") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW mv_create1
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
          |AS
          |SELECT * FROM column_type
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_create_error3") {
    compareError("missing") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW mv_create1
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
          |SELECT * FROM column_type
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_create_error4") {
    compareError("Partition column `longtypexxx` not found") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW mv_create1xxx
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtypexxx)
          |AS
          |SELECT * FROM column_type
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_create_error5") {
    compareError("Found duplicate column(s)") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW IF NOT EXISTS  mv_create_join1xxx
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
          |AS
          |SELECT * FROM
          |emps e1 JOIN column_type c1 JOIN depts d1
          |ON e1.empid=c1.empid
          |AND c1.deptno=d1.deptno
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_create_error6") {
    compareError("not support create mv from mv") {
      spark.sql(
        """
          |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_create1xxx
          |  DISABLE REWRITE
          |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
          |AS
          |SELECT * FROM mv_create1
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_drop1") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS default.mv_create_agg1
        |;
        |""".stripMargin
    ).show()

    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS default.mv_create_agg1
        |;
        |""".stripMargin
    ).show()
  }

  test("mv_drop2") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW mv_create_agg2
        |;
        |""".stripMargin
    ).show()
  }

  test("mv_drop_error1") {
    compareError("cannot drop a table with drop mv.") {
      spark.sql(
        """
          |DROP MATERIALIZED VIEW emps
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_drop_error2") {
    compareError("view not found") {
      spark.sql(
        """
          |DROP MATERIALIZED VIEW mv_create_agg2
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_show1") {
    assert {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS
          |;
          |""".stripMargin
      ).collect()
          .map(r => Row(r.getString(0), r.getString(1), r.getString(2), r.getString(4)))
          .contains(Row("default", "mv_create1", "false", "SELECT * FROM column_type"))
    }
  }

  test("mv_show2") {
    assert {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS ON mv_create1
          |;
          |""".stripMargin
      ).collect()
          .map(r => Row(r.getString(0), r.getString(1), r.getString(2), r.getString(4)))
          .contains(Row("default", "mv_create1", "false", "SELECT * FROM column_type"))
    }
  }

  test("mv_show3") {
    assert {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS ON default.mv_create1
          |;
          |""".stripMargin
      ).collect()
          .map(r => Row(r.getString(0), r.getString(1), r.getString(2), r.getString(4)))
          .contains(Row("default", "mv_create1", "false", "SELECT * FROM column_type"))
    }
  }

  test("mv_show4") {
    val sql =
      """
        |SELECT c1.*,e1.empname,d1.deptname FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |""".stripMargin.replaceAll("^[\r\n]+", "")

    val sql1 = sql.replaceAll("[\r\n]", "").trim
    val sql2 = sql.substring(0, OmniCachePluginConfig.getConf.showMVQuerySqlLen)
        .replaceAll("[\r\n]", "").trim

    assert {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS ON mv_create_join1
          |;
          |""".stripMargin
      ).collect()
          .map(r => Row(r.getString(0), r.getString(1), r.getString(2),
            r.getString(4).replaceAll("[\r\n]", "").trim))
          .contains(Row("default", "mv_create_join1", "false", sql1))
    }
    assert {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS
          |;
          |""".stripMargin
      ).collect()
          .map(r => Row(r.getString(0), r.getString(1), r.getString(2),
            r.getString(4).replaceAll("[\r\n]", "").trim))
          .contains(Row("default", "mv_create_join1", "false", sql2))
    }
  }

  test("mv_show_error1") {
    compareError("table or view not found") {
      spark.sql(
        """
          |SHOW MATERIALIZED VIEWS ON mv_createxxx
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_alter_rewrite1") {
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 DISABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    val table = TableIdentifier("mv_create1", Some("default"))
    assert(
      !spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      !ViewMetadata.isViewExists(table.quotedString)
    )
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    assert(
      spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      ViewMetadata.isViewExists(table.quotedString)
    )
  }

  test("mv_alter_rewrite2") {
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 DISABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    val table = TableIdentifier("mv_create1", Some("default"))
    assert(
      spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      ViewMetadata.isViewExists(table.quotedString)
    )
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 DISABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    assert(
      !spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      !ViewMetadata.isViewExists(table.quotedString)
    )
  }

  test("mv_alter_rewrite3") {
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW mv_create1 DISABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    val table = TableIdentifier("mv_create1", Some("default"))
    assert(
      !spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      !ViewMetadata.isViewExists(table.quotedString)
    )
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    assert(
      spark.sessionState.catalog.getTableMetadata(table)
          .properties(MV_REWRITE_ENABLED).toBoolean
    )
    assert(
      ViewMetadata.isViewExists(table.quotedString)
    )
  }

  test("mv_alter_rewrite_error1") {
    compareError("table or view not found") {
      spark.sql(
        """
          |ALTER MATERIALIZED VIEW mv_createxxx ENABLE REWRITE
          |;
          |""".stripMargin
      ).show()
    }
  }

  test("mv_is_cached") {
    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    val sql1 = "SELECT * FROM column_type"
    comparePlansAndRows(sql1, "default", "mv_create1", noData = false)

    spark.sql(
      """
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
    compareNotRewriteAndRows(sql1, noData = false)
  }

  test("explain_create") {
    spark.sql(
      """
        |EXPLAIN
        |CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_create1
        |  DISABLE REWRITE
        |  COMMENT 'mv_create1'
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT * FROM column_type
        |;
        |""".stripMargin
    ).show()
  }

  test("explain_drop") {
    spark.sql(
      """
        |EXPLAIN
        |DROP MATERIALIZED VIEW IF EXISTS default.mv_create1
        |;
        |""".stripMargin
    ).show()
  }

  test("explain_show") {
    spark.sql(
      """
        |EXPLAIN
        |SHOW MATERIALIZED VIEWS ON default.mv_create1
        |;
        |""".stripMargin
    ).show()
  }

  test("explain_alter_rewrite") {
    spark.sql(
      """
        |EXPLAIN
        |ALTER MATERIALIZED VIEW default.mv_create1 ENABLE REWRITE
        |;
        |""".stripMargin
    ).show()
  }

  test("refresh_partition_mv") {
    spark.sql("DROP TABLE IF EXISTS students;")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS students (age int) USING orc
        |PARTITIONED BY (id int);
        |""".stripMargin
    )
    spark.sql("INSERT INTO TABLE students partition(id=1) values(1);")

    spark.sql("DROP MATERIALIZED VIEW IF EXISTS refresh_mv;")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS refresh_mv PARTITIONED BY (id int)
        |AS
        |SELECT age,id FROM students;
        |""".stripMargin
    )
    spark.sql("INSERT INTO TABLE students partition(id=2) values(2);")
    spark.sql("REFRESH MATERIALIZED VIEW refresh_mv;")

    val df1 = "SELECT age,id FROM students"
    val df2 = "SELECT age,id FROM refresh_mv"
    val tableData2 = spark.sql(df2).collect().sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    RewriteHelper.disableCachePlugin()
    val tableData1 = spark.sql(df1).collect().sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    assert(tableData1.sameElements(tableData2))
  }

  test("refresh_nonpartition_mv") {
    spark.sql("DROP TABLE IF EXISTS students;")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS students (id int,age int) USING orc;
        |""".stripMargin
    )
    spark.sql("INSERT INTO TABLE students values(1,1);")

    spark.sql("DROP MATERIALIZED VIEW IF EXISTS refresh_mv;")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS refresh_mv
        |AS
        |SELECT age,id FROM students;
        |""".stripMargin
    )
    spark.sql("INSERT INTO TABLE students values(2,2);")
    spark.sql("REFRESH MATERIALIZED VIEW refresh_mv;")

    val df1 = "SELECT age,id FROM students"
    val df2 = "SELECT age,id FROM refresh_mv"
    val tableData2 = spark.sql(df2).collect().sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    RewriteHelper.disableCachePlugin()
    val tableData1 = spark.sql(df1).collect().sortWith { (a: Row, b: Row) =>
      a.toString().compareTo(b.toString()) < 0
    }
    assert(tableData1.sameElements(tableData2))
  }

  test("refresh_error1") {
    compareError("Table or view not found") {
      spark.sql("REFRESH MATERIALIZED VIEW mv")
    }
  }

  test("refresh_error2") {
    compareError("cannot refresh a table with refresh mv") {
      spark.sql("REFRESH MATERIALIZED VIEW emps")
    }
  }

  test("drop test mv") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_create1;")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_create2;")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_create_join1;")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_create_agg1;")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_create_agg2;")
  }
}
