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
import com.huawei.boostkit.spark.exception.OmniCacheException
import com.huawei.boostkit.spark.util.RewriteHelper.{disableCachePlugin, enableCachePlugin}
import com.huawei.boostkit.spark.util.ViewMetadata
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite
import org.apache.spark.sql.execution.command.WashOutStrategy

class WashOutMVSuite extends WashOutBase {

  test("view count accumulate") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS view_count;
        |""".stripMargin)
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS  view_count
        |  PARTITIONED BY (longtype,doubletype,datetype,stringtype)
        |AS
        |SELECT c1.*,e1.empname,d1.deptname FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |;
        |""".stripMargin
    )
    assert(ViewMetadata.viewCnt.get("default.view_count")(0) == 0)

    val sql1 =
      """
        |SELECT c1.*,e1.empname,d1.deptname FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql1, "default", "view_count", noData = false)
    assert(ViewMetadata.viewCnt.get("default.view_count")(0) == 1)

    val sql2 =
      """
        |SELECT c1.*,e1.empname,d1.deptname,e1.salary FROM
        |emps e1 JOIN column_type c1 JOIN depts d1
        |ON e1.empid=c1.empid
        |AND c1.deptno=d1.deptno
        |""".stripMargin
    RewriteSuite.compareNotRewriteAndRows(sql2, noData = false)
    assert(ViewMetadata.viewCnt.get("default.view_count")(0) == 1)

    RewriteSuite.comparePlansAndRows(sql1, "default", "view_count", noData = false)
    assert(ViewMetadata.viewCnt.get("default.view_count")(0) == 2)

    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS view_count;
        |""".stripMargin)
  }

  test("wash out mv by reserve quantity.") {
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.reserve.quantity.byViewCnt", "3")
    val reserveQuantity = OmniCachePluginConfig.getConf.reserveViewQuantityByViewCount
    spark.sql("WASH OUT ALL MATERIALIZED VIEW")
    val random = new Random()
    val viewsInfo = mutable.ArrayBuffer[(String, Array[Int])]()
    for (i <- 1 to 10) {
      val sql =
        f"""
           |SELECT * FROM COLUMN_TYPE WHERE empid=${i}0;
           |""".stripMargin
      // create mv
      spark.sql(
        f"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv$i
           |AS
           |$sql
           |""".stripMargin)
      val curTimes = random.nextInt(10)
      viewsInfo.append(
        (ViewMetadata.getDefaultDatabase + f".wash_mv$i", Array(curTimes, i)))
      // rewrite sql curTimes.
      for (_ <- 1 to curTimes) {
        RewriteSuite.comparePlansAndRows(sql, "default", s"wash_mv$i", noData = true)
      }
    }
    val toDel = viewsInfo.sorted {
      (x: (String, Array[Int]), y: (String, Array[Int])) => {
        if (y._2(0) != x._2(0)) {
          y._2(0).compare(x._2(0))
        } else {
          y._2(1).compare(x._2(1))
        }
      }
    }.slice(reserveQuantity, viewsInfo.size).map(_._1)
    spark.sql(f"WASH OUT MATERIALIZED VIEW USING " +
        f"${WashOutStrategy.RESERVE_QUANTITY_BY_VIEW_COUNT} $reserveQuantity")
    val data = mutable.Map[String, Array[Long]]()
    loadData(new Path(
      new Path(ViewMetadata.metadataPath,
        ViewMetadata.getDefaultDatabase),
      ViewMetadata.getViewCntPath), data)
    data.foreach {
      info =>
        assert(!toDel.contains(info._1))
    }
  }

  test("wash out mv by unused days.") {
    spark.sql("WASH OUT ALL MATERIALIZED VIEW")
    val unUsedDays = OmniCachePluginConfig.getConf.minimumUnusedDaysForWashOut
    for (i <- 1 to 5) {
      val sql =
        f"""
           |SELECT * FROM COLUMN_TYPE WHERE empid=${i}0;
           |""".stripMargin
      // create mv
      spark.sql(
        f"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv$i
           |AS
           |$sql
           |""".stripMargin)
    }
    var data = mutable.Map[String, Array[Long]]()
    val path = new Path(new Path(
      ViewMetadata.metadataPath, ViewMetadata.getDefaultDatabase), ViewMetadata.getViewCntPath)
    loadData(path, data)
    var cnt = 2
    val toDel = mutable.Set[String]()
    data.foreach {
      a =>
        if (cnt > 0) {
          // update mv used timestamp.
          data.update(a._1, Array(1, 0))
          cnt -= 1
          toDel += a._1
        }
    }
    saveData(path, data)
    ViewMetadata.forceLoad()
    spark.sql(f"WASH OUT MATERIALIZED VIEW USING " +
        f"${WashOutStrategy.UNUSED_DAYS} $unUsedDays")
    data = mutable.Map[String, Array[Long]]()
    loadData(path, data)
    data.foreach {
      info =>
        assert(!toDel.contains(info._1))
    }
  }

  test("wash out mv by space consumed.") {
    spark.sql("WASH OUT ALL MATERIALIZED VIEW")
    val dropQuantity = 2
    for (i <- 1 to 10) {
      val sql =
        f"""
           |SELECT * FROM COLUMN_TYPE WHERE empid=$i;
           |""".stripMargin
      // create mv
      spark.sql(
        f"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv$i
           |AS
           |$sql
           |""".stripMargin)
    }
    spark.sql("WASH OUT MATERIALIZED VIEW USING " +
        f"${WashOutStrategy.DROP_QUANTITY_BY_SPACE_CONSUMED} $dropQuantity")
    val data = mutable.Map[String, Array[Long]]()
    val path = new Path(new Path(
      ViewMetadata.metadataPath, ViewMetadata.getDefaultDatabase), ViewMetadata.getViewCntPath)
    loadData(path, data)
    val dropList = List(1, 4)
    dropList.foreach {
      a =>
        assert(!data.contains(f"${ViewMetadata.getDefaultDatabase}.wash_mv$a"))
    }
  }

  test("wash out all mv") {
    spark.sql("WASH OUT ALL MATERIALIZED VIEW")
    for (i <- 1 to 5) {
      val sql =
        f"""
           |SELECT * FROM COLUMN_TYPE WHERE empid=${i}0;
           |""".stripMargin
      // create mv
      spark.sql(
        f"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv$i
           |AS
           |$sql
           |""".stripMargin)
    }
    var data = mutable.Map[String, Array[Long]]()
    loadData(new Path(
      new Path(ViewMetadata.metadataPath,
        ViewMetadata.getDefaultDatabase),
      ViewMetadata.getViewCntPath), data)
    assert(data.size == 5)
    spark.sql("WASH OUT ALL MATERIALIZED VIEW")
    data = mutable.Map[String, Array[Long]]()
    loadData(new Path(
      new Path(ViewMetadata.metadataPath,
        ViewMetadata.getDefaultDatabase),
      ViewMetadata.getViewCntPath), data)
    assert(data.isEmpty)
  }

  test("auto wash out") {
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.unused.day", "0")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.reserve.quantity.byViewCnt", "1")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.drop.quantity.bySpaceConsumed", "1")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.automatic.time.interval", "0")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.automatic.view.quantity", "1")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.automatic.enable", "true")
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.automatic.checkTime.interval", "0")
    spark.sql(
      f"""
         |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv1
         |AS
         |SELECT * FROM COLUMN_TYPE WHERE empid=100;
         |""".stripMargin)
    spark.sql(
      f"""
         |CREATE MATERIALIZED VIEW IF NOT EXISTS wash_mv2
         |AS
         |SELECT * FROM COLUMN_TYPE WHERE empid=200;
         |""".stripMargin)
    val sql =
      """
        |SELECT * FROM COLUMN_TYPE WHERE empid=100;
        |""".stripMargin
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(RewriteSuite.isNotRewritedByMV(plan))
    spark.sessionState.conf.setConfString(
      "spark.sql.omnicache.washout.automatic.enable", "false")
  }
}

class WashOutBase extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  System.setProperty("HADOOP_USER_NAME", "root")
  lazy val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.sql.extensions", "com.huawei.boostkit.spark.OmniCache")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.ui.port", "4050")
      // .config("spark.sql.planChangeLog.level", "WARN")
      .config("spark.sql.omnicache.logLevel", "WARN")
      .config("spark.sql.omnicache.dbs", "default")
      .config("spark.sql.omnicache.metadata.initbyquery.enable", "false")
      .config("hive.in.test", "true")
      .config("spark.sql.omnicache.metadata.path", "./user/omnicache/metadata")
      .config("spark.sql.omnicache.washout.automatic.enable", "false")
      .enableHiveSupport()
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  lazy val catalog: SessionCatalog = spark.sessionState.catalog

  override def beforeEach(): Unit = {
    enableCachePlugin()
  }

  override def beforeAll(): Unit = {
    preCreateTable()
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
    disableCachePlugin()
    preDropTable()
    if (catalog.tableExists(TableIdentifier("locations"))) {
      enableCachePlugin()
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
        |INSERT INTO TABLE depts VALUES(3,'deptname3');
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE depts VALUES(4,'deptname4');
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
        |INSERT INTO TABLE emps VALUES(3,null,3,'empname3',3.0);
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
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   1,1,1,null,null,null,null,null,null,null,
        |   null,
        |   null,
        |   null,null
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   3,3,3,TRUE,3,3,3,3,3.0,3.0,
        |   DATE '2022-03-03',
        |   TIMESTAMP '2022-03-03',
        |   'stringtype3',null
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   4,4,4,TRUE,4,4,4,4,4.0,4.0,
        |   DATE '2022-04-04',
        |   TIMESTAMP '2022-04-04',
        |   null,4.0
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   4,4,4,TRUE,4,4,4,4,4.0,4.0,
        |   DATE '2022-04-04',
        |   null,
        |   null,4.0
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   4,4,4,TRUE,4,4,4,4,4.0,4.0,
        |   DATE '2022-04-04',
        |   TIMESTAMP '2022-04-04',
        |   'stringtype4',null
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   1,1,2,TRUE,1,1,1,1,1.0,1.0,
        |   DATE '2022-01-01',
        |   TIMESTAMP '2022-01-01',
        |   'stringtype1',1.0
        |);
        |""".stripMargin
    )
    spark.sql(
      """
        |INSERT INTO TABLE column_type VALUES(
        |   1,1,2,TRUE,1,1,1,1,1.0,1.0,
        |   DATE '2022-01-02',
        |   TIMESTAMP '2022-01-01',
        |   'stringtype1',1.0
        |);
        |""".stripMargin
    )
    enableCachePlugin()
  }

  def loadData[K: Manifest, V: Manifest](file: Path,
      buffer: mutable.Map[K, V]): Unit = {
    try {
      val fs = file.getFileSystem(new Configuration)
      val is = fs.open(file)
      val content = IOUtils.readFullyToByteArray(is)
          .map(_.toChar.toString).reduce((a, b) => a + b)
      Json(DefaultFormats).read[mutable.Map[K, V]](content).foreach {
        data =>
          buffer += data
      }
      is.close()
    } catch {
      case _: Throwable =>
        throw OmniCacheException("load data failed.")
    }
  }

  def saveData[K: Manifest, V: Manifest](file: Path,
      buffer: mutable.Map[K, V]): Unit = {
    try {
      val fs = file.getFileSystem(new Configuration)
      val os = fs.create(file, true)
      val bytes = Json(DefaultFormats).write(buffer).getBytes
      os.write(bytes)
      os.close()
    } catch {
      case _: Throwable =>
        throw OmniCacheException("save data failed.")
    }
  }
}
