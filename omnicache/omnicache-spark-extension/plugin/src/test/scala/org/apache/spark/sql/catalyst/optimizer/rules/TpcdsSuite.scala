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

package org.apache.spark.sql.catalyst.optimizer.rules

import org.apache.commons.io.IOUtils
import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier

class TpcdsSuite extends RewriteSuite {

  def createTable(): Unit = {
    if (catalog.tableExists(TableIdentifier("store_sales"))) {
      return
    }
    val fis = this.getClass.getResourceAsStream("/tpcds_ddl.sql")
    val lines = IOUtils.readLines(fis, "UTF-8")
    IOUtils.closeQuietly(fis)

    var sqls = Seq.empty[String]
    val sql = mutable.StringBuilder.newBuilder
    lines.forEach { line =>
      sql.append(line)
      sql.append(" ")
      if (line.contains(';')) {
        sqls +:= sql.toString()
        sql.clear()
      }
    }
    sqls.foreach { sql =>
      spark.sql(sql)
    }
  }

  createTable()

  test("subQuery outReference") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv536")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv536 PARTITIONED BY (ws_sold_date_sk) AS
        | SELECT
        | web_sales.ws_ext_discount_amt,
        | item.i_item_sk,
        | web_sales.ws_sold_date_sk,
        | web_sales.ws_item_sk,
        | item.i_manufact_id
        |FROM
        | web_sales,
        | item
        |WHERE
        | item.i_manufact_id = 350
        | AND web_sales.ws_item_sk = item.i_item_sk
        |distribute by ws_sold_date_sk;
        |""".stripMargin
    )
    val sql =
      """
        |SELECT sum(ws_ext_discount_amt) AS `Excess Discount Amount `
        |FROM web_sales, item, date_dim
        |WHERE i_manufact_id = 350
        |  AND i_item_sk = ws_item_sk
        |  AND d_date BETWEEN '2000-01-27' AND (cast('2000-01-27' AS DATE) + INTERVAL 90 days)
        |  AND d_date_sk = ws_sold_date_sk
        |  AND ws_ext_discount_amt >
        |  (
        |    SELECT 1.3 * avg(ws_ext_discount_amt)
        |    FROM web_sales, date_dim
        |    WHERE ws_item_sk = i_item_sk
        |      AND d_date BETWEEN '2000-01-27' AND (cast('2000-01-27' AS DATE) + INTERVAL 90 days)
        |      AND d_date_sk = ws_sold_date_sk
        |  )
        |ORDER BY sum(ws_ext_discount_amt)
        |LIMIT 100
        |
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv536")
  }

  test("sum decimal") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_q11")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_q11 AS
        |  SELECT
        |    c_customer_id customer_id,
        |    c_first_name customer_first_name,
        |    c_last_name customer_last_name,
        |    c_preferred_cust_flag customer_preferred_cust_flag,
        |    c_birth_country customer_birth_country,
        |    c_login customer_login,
        |    c_email_address customer_email_address,
        |    d_year dyear,
        |    sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
        |    's' sale_type
        |  FROM customer, store_sales, date_dim
        |  WHERE c_customer_sk = ss_customer_sk
        |    AND ss_sold_date_sk = d_date_sk
        |  GROUP BY c_customer_id
        |    , c_first_name
        |    , c_last_name
        |    , d_year
        |    , c_preferred_cust_flag
        |    , c_birth_country
        |    , c_login
        |    , c_email_address
        |    , d_year
        |    , c_customer_sk
        |""".stripMargin
    )
    val sql =
      """
        |WITH year_total AS (
        |  SELECT
        |    c_customer_id customer_id,
        |    c_first_name customer_first_name,
        |    c_last_name customer_last_name,
        |    c_preferred_cust_flag customer_preferred_cust_flag,
        |    c_birth_country customer_birth_country,
        |    c_login customer_login,
        |    c_email_address customer_email_address,
        |    d_year dyear,
        |    sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
        |    's' sale_type
        |  FROM customer, store_sales, date_dim
        |  WHERE c_customer_sk = ss_customer_sk
        |    AND ss_sold_date_sk = d_date_sk
        |  GROUP BY c_customer_id
        |    , c_first_name
        |    , c_last_name
        |    , d_year
        |    , c_preferred_cust_flag
        |    , c_birth_country
        |    , c_login
        |    , c_email_address
        |    , d_year
        |  UNION ALL
        |  SELECT
        |    c_customer_id customer_id,
        |    c_first_name customer_first_name,
        |    c_last_name customer_last_name,
        |    c_preferred_cust_flag customer_preferred_cust_flag,
        |    c_birth_country customer_birth_country,
        |    c_login customer_login,
        |    c_email_address customer_email_address,
        |    d_year dyear,
        |    sum(ws_ext_list_price - ws_ext_discount_amt) year_total,
        |    'w' sale_type
        |  FROM customer, web_sales, date_dim
        |  WHERE c_customer_sk = ws_bill_customer_sk
        |    AND ws_sold_date_sk = d_date_sk
        |  GROUP BY
        |    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country,
        |    c_login, c_email_address, d_year)
        |SELECT t_s_secyear.customer_preferred_cust_flag
        |FROM year_total t_s_firstyear
        |  , year_total t_s_secyear
        |  , year_total t_w_firstyear
        |  , year_total t_w_secyear
        |WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
        |  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
        |  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
        |  AND t_s_firstyear.sale_type = 's'
        |  AND t_w_firstyear.sale_type = 'w'
        |  AND t_s_secyear.sale_type = 's'
        |  AND t_w_secyear.sale_type = 'w'
        |  AND t_s_firstyear.dyear = 2001
        |  AND t_s_secyear.dyear = 2001 + 1
        |  AND t_w_firstyear.dyear = 2001
        |  AND t_w_secyear.dyear = 2001 + 1
        |  AND t_s_firstyear.year_total > 0
        |  AND t_w_firstyear.year_total > 0
        |  AND CASE WHEN t_w_firstyear.year_total > 0
        |  THEN t_w_secyear.year_total / t_w_firstyear.year_total
        |      ELSE NULL END
        |  > CASE WHEN t_s_firstyear.year_total > 0
        |  THEN t_s_secyear.year_total / t_s_firstyear.year_total
        |    ELSE NULL END
        |ORDER BY t_s_secyear.customer_preferred_cust_flag
        |LIMIT 100
        |
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_q11", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_q11")
  }
}
