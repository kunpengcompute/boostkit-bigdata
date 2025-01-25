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

import com.huawei.boostkit.spark.util.ViewMetadata
import java.util
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import scala.collection.mutable
import scala.io.Source

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite._

class TpcdsSuite extends RewriteSuite {

  def createTable(): Unit = {
    if (RewriteSuite.catalog.tableExists(TableIdentifier("store_sales"))) {
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
    RewriteSuite.comparePlansAndRows(sql, "default", "mv536", noData = true)
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
    RewriteSuite.comparePlansAndRows(sql, "default", "mv_q11", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_q11")
  }
  test("resort") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv103")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv103
        |PARTITIONED BY (ss_sold_date_sk)
        |AS
        |SELECT
        | item.i_item_id,
        | store_sales.ss_ext_discount_amt,
        | store_sales.ss_quantity,
        | item.i_item_desc,
        | item.i_product_name,
        | item.i_manufact_id,
        | store_sales.ss_sold_date_sk,
        | item.i_brand_id,
        | item.i_item_sk,
        | date_dim.d_moy,
        | item.i_category,
        | store_sales.ss_item_sk,
        | item.i_brand,
        | date_dim.d_date,
        | date_dim.d_month_seq,
        | item.i_wholesale_cost,
        | date_dim.d_dom,
        | store_sales.ss_net_paid,
        | store_sales.ss_addr_sk,
        | item.i_color,
        | store_sales.ss_store_sk,
        | store_sales.ss_cdemo_sk,
        | store_sales.ss_list_price,
        | store_sales.ss_wholesale_cost,
        | store_sales.ss_ticket_number,
        | date_dim.d_year,
        | store_sales.ss_hdemo_sk,
        | store_sales.ss_customer_sk,
        | item.i_manufact,
        | store_sales.ss_sales_price,
        | item.i_current_price,
        | item.i_class,
        | store_sales.ss_ext_list_price,
        | date_dim.d_quarter_name,
        | item.i_units,
        | item.i_manager_id,
        | date_dim.d_day_name,
        | store_sales.ss_coupon_amt,
        | item.i_category_id,
        | store_sales.ss_promo_sk,
        | store_sales.ss_net_profit,
        | date_dim.d_qoy,
        | date_dim.d_week_seq,
        | store_sales.ss_ext_sales_price,
        | item.i_size,
        | store_sales.ss_sold_time_sk,
        | item.i_class_id,
        | date_dim.d_dow,
        | store_sales.ss_ext_wholesale_cost,
        | store_sales.ss_ext_tax,
        | date_dim.d_date_sk
        |FROM
        | date_dim,
        | item,
        | store_sales
        |WHERE
        | store_sales.ss_item_sk = item.i_item_sk
        | AND date_dim.d_date_sk = store_sales.ss_sold_date_sk
        | AND (item.i_manager_id = 8 OR item.i_manager_id = 1 OR item.i_manager_id = 28)
        | AND (date_dim.d_year = 1998 OR date_dim.d_year = 2000 OR date_dim.d_year = 1999)
        | AND date_dim.d_moy = 11
        |DISTRIBUTE BY ss_sold_date_sk;
        |""".stripMargin
    )
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv9")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv9
        |AS
        |SELECT
        | date_dim.d_year,
        | item.i_category,
        | item.i_item_id,
        | item.i_class,
        | item.i_current_price,
        | item.i_item_desc,
        | item.i_brand,
        | date_dim.d_date,
        | item.i_manufact_id,
        | item.i_manager_id,
        | item.i_brand_id,
        | item.i_category_id,
        | date_dim.d_moy,
        | item.i_item_sk,
        | sum(store_sales.ss_ext_sales_price) AS AGG0,
        | count(1) AS AGG1
        |FROM
        | date_dim,
        | item,
        | store_sales
        |WHERE
        | store_sales.ss_item_sk = item.i_item_sk
        | AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |GROUP BY
        | date_dim.d_year,
        | item.i_category,
        | item.i_item_id,
        | item.i_class,
        | item.i_current_price,
        | item.i_item_desc,
        | item.i_brand,
        | date_dim.d_date,
        | item.i_manufact_id,
        | item.i_manager_id,
        | item.i_brand_id,
        | item.i_category_id,
        | date_dim.d_moy,
        | item.i_item_sk;
        |""".stripMargin
    )
    val os = ViewMetadata.fs.create(new Path(ViewMetadata.metadataPriorityPath, "mv103_9"))
    val list = new util.ArrayList[String]()
    list.add("default.mv9,default.mv103")
    IOUtils.writeLines(list, "\n", os)
    os.close()
    ViewMetadata.loadViewPriorityFromFile()
    val sql =
      """
        |SELECT
        |  dt.d_year,
        |  item.i_category_id,
        |  item.i_category,
        |  sum(ss_ext_sales_price)
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |  AND store_sales.ss_item_sk = item.i_item_sk
        |  AND item.i_manager_id = 1
        |  AND dt.d_moy = 11
        |  AND dt.d_year = 2000
        |GROUP BY dt.d_year
        |  , item.i_category_id
        |  , item.i_category
        |ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year
        |  , item.i_category_id
        |  , item.i_category
        |LIMIT 100
        |
        |""".stripMargin
    spark.sql(sql).explain()
    RewriteSuite.comparePlansAndRows(sql, "default", "mv9", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv103")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv9")
  }

  test("resort2") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv103")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv103
        |PARTITIONED BY (ss_sold_date_sk)
        |AS
        |SELECT
        | item.i_item_id,
        | store_sales.ss_ext_discount_amt,
        | store_sales.ss_quantity,
        | item.i_item_desc,
        | item.i_product_name,
        | item.i_manufact_id,
        | store_sales.ss_sold_date_sk,
        | item.i_brand_id,
        | item.i_item_sk,
        | date_dim.d_moy,
        | item.i_category,
        | store_sales.ss_item_sk,
        | item.i_brand,
        | date_dim.d_date,
        | date_dim.d_month_seq,
        | item.i_wholesale_cost,
        | date_dim.d_dom,
        | store_sales.ss_net_paid,
        | store_sales.ss_addr_sk,
        | item.i_color,
        | store_sales.ss_store_sk,
        | store_sales.ss_cdemo_sk,
        | store_sales.ss_list_price,
        | store_sales.ss_wholesale_cost,
        | store_sales.ss_ticket_number,
        | date_dim.d_year,
        | store_sales.ss_hdemo_sk,
        | store_sales.ss_customer_sk,
        | item.i_manufact,
        | store_sales.ss_sales_price,
        | item.i_current_price,
        | item.i_class,
        | store_sales.ss_ext_list_price,
        | date_dim.d_quarter_name,
        | item.i_units,
        | item.i_manager_id,
        | date_dim.d_day_name,
        | store_sales.ss_coupon_amt,
        | item.i_category_id,
        | store_sales.ss_promo_sk,
        | store_sales.ss_net_profit,
        | date_dim.d_qoy,
        | date_dim.d_week_seq,
        | store_sales.ss_ext_sales_price,
        | item.i_size,
        | store_sales.ss_sold_time_sk,
        | item.i_class_id,
        | date_dim.d_dow,
        | store_sales.ss_ext_wholesale_cost,
        | store_sales.ss_ext_tax,
        | date_dim.d_date_sk
        |FROM
        | date_dim,
        | item,
        | store_sales
        |WHERE
        | store_sales.ss_item_sk = item.i_item_sk
        | AND date_dim.d_date_sk = store_sales.ss_sold_date_sk
        | AND (item.i_manager_id = 8 OR item.i_manager_id = 1 OR item.i_manager_id = 28)
        | AND (date_dim.d_year = 1998 OR date_dim.d_year = 2000 OR date_dim.d_year = 1999)
        | AND date_dim.d_moy = 11
        |DISTRIBUTE BY ss_sold_date_sk;
        |""".stripMargin
    )
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv9")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv9
        |AS
        |SELECT
        | date_dim.d_year,
        | item.i_category,
        | item.i_item_id,
        | item.i_class,
        | item.i_current_price,
        | item.i_item_desc,
        | item.i_brand,
        | date_dim.d_date,
        | item.i_manufact_id,
        | item.i_manager_id,
        | item.i_brand_id,
        | item.i_category_id,
        | date_dim.d_moy,
        | item.i_item_sk,
        | sum(store_sales.ss_ext_sales_price) AS AGG0,
        | count(1) AS AGG1
        |FROM
        | date_dim,
        | item,
        | store_sales
        |WHERE
        | store_sales.ss_item_sk = item.i_item_sk
        | AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |GROUP BY
        | date_dim.d_year,
        | item.i_category,
        | item.i_item_id,
        | item.i_class,
        | item.i_current_price,
        | item.i_item_desc,
        | item.i_brand,
        | date_dim.d_date,
        | item.i_manufact_id,
        | item.i_manager_id,
        | item.i_brand_id,
        | item.i_category_id,
        | date_dim.d_moy,
        | item.i_item_sk;
        |""".stripMargin
    )
    val os = ViewMetadata.fs.create(new Path(ViewMetadata.metadataPriorityPath, "mv103_9"))
    val list = new util.ArrayList[String]()
    list.add("default.mv103,default.mv9")
    IOUtils.writeLines(list, "\n", os)
    os.close()
    ViewMetadata.loadViewPriorityFromFile()
    val sql =
      """
        |SELECT
        |  dt.d_year,
        |  item.i_category_id,
        |  item.i_category,
        |  sum(ss_ext_sales_price)
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |  AND store_sales.ss_item_sk = item.i_item_sk
        |  AND item.i_manager_id = 1
        |  AND dt.d_moy = 11
        |  AND dt.d_year = 2000
        |GROUP BY dt.d_year
        |  , item.i_category_id
        |  , item.i_category
        |ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year
        |  , item.i_category_id
        |  , item.i_category
        |LIMIT 100
        |
        |""".stripMargin
    spark.sql(sql).explain()
    RewriteSuite.comparePlansAndRows(sql, "default", "mv103", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv103")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv9")
  }

  test("subQuery condition 01") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS sc01")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS sc01 AS
        |SELECT *
        |FROM catalog_sales t1
        | LEFT JOIN (select * from inventory where inv_item_sk > 100 or
        | inv_date_sk < 40) t2 ON (cs_item_sk = t2.inv_item_sk)
        | LEFT JOIN warehouse t3 ON (t3.w_warehouse_sk = t2.inv_warehouse_sk)
        | Join item t4 ON (t4.i_item_sk = t1.cs_item_sk)
        |WHERE t2.inv_quantity_on_hand < t1.cs_quantity;
        |""".stripMargin
    )
    val sql =
      """
        |SELECT
        | i_item_desc,
        | w_warehouse_name,
        | count(CASE WHEN p_promo_sk IS NULL
        |   THEN 1
        |     ELSE 0 END) promo,
        | count(*) total_cnt
        |FROM catalog_sales t1
        | LEFT JOIN (select * from inventory where inv_item_sk > 100) t2
        | ON (cs_item_sk = t2.inv_item_sk)
        | LEFT JOIN warehouse t3 ON (t3.w_warehouse_sk = t2.inv_warehouse_sk)
        | Join item t4 ON (t4.i_item_sk = t1.cs_item_sk)
        | LEFT JOIN promotion ON (cs_item_sk = p_promo_sk)
        |WHERE t2.inv_quantity_on_hand < t1.cs_quantity
        |GROUP BY i_item_desc, w_warehouse_name;
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql, "default", "sc01", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS sc01")
  }

  test("mv22") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv22")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv22 AS
        |SELECT table_5.c_last_name column0 , table_5.c_first_name column1 , table_3.s_store_name column2 , table_6.ca_state column3 , table_3.s_state column4 , table_4.i_color column5 , table_4.i_current_price column6 , table_4.i_manager_id column7 , table_4.i_units column8 , table_4.i_size column9 , sum ( table_1.ss_net_paid ) column10
        | FROM store_sales table_1  , store_returns table_2  , store table_3  , item table_4  , customer table_5  , customer_address table_6
        | WHERE table_2.sr_ticket_number = table_1.ss_ticket_number AND table_2.sr_item_sk = table_1.ss_item_sk AND table_1.ss_customer_sk = table_5.c_customer_sk AND table_1.ss_item_sk = table_4.i_item_sk AND table_1.ss_store_sk = table_3.s_store_sk AND upper ( table_6.ca_country ) = table_5.c_birth_country AND table_6.ca_zip = table_3.s_zip AND ( table_3.s_market_id = 8 )
        | GROUP BY table_5.c_last_name , table_5.c_first_name , table_3.s_store_name , table_6.ca_state , table_3.s_state , table_4.i_color , table_4.i_current_price , table_4.i_manager_id , table_4.i_units , table_4.i_size
        |;
        |""".stripMargin
    )
    val sql =
      """
        |WITH ssales AS
        |(SELECT
        |    c_last_name,
        |    c_first_name,
        |    s_store_name,
        |    ca_state,
        |    s_state,
        |    i_color,
        |    i_current_price,
        |    i_manager_id,
        |    i_units,
        |    i_size,
        |    sum(ss_net_paid) netpaid
        |  FROM store_sales, store_returns, store, item, customer, customer_address
        |  WHERE ss_ticket_number = sr_ticket_number
        |    AND ss_item_sk = sr_item_sk
        |    AND ss_customer_sk = c_customer_sk
        |    AND ss_item_sk = i_item_sk
        |    AND ss_store_sk = s_store_sk
        |    AND c_birth_country = upper(ca_country)
        |    AND s_zip = ca_zip
        |    AND s_market_id = 8
        |  GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color,
        |    i_current_price, i_manager_id, i_units, i_size)
        |SELECT
        |  c_last_name,
        |  c_first_name,
        |  s_store_name,
        |  sum(netpaid) paid
        |FROM ssales
        |WHERE i_color = 'pale'
        |GROUP BY c_last_name, c_first_name, s_store_name
        |HAVING sum(netpaid) > (SELECT 0.05 * avg(netpaid)
        |FROM ssales)
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql, "default", "mv22", noData = true)
  }


  test("mv29") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv29")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv29 AS
        |SELECT table_4.i_manufact_id column0 , sum ( table_1.ss_ext_sales_price ) column1 , table_4.i_item_id column2 , table_2.d_year column3 , table_2.d_moy column4 , table_3.ca_gmt_offset column5
        | FROM store_sales table_1  , date_dim table_2  , customer_address table_3  , item table_4
        | WHERE table_1.ss_item_sk = table_4.i_item_sk AND table_1.ss_sold_date_sk = table_2.d_date_sk AND table_3.ca_address_sk = table_1.ss_addr_sk AND ( ( ( table_2.d_year = 1998 AND table_2.d_moy = 5 AND table_3.ca_gmt_offset = -5 ) OR ( table_2.d_year = 2001 AND table_2.d_moy = 2 AND table_3.ca_gmt_offset = -5 ) ) OR ( table_2.d_year = 1998 AND table_2.d_moy = 9 AND table_3.ca_gmt_offset = -5 ) )
        | GROUP BY table_4.i_manufact_id , table_4.i_item_id , table_2.d_year , table_2.d_moy , table_3.ca_gmt_offset
        |;
        |""".stripMargin
    )
    val sql56 =
      """
        |WITH ss AS (
        |  SELECT
        |    i_item_id,
        |    sum(ss_ext_sales_price) total_sales
        |  FROM
        |    store_sales, date_dim, customer_address, item
        |  WHERE
        |    i_item_id IN (SELECT i_item_id
        |    FROM item
        |    WHERE i_color IN ('slate', 'blanched', 'burnished'))
        |      AND ss_item_sk = i_item_sk
        |      AND ss_sold_date_sk = d_date_sk
        |      AND d_year = 2001
        |      AND d_moy = 2
        |      AND ss_addr_sk = ca_address_sk
        |      AND ca_gmt_offset = -5
        |  GROUP BY i_item_id),
        |    cs AS (
        |    SELECT
        |      i_item_id,
        |      sum(cs_ext_sales_price) total_sales
        |    FROM
        |      catalog_sales, date_dim, customer_address, item
        |    WHERE
        |      i_item_id IN (SELECT i_item_id
        |      FROM item
        |      WHERE i_color IN ('slate', 'blanched', 'burnished'))
        |        AND cs_item_sk = i_item_sk
        |        AND cs_sold_date_sk = d_date_sk
        |        AND d_year = 2001
        |        AND d_moy = 2
        |        AND cs_bill_addr_sk = ca_address_sk
        |        AND ca_gmt_offset = -5
        |    GROUP BY i_item_id),
        |    ws AS (
        |    SELECT
        |      i_item_id,
        |      sum(ws_ext_sales_price) total_sales
        |    FROM
        |      web_sales, date_dim, customer_address, item
        |    WHERE
        |      i_item_id IN (SELECT i_item_id
        |      FROM item
        |      WHERE i_color IN ('slate', 'blanched', 'burnished'))
        |        AND ws_item_sk = i_item_sk
        |        AND ws_sold_date_sk = d_date_sk
        |        AND d_year = 2001
        |        AND d_moy = 2
        |        AND ws_bill_addr_sk = ca_address_sk
        |        AND ca_gmt_offset = -5
        |    GROUP BY i_item_id)
        |SELECT
        |  i_item_id,
        |  sum(total_sales) total_sales
        |FROM (SELECT *
        |      FROM ss
        |      UNION ALL
        |      SELECT *
        |      FROM cs
        |      UNION ALL
        |      SELECT *
        |      FROM ws) tmp1
        |GROUP BY i_item_id
        |ORDER BY total_sales
        |LIMIT 100
        |
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql56, "default", "mv29", noData = true)
  }

  test("mv37") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv37;")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv37 AS
        |SELECT table_3.ca_zip column0 , table_3.ca_city column1 , sum ( table_1.ws_sales_price ) column2 , table_3.ca_zip column3 , table_5.i_item_id column4
        | FROM web_sales table_1  , customer table_2  , customer_address table_3  , date_dim table_4  , item table_5
        | WHERE table_1.ws_bill_customer_sk = table_2.c_customer_sk AND table_2.c_current_addr_sk = table_3.ca_address_sk AND table_1.ws_item_sk = table_5.i_item_sk AND table_1.ws_sold_date_sk = table_4.d_date_sk AND ( table_4.d_qoy = 2 AND table_4.d_year = 2001 )
        | GROUP BY table_3.ca_zip , table_3.ca_city , table_5.i_item_id
        |""".stripMargin
    )
    val sql45 =
      """
        |SELECT
        |  ca_zip,
        |  ca_city,
        |  sum(ws_sales_price)
        |FROM web_sales, customer, customer_address, date_dim, item
        |WHERE ws_bill_customer_sk = c_customer_sk
        |  AND c_current_addr_sk = ca_address_sk
        |  AND ws_item_sk = i_item_sk
        |  AND (substr(ca_zip, 1, 5) IN
        |  ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
        |  OR
        |  i_item_id IN (SELECT i_item_id
        |  FROM item
        |  WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
        |  )
        |)
        |  AND ws_sold_date_sk = d_date_sk
        |  AND d_qoy = 2 AND d_year = 2001
        |GROUP BY ca_zip, ca_city
        |ORDER BY ca_zip, ca_city
        |LIMIT 100
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql45, "default", "mv37", noData = true)
  }

  test("q93") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv81;")
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv81 AS
        |SELECT table_1.ss_item_sk column0 , table_1.ss_ticket_number column1 , table_1.ss_customer_sk column2 , CASE WHEN table_2.sr_return_quantity IS NOT NULL THEN ( table_1.ss_quantity - table_2.sr_return_quantity ) * table_1.ss_sales_price ELSE ( table_1.ss_quantity * table_1.ss_sales_price ) END column3
        | FROM store_sales table_1  LEFT OUTER JOIN  store_returns  table_2 ON  table_2.sr_item_sk = table_1.ss_item_sk AND table_2.sr_ticket_number = table_1.ss_ticket_number  , reason table_3
        | WHERE table_2.sr_reason_sk = table_3.r_reason_sk AND ( table_3.r_reason_desc = 'reason 28' )
        |;
        |
        |""".stripMargin
    )
    val sql93 =
      """
        |SELECT
        |  ss_customer_sk,
        |  sum(act_sales) sumsales
        |FROM (SELECT
        |  ss_item_sk,
        |  ss_ticket_number,
        |  ss_customer_sk,
        |  CASE WHEN sr_return_quantity IS NOT NULL
        |    THEN (ss_quantity - sr_return_quantity) * ss_sales_price
        |  ELSE (ss_quantity * ss_sales_price) END act_sales
        |FROM store_sales
        |  LEFT OUTER JOIN store_returns
        |    ON (sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number)
        |  ,
        |  reason
        |WHERE sr_reason_sk = r_reason_sk AND r_reason_desc = 'reason 28') t
        |GROUP BY ss_customer_sk
        |ORDER BY sumsales, ss_customer_sk
        |LIMIT 100
        |
        |""".stripMargin
    spark.sql(sql93).explain(true)
    // RewriteSuite.comparePlansAndRows(sql93, "default", "mv81", noData = true)
  }


  test("q80") {
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv68")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv69")
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv70")

    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv68 AS
        |SELECT table_4.s_store_id column0 , sum ( table_1.ss_ext_sales_price ) column1 , sum ( coalesce ( table_2.sr_return_amt , 0 ) ) column2 , sum ( table_1.ss_net_profit - coalesce ( table_2.sr_net_loss , 0 ) ) column3
        | FROM store_sales table_1  LEFT OUTER JOIN  store_returns  table_2 ON  table_2.sr_item_sk = table_1.ss_item_sk AND table_2.sr_ticket_number = table_1.ss_ticket_number  , date_dim table_3  , store table_4  , item table_5  , promotion table_6
        | WHERE table_1.ss_sold_date_sk = table_3.d_date_sk AND table_1.ss_store_sk = table_4.s_store_sk AND table_1.ss_item_sk = table_5.i_item_sk AND table_1.ss_promo_sk = table_6.p_promo_sk AND ( table_3.d_date BETWEEN cast ( '2000-08-23' AS DATE ) AND ( cast ( '2000-08-23' AS DATE ) + INTERVAL 30 days ) AND table_5.i_current_price > 50 AND table_6.p_channel_tv = 'N' )
        | GROUP BY table_4.s_store_id
        |;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv69 AS
        |SELECT table_4.cp_catalog_page_id column0 , sum ( table_1.cs_ext_sales_price ) column1 , sum ( coalesce ( table_2.cr_return_amount , 0 ) ) column2 , sum ( table_1.cs_net_profit - coalesce ( table_2.cr_net_loss , 0 ) ) column3
        | FROM catalog_sales table_1  LEFT OUTER JOIN  catalog_returns  table_2 ON  table_2.cr_item_sk = table_1.cs_item_sk AND table_2.cr_order_number = table_1.cs_order_number  , date_dim table_3  , catalog_page table_4  , item table_5  , promotion table_6
        | WHERE table_1.cs_sold_date_sk = table_3.d_date_sk AND table_4.cp_catalog_page_sk = table_1.cs_catalog_page_sk AND table_1.cs_item_sk = table_5.i_item_sk AND table_1.cs_promo_sk = table_6.p_promo_sk AND ( table_3.d_date BETWEEN cast ( '2000-08-23' AS DATE ) AND ( cast ( '2000-08-23' AS DATE ) + INTERVAL 30 days ) AND table_5.i_current_price > 50 AND table_6.p_channel_tv = 'N' )
        | GROUP BY table_4.cp_catalog_page_id
        |;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv70 AS
        |SELECT table_4.web_site_id column0 , sum ( table_1.ws_ext_sales_price ) column1 , sum ( coalesce ( table_2.wr_return_amt , 0 ) ) column2 , sum ( table_1.ws_net_profit - coalesce ( table_2.wr_net_loss , 0 ) ) column3
        | FROM web_sales table_1  LEFT OUTER JOIN  web_returns  table_2 ON  table_2.wr_item_sk = table_1.ws_item_sk AND table_2.wr_order_number = table_1.ws_order_number  , date_dim table_3  , web_site table_4  , item table_5  , promotion table_6
        | WHERE table_1.ws_sold_date_sk = table_3.d_date_sk AND table_1.ws_web_site_sk = table_4.web_site_sk AND table_1.ws_item_sk = table_5.i_item_sk AND table_1.ws_promo_sk = table_6.p_promo_sk AND ( table_3.d_date BETWEEN cast ( '2000-08-23' AS DATE ) AND ( cast ( '2000-08-23' AS DATE ) + INTERVAL 30 days ) AND table_5.i_current_price > 50 AND table_6.p_channel_tv = 'N' )
        | GROUP BY table_4.web_site_id
        |;
        |""".stripMargin
    )
    val sql80 =
      """
        |WITH ssr AS
        |(SELECT
        |    s_store_id AS store_id,
        |    sum(ss_ext_sales_price) AS sales,
        |    sum(coalesce(sr_return_amt, 0)) AS returns,
        |    sum(ss_net_profit - coalesce(sr_net_loss, 0)) AS profit
        |  FROM store_sales
        |    LEFT OUTER JOIN store_returns ON
        |                                    (ss_item_sk = sr_item_sk AND
        |                                      ss_ticket_number = sr_ticket_number)
        |    ,
        |    date_dim, store, item, promotion
        |  WHERE ss_sold_date_sk = d_date_sk
        |    AND d_date BETWEEN cast('2000-08-23' AS DATE)
        |  AND (cast('2000-08-23' AS DATE) + INTERVAL 30 days)
        |    AND ss_store_sk = s_store_sk
        |    AND ss_item_sk = i_item_sk
        |    AND i_current_price > 50
        |    AND ss_promo_sk = p_promo_sk
        |    AND p_channel_tv = 'N'
        |  GROUP BY s_store_id),
        |    csr AS
        |  (SELECT
        |    cp_catalog_page_id AS catalog_page_id,
        |    sum(cs_ext_sales_price) AS sales,
        |    sum(coalesce(cr_return_amount, 0)) AS returns,
        |    sum(cs_net_profit - coalesce(cr_net_loss, 0)) AS profit
        |  FROM catalog_sales
        |    LEFT OUTER JOIN catalog_returns ON
        |                                      (cs_item_sk = cr_item_sk AND
        |                                        cs_order_number = cr_order_number)
        |    ,
        |    date_dim, catalog_page, item, promotion
        |  WHERE cs_sold_date_sk = d_date_sk
        |    AND d_date BETWEEN cast('2000-08-23' AS DATE)
        |  AND (cast('2000-08-23' AS DATE) + INTERVAL 30 days)
        |    AND cs_catalog_page_sk = cp_catalog_page_sk
        |    AND cs_item_sk = i_item_sk
        |    AND i_current_price > 50
        |    AND cs_promo_sk = p_promo_sk
        |    AND p_channel_tv = 'N'
        |  GROUP BY cp_catalog_page_id),
        |    wsr AS
        |  (SELECT
        |    web_site_id,
        |    sum(ws_ext_sales_price) AS sales,
        |    sum(coalesce(wr_return_amt, 0)) AS returns,
        |    sum(ws_net_profit - coalesce(wr_net_loss, 0)) AS profit
        |  FROM web_sales
        |    LEFT OUTER JOIN web_returns ON
        |                                  (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number)
        |    ,
        |    date_dim, web_site, item, promotion
        |  WHERE ws_sold_date_sk = d_date_sk
        |    AND d_date BETWEEN cast('2000-08-23' AS DATE)
        |  AND (cast('2000-08-23' AS DATE) + INTERVAL 30 days)
        |    AND ws_web_site_sk = web_site_sk
        |    AND ws_item_sk = i_item_sk
        |    AND i_current_price > 50
        |    AND ws_promo_sk = p_promo_sk
        |    AND p_channel_tv = 'N'
        |  GROUP BY web_site_id)
        |SELECT
        |  channel,
        |  id,
        |  sum(sales) AS sales,
        |  sum(returns) AS returns,
        |  sum(profit) AS profit
        |FROM (SELECT
        |        'store channel' AS channel,
        |        concat('store', store_id) AS id,
        |        sales,
        |        returns,
        |        profit
        |      FROM ssr
        |      UNION ALL
        |      SELECT
        |        'catalog channel' AS channel,
        |        concat('catalog_page', catalog_page_id) AS id,
        |        sales,
        |        returns,
        |        profit
        |      FROM csr
        |      UNION ALL
        |      SELECT
        |        'web channel' AS channel,
        |        concat('web_site', web_site_id) AS id,
        |        sales,
        |        returns,
        |        profit
        |      FROM wsr) x
        |GROUP BY ROLLUP (channel, id)
        |ORDER BY channel, id
        |LIMIT 100
        |
        |""".stripMargin
    RewriteSuite.comparePlansAndRows(sql80, "default", "mv68", noData = true)
  }
}

object TpcdsUtils {
  /**
   * Obtain the contents of the resource file
   *
   * @param path     If the path of the file relative to reousrce is "/tpcds", enter "/tpcds".
   * @param fileName If the file name is q14.sql, enter q14.sql here
   * @return
   */
  def getResource(path: String = "/", fileName: String): String = {
    val filePath = s"${this.getClass.getResource(path).getPath}/${fileName}"
    Source.fromFile(filePath).mkString
  }
}

