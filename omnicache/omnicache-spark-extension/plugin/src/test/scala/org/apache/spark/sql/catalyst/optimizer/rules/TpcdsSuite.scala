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
    comparePlansAndRows(sql, "default", "mv536", noData = true)
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
    comparePlansAndRows(sql, "default", "mv9", noData = true)
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
    comparePlansAndRows(sql, "default", "mv103", noData = true)
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
    comparePlansAndRows(sql, "default", "sc01", noData = true)
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS sc01")
  }
}

