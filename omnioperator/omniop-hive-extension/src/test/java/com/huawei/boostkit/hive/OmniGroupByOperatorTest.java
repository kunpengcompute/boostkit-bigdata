/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniGroupByOperatorTest extends CommonTest {
    @Test
    public void testGroupBySum() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_sum");
        utRunSqlOnDriver("create table if not exists group_by_sum(id int, name string) stored as orc");
        utRunSqlOnDriver("insert into group_by_sum values(1, 'Tom')");
        utRunSqlOnDriver("insert into group_by_sum values(2, 'Jerry')");

        utRunSqlOnDriver("select sum(id) from group_by_sum");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_sum");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("3", rs.get(0));
    }

    @Test
    public void testGroupByRollUp() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_roll_up");
        utRunSqlOnDriver("create table if not exists group_by_roll_up(id int, name string) stored as orc");
        utRunSqlOnDriver("insert into group_by_roll_up values(1, 'Tom')");
        utRunSqlOnDriver("insert into group_by_roll_up values(2, 'Jerry')");

        utRunSqlOnDriver("select id, name, count(*) from group_by_roll_up group by rollup(id, name) order by id, name");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_roll_up");
        Assert.assertEquals(5, rs.size());
        List<String> exceptedResult = new ArrayList<>();
        exceptedResult.add("NULL\tNULL\t2");
        exceptedResult.add("1\tNULL\t1");
        exceptedResult.add("1\tTom\t1");
        exceptedResult.add("2\tNULL\t1");
        exceptedResult.add("2\tJerry\t1");
        Assert.assertEquals(exceptedResult, rs);
    }

    @Test
    public void testGroupByConstantKey() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_constant_key");
        utRunSqlOnDriver("create table if not exists group_by_constant_key(id int, name string, age int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into group_by_constant_key values(1, 'Tom', 15)");
        utRunSqlOnDriver("insert into group_by_constant_key values(2, 'Jerry', 15)");

        utRunSqlOnDriver("select count(*) from group_by_constant_key where age = 15 group by age");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_constant_key");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("2", rs.get(0));
    }

    @Test
    public void testGroupByNonVectorized() throws IOException {
        createNonVectorizedDriver();
        utRunSqlOnDriver("create database if not exists group_by_non_vectorized");
        utRunSqlOnDriver("use group_by_non_vectorized");
        utRunSqlOnDriver("drop table if exists group_by_non_vectorized");
        utRunSqlOnDriver("create table if not exists group_by_non_vectorized(id int, name string, age int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into group_by_non_vectorized values(1, 'Tom', 15)");
        utRunSqlOnDriver("insert into group_by_non_vectorized values(2, 'Jerry', 15)");

        utRunSqlOnDriver("select count(*) from group_by_non_vectorized where age = 15 group by age");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_non_vectorized");
        utRunSqlOnDriver("drop database if exists group_by_non_vectorized cascade");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("2", rs.get(0));
        createVectorizedDriver();
    }


    @Test
    public void testGroupBySumNull() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_sum");
        utRunSqlOnDriver("create table if not exists group_by_sum(id int, name string) stored as orc");

        utRunSqlOnDriver("select sum(id) from group_by_sum");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_sum");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("NULL", rs.get(0));
    }

    @Test
    public void testGroupBySwitch() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_sum");
        utRunSqlOnDriver("create table if not exists group_by_sum(id int, name string) stored as orc");
        utRunSqlOnDriver("insert into group_by_sum values(1, 'Tom')");
        utRunSqlOnDriver("insert into group_by_sum values(2, 'Jerry')");

        String sql = "select sum(id) from group_by_sum";
        String confStr = "omni.hive.groupby.enabled";
        driver.getConf().set(confStr, "true");
        utRunSqlOnDriver("explain " + sql);
        List<String> rsExplainTrue = new ArrayList<>();
        driver.getResults(rsExplainTrue);
        utRunSqlOnDriver(sql);
        List<String> rsTrue = new ArrayList<>();
        driver.getResults(rsTrue);
        driver.getConf().set(confStr, "false");
        utRunSqlOnDriver("explain " + sql);
        List<String> rsExplainFalse = new ArrayList<>();
        driver.getResults(rsExplainFalse);
        utRunSqlOnDriver(sql);
        List<String> rsFalse = new ArrayList<>();
        driver.getResults(rsFalse);
        driver.getConf().set(confStr, "true");
        utRunSqlOnDriver("drop table if exists group_by_sum");

        String str = "Omni Group By Operator";
        boolean flag1 = false;
        for (String s : rsExplainTrue) {
            if (s.contains(str)) {
                flag1 = true;
                break;
            }
        }
        boolean flag2 = true;
        for (String s : rsExplainFalse) {
            if (s.contains(str)) {
                flag2 = false;
                break;
            }
        }
        assert flag1 : "[" + confStr + "=true] is invalid";
        assert flag2 : "[" + confStr + "=false] is invalid";
        Assert.assertEquals(rsTrue.size(), rsFalse.size());
        for (int i = 0; i < rsTrue.size(); i++) {
            Assert.assertEquals(rsTrue.get(i), rsFalse.get(i));
        }
    }

}
