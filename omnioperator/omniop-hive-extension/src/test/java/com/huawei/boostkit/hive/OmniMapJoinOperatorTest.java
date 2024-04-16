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

public class OmniMapJoinOperatorTest extends CommonTest {
    @Test
    public void testMapJoinTwoTables() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("create table if not exists map_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table1 values(1, 100)");
        utRunSqlOnDriver("insert into map_join_table1 values(2, 90)");

        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("create table if not exists map_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into map_join_table2 values(1, 'Tom')");
        utRunSqlOnDriver("insert into map_join_table2 values(2, 'Jerry')");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score from map_join_table1 t1, map_join_table2 t2 "
                + "where t1.id = t2.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("drop table if exists map_join_table2");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100", rs.get(0));
        Assert.assertEquals("2\tJerry\t90", rs.get(1));
    }

    @Test
    public void testMapJoinThreeTables() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("create table if not exists map_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table1 values(1, 100)");
        utRunSqlOnDriver("insert into map_join_table1 values(2, 90)");

        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("create table if not exists map_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into map_join_table2 values(1, 'Tom')");
        utRunSqlOnDriver("insert into map_join_table2 values(2, 'Jerry')");

        utRunSqlOnDriver("drop table if exists map_join_table3");
        utRunSqlOnDriver("create table if not exists map_join_table3 (id int, age int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table3 values(1, 15)");
        utRunSqlOnDriver("insert into map_join_table3 values(2, 16)");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score, t3.age from map_join_table1 t1, map_join_table2 t2, "
                + "map_join_table3 t3 where t1.id = t2.id and t1.id = t3.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("drop table if exists map_join_table3");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100\t15", rs.get(0));
        Assert.assertEquals("2\tJerry\t90\t16", rs.get(1));
    }

    @Test
    public void testMapJoinSwitch() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("create table if not exists map_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table1 values(1, 100)");
        utRunSqlOnDriver("insert into map_join_table1 values(2, 90)");

        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("create table if not exists map_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into map_join_table2 values(1, 'Tom')");
        utRunSqlOnDriver("insert into map_join_table2 values(2, 'Jerry')");

        String sql = "select t1.id, t2.name, t1.score from map_join_table1 t1, map_join_table2 t2 "
                + "where t1.id = t2.id order by t2.id";
        String confStr = "omni.hive.mapjoin.enabled";
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
        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("drop table if exists map_join_table2");

        String str = "Omni Map Join Operator";
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
