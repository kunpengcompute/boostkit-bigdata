/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

public class OmniTableScanOperatorTest extends CommonTest {
    @Test
    public void testTableScanOrc() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_tablescan");
        utRunSqlOnDriver("create table if not exists test_tablescan (class_id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_tablescan values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into test_tablescan values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into test_tablescan values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into test_tablescan values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score from test_tablescan order by class_id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
        Assert.assertEquals("2\tBob\t91", rs.get(2));
        Assert.assertEquals("2\tLi\t99", rs.get(3));

        conf.set("hive.vectorized.execution.enabled", "false");
        utRunSqlOnDriver("select class_id, name, score from test_tablescan order by class_id");
        rs = new ArrayList<>();
        driver.getResults(rs);
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
        Assert.assertEquals("2\tBob\t91", rs.get(2));
        Assert.assertEquals("2\tLi\t99", rs.get(3));
        utRunSqlOnDriver("drop table if exists test_tablescan");
    }

    @Test
    public void testTableScanParquet() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_tablescan");
        utRunSqlOnDriver("create table if not exists test_tablescan (class_id int, name string, score int) " +
                "stored as parquet");
        utRunSqlOnDriver("insert into test_tablescan values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into test_tablescan values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into test_tablescan values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into test_tablescan values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score from test_tablescan order by class_id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
        Assert.assertEquals("2\tBob\t91", rs.get(2));
        Assert.assertEquals("2\tLi\t99", rs.get(3));

        conf.set("hive.vectorized.execution.enabled", "false");
        utRunSqlOnDriver("select class_id, name, score from test_tablescan order by class_id");
        rs = new ArrayList<>();
        driver.getResults(rs);
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
        Assert.assertEquals("2\tBob\t91", rs.get(2));
        Assert.assertEquals("2\tLi\t99", rs.get(3));
        utRunSqlOnDriver("drop table if exists test_tablescan");
    }
}
