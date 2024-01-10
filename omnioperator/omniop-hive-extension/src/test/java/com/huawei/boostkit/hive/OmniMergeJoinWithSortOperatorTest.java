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

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniMergeJoinWithSortOperatorTest extends CommonTest {
    @Test
    public void testMergeJoinTwoTableWithSort() throws IOException {
        createNonAutoConvertJoinDriver();
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists merge_join_table1");
        utRunSqlOnDriver("create table if not exists merge_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into merge_join_table1 values(1,  100)");
        utRunSqlOnDriver("insert into merge_join_table1 values(2,  90)");

        utRunSqlOnDriver("drop table if exists merge_join_table2");
        utRunSqlOnDriver("create table if not exists merge_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into merge_join_table2 values(1,  'Tom')");
        utRunSqlOnDriver("insert into merge_join_table2 values(2,  'Jerry')");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score from merge_join_table1 t1, merge_join_table2 t2 "
                + "where t1.id = t2.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists merge_join_table1");
        utRunSqlOnDriver("drop table if exists merge_join_table2");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100", rs.get(0));
        Assert.assertEquals("2\tJerry\t90", rs.get(1));
        createAutoConvertJoinDriver();
    }

    public void createAutoConvertJoinDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.auto.convert.join", "true");
        SessionState.start(conf);
        driver = new Driver(conf);
    }

    public void createNonAutoConvertJoinDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.auto.convert.join", "false");
        SessionState.start(conf);
        driver = new Driver(conf);
    }
}
