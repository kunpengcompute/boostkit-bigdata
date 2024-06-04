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

package com.huawei.boostkit.hive.shuffle;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SerDeTest extends CommonTest {
    @Test
    public void testSerDe() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_serde");
        utRunSqlOnDriver("create table if not exists test_serde (c_id int, c_string string) stored as orc");
        utRunSqlOnDriver("insert into test_serde values(1, 'a')");
        utRunSqlOnDriver("insert into test_serde values(1, 'a')");
        utRunSqlOnDriver("insert into test_serde values(2, 'b')");
        utRunSqlOnDriver("insert into test_serde values(2, 'b')");
        utRunSqlOnDriver("insert into test_serde values(3, 'c')");
        utRunSqlOnDriver("insert into test_serde values(3, 'c')");

        utRunSqlOnDriver("select c_id,count(*) from test_serde group by c_id order by c_id");
        List<String> rs1 = new ArrayList<>();
        driver.getResults(rs1);
        utRunSqlOnDriver("select c_string,count(*) from test_serde group by c_string order by c_string limit 1");
        List<String> rs2 = new ArrayList<>();
        driver.getResults(rs2);
        utRunSqlOnDriver("select c_string,count(*) from test_serde group by c_string order by c_string desc limit 1");
        List<String> rs3 = new ArrayList<>();
        driver.getResults(rs3);

        utRunSqlOnDriver("drop table if exists test_serde");
        Assert.assertEquals(3, rs1.size());
        Assert.assertEquals("1\t2", rs1.get(0));
        Assert.assertEquals("2\t2", rs1.get(1));
        Assert.assertEquals("3\t2", rs1.get(2));
        Assert.assertEquals(1, rs2.size());
        Assert.assertEquals("a\t2", rs2.get(0));
        Assert.assertEquals(1, rs3.size());
        Assert.assertEquals("c\t2", rs3.get(0));
    }
}
