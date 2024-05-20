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

package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LengthExpressionTest extends CommonTest {

    @Test
    public void testLengthExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists length_expression");
        utRunSqlOnDriver("create table if not exists length_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into length_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into length_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id, name, length(name) from length_expression order by id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("select id, name from length_expression where length(name) > 3");
        List<String> rs1 = new ArrayList<>();
        driver.getResults(rs1);

        utRunSqlOnDriver("drop table if exists length_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t3", rs.get(0));
        Assert.assertEquals("2\tJerry\t5", rs.get(1));
        Assert.assertEquals(1, rs1.size());
        Assert.assertEquals("2\tJerry", rs1.get(0));
    }
}
