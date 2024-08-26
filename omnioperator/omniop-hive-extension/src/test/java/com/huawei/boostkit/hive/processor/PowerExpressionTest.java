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

package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PowerExpressionTest extends CommonTest {
    @Test
    public void testPowerExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists power_expression");
        utRunSqlOnDriver("create table if not exists power_expression (id int, base double, exponent double) "
                + "stored as orc");
        utRunSqlOnDriver("insert into power_expression values(1, 2, 1)");
        utRunSqlOnDriver("insert into power_expression values(2, 2, 2)");
        utRunSqlOnDriver("insert into power_expression values(3, 2, 3)");
        utRunSqlOnDriver("insert into power_expression values(4, 2, 4)");
        utRunSqlOnDriver("select power(base, exponent) from power_expression order by id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);

        utRunSqlOnDriver("drop table if exists power_expression");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("2.0", rs.get(0));
        Assert.assertEquals("4.0", rs.get(1));
        Assert.assertEquals("8.0", rs.get(2));
        Assert.assertEquals("16.0", rs.get(3));
    }
}
