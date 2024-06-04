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

public class ArithmeticExpressionTest extends CommonTest {
    @Test
    public void testArithmeticExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists arithmetic_expression");
        utRunSqlOnDriver("create table if not exists arithmetic_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into arithmetic_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into arithmetic_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id * 2, name from arithmetic_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists arithmetic_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("4\tJerry", rs.get(0));
        Assert.assertEquals("2\tTom", rs.get(1));
    }

    @Test
    public void testDecimalArithmeticExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");
        utRunSqlOnDriver("drop table if exists decimal_arithmetic_expression");
        utRunSqlOnDriver("create table if not exists decimal_arithmetic_expression (d1 decimal(18,8), d2 decimal(18,8)) " +
                "stored as orc");
        utRunSqlOnDriver("insert into decimal_arithmetic_expression values(1, 1)");
        utRunSqlOnDriver("insert into decimal_arithmetic_expression values(2, 2)");
        utRunSqlOnDriver("select d1 * 2, d2 + d1 from decimal_arithmetic_expression order by d1");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists decimal_arithmetic_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2.00000000\t2.00000000", rs.get(0));
        Assert.assertEquals("4.00000000\t4.00000000", rs.get(1));
    }
}
