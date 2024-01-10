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

package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IntVecConverterTest extends CommonTest {
    @Test
    public void testIntVecConverter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_converter");
        utRunSqlOnDriver("create table if not exists test_converter (class_id int, flag int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_converter values(1, 11)");
        utRunSqlOnDriver("insert into test_converter values(2, 22)");
        utRunSqlOnDriver("insert into test_converter values(3, 33)");
        utRunSqlOnDriver("insert into test_converter values(4, 44)");
        utRunSqlOnDriver("select class_id, flag from test_converter order by flag");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_converter");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\t11", rs.get(0));
        Assert.assertEquals("2\t22", rs.get(1));
        Assert.assertEquals("3\t33", rs.get(2));
        Assert.assertEquals("4\t44", rs.get(3));
    }
}
