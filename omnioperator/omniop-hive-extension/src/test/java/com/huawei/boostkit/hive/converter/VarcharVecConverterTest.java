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

public class VarcharVecConverterTest extends CommonTest {
    @Test
    public void testVarcharVecConverter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_varchar_vec_converter");
        utRunSqlOnDriver("create table if not exists test_varchar_vec_converter (class_id int, flag varchar(20)) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_varchar_vec_converter values(1, 'aaaa')");
        utRunSqlOnDriver("insert into test_varchar_vec_converter values(2, 'bbbb')");
        utRunSqlOnDriver("insert into test_varchar_vec_converter values(3, 'cccc')");
        utRunSqlOnDriver("insert into test_varchar_vec_converter values(4, 'dddd')");
        utRunSqlOnDriver("insert into test_varchar_vec_converter values(5, null)");

        createNonVectorizedDriver();
        utRunSqlOnDriver("use ut_database");
        utRunSqlOnDriver("select class_id, flag from test_varchar_vec_converter order by class_id");
        List<String> rsNonVectorized = new ArrayList<>();
        driver.getResults(rsNonVectorized);
        createVectorizedDriver();
        utRunSqlOnDriver("use ut_database");
        utRunSqlOnDriver("select class_id, flag from test_varchar_vec_converter order by class_id");
        List<String> rsVectorized = new ArrayList<>();
        driver.getResults(rsVectorized);

        utRunSqlOnDriver("drop table if exists test_varchar_vec_converter");
        Assert.assertEquals(5, rsNonVectorized.size());
        Assert.assertEquals("1\taaaa", rsNonVectorized.get(0));
        Assert.assertEquals("2\tbbbb", rsNonVectorized.get(1));
        Assert.assertEquals("3\tcccc", rsNonVectorized.get(2));
        Assert.assertEquals("4\tdddd", rsNonVectorized.get(3));
        Assert.assertEquals("5\tNULL", rsNonVectorized.get(4));
        Assert.assertEquals(5, rsVectorized.size());
        Assert.assertEquals("1\taaaa", rsVectorized.get(0));
        Assert.assertEquals("2\tbbbb", rsVectorized.get(1));
        Assert.assertEquals("3\tcccc", rsVectorized.get(2));
        Assert.assertEquals("4\tdddd", rsVectorized.get(3));
        Assert.assertEquals("5\tNULL", rsVectorized.get(4));
    }
}
