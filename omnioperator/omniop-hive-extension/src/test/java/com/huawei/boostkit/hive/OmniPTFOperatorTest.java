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

public class OmniPTFOperatorTest extends CommonTest {
    @Test
    public void testPTFRank() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists ptf_rank");
        utRunSqlOnDriver("create table if not exists ptf_rank (class_id int, name string, score int) stored as orc");
        utRunSqlOnDriver("insert into ptf_rank values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into ptf_rank values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into ptf_rank values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into ptf_rank values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score, rank() over(partition by class_id order by score desc) " +
                "from ptf_rank");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists ptf_rank");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tTom\t100\t1", rs.get(0));
        Assert.assertEquals("1\tJerry\t90\t2", rs.get(1));
        Assert.assertEquals("2\tLi\t99\t1", rs.get(2));
        Assert.assertEquals("2\tBob\t91\t2", rs.get(3));
    }
}
