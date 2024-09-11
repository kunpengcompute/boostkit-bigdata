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

public class OmniConditionSelectOperatorTest extends CommonTest {
    @Test
    public void testConditionSelect() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_condition_select");
        utRunSqlOnDriver("create table if not exists test_condition_select (class_id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_condition_select values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into test_condition_select values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into test_condition_select values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into test_condition_select values(2, 'Li', 99)");

        String sql = "select class_id, name, score from test_condition_select where class_id=1";
        String confStr = "omni.hive.conditionselect.enabled";
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
        utRunSqlOnDriver("drop table if exists test_condition_select");

        String str = "Omni ConditionSelect Operator";
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
        assert flag1 : "[" + confStr + " = true] is invalid";
        assert flag2 : "[" + confStr + " = false] is invalid";
        Assert.assertEquals(rsTrue.size(), rsFalse.size());
        for (int i = 0; i < rsTrue.size(); i++) {
            Assert.assertEquals(rsTrue.get(i), rsFalse.get(i));
        }
    }
}
