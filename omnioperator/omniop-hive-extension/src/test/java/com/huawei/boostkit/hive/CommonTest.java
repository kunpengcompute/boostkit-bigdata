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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.URL;

public class CommonTest {
    protected static Driver driver;

    protected static HiveConf conf;

    @BeforeClass
    public static void Setup() {
        URL resource = CommonTest.class.getClassLoader().getResource("test-log4j2.properties");
        assert resource != null;
        System.setProperty("log4j.configurationFile", resource.getFile());
        HiveConf.setHiveSiteLocation(CommonTest.class.getClassLoader().getResource("test-hive-site.xml"));
        conf = new HiveConf();

        // use omni
        conf.set("hive.exec.pre.hooks", "com.huawei.boostkit.hive.OmniExecuteWithHookContext");

        SessionState.start(conf);
        driver = new Driver(conf);
        utRunSqlOnDriver("create database if not exists ut_database");
    }

    @AfterClass
    public static void Teardown() {
        utRunSqlOnDriver("drop database if exists ut_database cascade");
        driver.close();
        driver.destroy();
    }

    public static void utRunSqlOnDriver(String cmd) {
        int ret = driver.run(cmd).getResponseCode();
        Assert.assertEquals("Checking command success", 0, ret);
    }

    public void createVectorizedDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.vectorized.execution.enabled", "true");
        SessionState.start(conf);
        driver = new Driver(conf);
    }

    public void createNonVectorizedDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.vectorized.execution.enabled", "false");
        SessionState.start(conf);
        driver = new Driver(conf);
    }
}
