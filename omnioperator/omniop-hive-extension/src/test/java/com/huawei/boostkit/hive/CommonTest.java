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
