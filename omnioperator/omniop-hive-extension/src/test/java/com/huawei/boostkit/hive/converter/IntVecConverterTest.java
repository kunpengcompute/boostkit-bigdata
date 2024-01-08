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
