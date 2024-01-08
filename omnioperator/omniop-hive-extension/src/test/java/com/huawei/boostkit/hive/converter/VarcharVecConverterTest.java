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

        utRunSqlOnDriver("drop table if exists test_converter");
        utRunSqlOnDriver("create table if not exists test_converter (class_id int, flag varchar(20)) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_converter values(1, 'aaaa')");
        utRunSqlOnDriver("insert into test_converter values(2, 'bbbb')");
        utRunSqlOnDriver("insert into test_converter values(3, 'cccc')");
        utRunSqlOnDriver("insert into test_converter values(4, 'dddd')");
        utRunSqlOnDriver("select class_id, flag from test_converter order by flag");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_converter");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\taaaa", rs.get(0));
        Assert.assertEquals("2\tbbbb", rs.get(1));
        Assert.assertEquals("3\tcccc", rs.get(2));
        Assert.assertEquals("4\tdddd", rs.get(3));
    }
}
