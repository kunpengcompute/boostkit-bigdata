package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DecimalVecConverterTest extends CommonTest {
    @Test
    public void testDecimalVecConverter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_converter");
        utRunSqlOnDriver("create table if not exists test_converter (class_id int, flag decimal(30, 6)) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_converter values(1, 111111111.111)");
        utRunSqlOnDriver("insert into test_converter values(2, 222222222.222)");
        utRunSqlOnDriver("insert into test_converter values(3, 333333333.333)");
        utRunSqlOnDriver("insert into test_converter values(4, 444444444.444)");
        utRunSqlOnDriver("select class_id, flag from test_converter order by flag");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_converter");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\t111111111.111000", rs.get(0));
        Assert.assertEquals("2\t222222222.222000", rs.get(1));
        Assert.assertEquals("3\t333333333.333000", rs.get(2));
        Assert.assertEquals("4\t444444444.444000", rs.get(3));
    }
}
