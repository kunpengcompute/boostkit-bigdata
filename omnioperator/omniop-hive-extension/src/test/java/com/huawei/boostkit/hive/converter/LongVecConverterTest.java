package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongVecConverterTest extends CommonTest {
    @Test
    public void testLongVecConverter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_converter");
        utRunSqlOnDriver("create table if not exists test_converter (class_id int, flag bigint) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_converter values(1, 11111111111111)");
        utRunSqlOnDriver("insert into test_converter values(2, 22222222222222)");
        utRunSqlOnDriver("insert into test_converter values(3, 33333333333333)");
        utRunSqlOnDriver("insert into test_converter values(4, 44444444444444)");
        utRunSqlOnDriver("select class_id, flag from test_converter order by flag");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_converter");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\t11111111111111", rs.get(0));
        Assert.assertEquals("2\t22222222222222", rs.get(1));
        Assert.assertEquals("3\t33333333333333", rs.get(2));
        Assert.assertEquals("4\t44444444444444", rs.get(3));
    }
}
