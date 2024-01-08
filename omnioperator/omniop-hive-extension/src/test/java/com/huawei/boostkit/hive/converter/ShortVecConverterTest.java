package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShortVecConverterTest extends CommonTest {
    @Test
    public void testShortVecConverter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_converter");
        utRunSqlOnDriver("create table if not exists test_converter (class_id int, flag smallint) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_converter values(1, 1111)");
        utRunSqlOnDriver("insert into test_converter values(2, 2222)");
        utRunSqlOnDriver("insert into test_converter values(3, 3333)");
        utRunSqlOnDriver("insert into test_converter values(4, 4444)");
        utRunSqlOnDriver("select class_id, flag from test_converter order by flag");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_converter");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\t1111", rs.get(0));
        Assert.assertEquals("2\t2222", rs.get(1));
        Assert.assertEquals("3\t3333", rs.get(2));
        Assert.assertEquals("4\t4444", rs.get(3));
    }
}
