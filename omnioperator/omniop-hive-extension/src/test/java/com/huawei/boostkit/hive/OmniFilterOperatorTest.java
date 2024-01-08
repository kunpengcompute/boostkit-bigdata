package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniFilterOperatorTest extends CommonTest{
    @Test
    public void testFilter() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists test_filter");
        utRunSqlOnDriver("create table if not exists test_filter (class_id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into test_filter values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into test_filter values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into test_filter values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into test_filter values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score from test_filter where class_id=1");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists test_filter");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
    }
}
