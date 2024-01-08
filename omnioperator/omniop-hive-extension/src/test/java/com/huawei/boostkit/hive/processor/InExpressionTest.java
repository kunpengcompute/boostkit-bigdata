package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InExpressionTest extends CommonTest {
    @Test
    public void testInExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists in_expression");
        utRunSqlOnDriver("create table if not exists in_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into in_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into in_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id, name from in_expression where id in (1, 2)");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists in_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJerry", rs.get(0));
        Assert.assertEquals("1\tTom", rs.get(1));
    }
}
