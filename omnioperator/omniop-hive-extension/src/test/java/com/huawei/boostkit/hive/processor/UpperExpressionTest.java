package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpperExpressionTest extends CommonTest {
    @Test
    public void testUpperExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists upper_expression");
        utRunSqlOnDriver("create table if not exists upper_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into upper_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into upper_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id, upper(name) from upper_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists upper_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJERRY", rs.get(0));
        Assert.assertEquals("1\tTOM", rs.get(1));
    }
}
