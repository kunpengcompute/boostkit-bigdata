package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArithmeticExpressionTest extends CommonTest {
    @Test
    public void testArithmeticExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists arithmetic_expression");
        utRunSqlOnDriver("create table if not exists arithmetic_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into arithmetic_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into arithmetic_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id * 2, name from arithmetic_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists arithmetic_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("4\tJerry", rs.get(0));
        Assert.assertEquals("2\tTom", rs.get(1));
    }
}
