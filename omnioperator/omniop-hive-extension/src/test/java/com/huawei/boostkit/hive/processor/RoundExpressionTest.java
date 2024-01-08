package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RoundExpressionTest extends CommonTest {
    @Test
    public void testRoundExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists round_expression");
        utRunSqlOnDriver("create table if not exists round_expression (id int, name string, money int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into round_expression values(1, 'Tom', 1000)");
        utRunSqlOnDriver("insert into round_expression values(2, 'Jerry', 800)");

        utRunSqlOnDriver("select id, name, round(money / 3, 2) from round_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists round_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJerry\t266.67", rs.get(0));
        Assert.assertEquals("1\tTom\t333.33", rs.get(1));
    }
}
