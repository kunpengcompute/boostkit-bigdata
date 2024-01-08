package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogicExpressionTest extends CommonTest {
    @Test
    public void testLogicExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists logic_expression");
        utRunSqlOnDriver("create table if not exists logic_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into logic_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into logic_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id, name from logic_expression where id = 1 and name = 'Tom'");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists logic_expression");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("1\tTom", rs.get(0));
    }
}

