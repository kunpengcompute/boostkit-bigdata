package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BetweenExpressionTest extends CommonTest {
    @Test
    public void testBetweenExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists between_expression");
        utRunSqlOnDriver("create table if not exists between_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into between_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into between_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select * from between_expression where id between 0 and 1 ");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists between_expression");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("1\tTom", rs.get(0));
    }
}
