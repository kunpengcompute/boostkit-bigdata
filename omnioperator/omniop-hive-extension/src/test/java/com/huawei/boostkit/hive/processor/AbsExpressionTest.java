package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AbsExpressionTest extends CommonTest {
    @Test
    public void testAbsExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists abs_expression");
        utRunSqlOnDriver("create table if not exists abs_expression (id int, name string, money int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into abs_expression values(1, 'Tom', -100)");
        utRunSqlOnDriver("insert into abs_expression values(2, 'Jerry', 90)");

        utRunSqlOnDriver("select id, name, abs(money) from abs_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists abs_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
    }
}
