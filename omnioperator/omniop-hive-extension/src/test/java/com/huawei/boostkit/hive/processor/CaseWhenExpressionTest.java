package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CaseWhenExpressionTest extends CommonTest {
    @Test
    public void testCaseWhenExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists case_when_expression");
        utRunSqlOnDriver("create table if not exists case_when_expression (id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into case_when_expression values(1, 'Tom', 80)");
        utRunSqlOnDriver("insert into case_when_expression values(2, 'Jerry', 90)");

        utRunSqlOnDriver("select id, name from case_when_expression where case when score >= 90 then score " +
                " else score + 1 end > 80");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists case_when_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJerry", rs.get(0));
        Assert.assertEquals("1\tTom", rs.get(1));
    }
}
