package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.CommonTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SubstrExpressionTest extends CommonTest {
    @Test
    public void testSubstrExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists substr_expression");
        utRunSqlOnDriver("create table if not exists substr_expression (id int, name string) " +
                "stored as orc");
        utRunSqlOnDriver("insert into substr_expression values(1, 'Tom')");
        utRunSqlOnDriver("insert into substr_expression values(2, 'Jerry')");

        utRunSqlOnDriver("select id, substr(name, 0, 2) from substr_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists substr_expression");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("2\tJe", rs.get(0));
        Assert.assertEquals("1\tTo", rs.get(1));
    }
}
