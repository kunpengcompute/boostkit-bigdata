package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniSelectOperatorTest extends CommonTest {
    @Test
    public void testSelectColumn() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists select_column");
        utRunSqlOnDriver("create table if not exists select_column (class_id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into select_column values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into select_column values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into select_column values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into select_column values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score from select_column");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists select_column");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t90", rs.get(0));
        Assert.assertEquals("1\tTom\t100", rs.get(1));
        Assert.assertEquals("2\tBob\t91", rs.get(2));
        Assert.assertEquals("2\tLi\t99", rs.get(3));
    }

    @Test
    public void testSelectExpression() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists select_expression");
        utRunSqlOnDriver("create table if not exists select_expression (class_id int, name string, score int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into select_expression values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into select_expression values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into select_expression values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into select_expression values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, (score / 100) from select_expression");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists select_expression");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tJerry\t0.9", rs.get(0));
        Assert.assertEquals("1\tTom\t1.0", rs.get(1));
        Assert.assertEquals("2\tBob\t0.91", rs.get(2));
        Assert.assertEquals("2\tLi\t0.99", rs.get(3));
    }
}
