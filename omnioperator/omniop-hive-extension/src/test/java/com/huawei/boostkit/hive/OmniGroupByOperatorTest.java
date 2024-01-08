package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniGroupByOperatorTest extends CommonTest {
    @Test
    public void testGroupBySum() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_sum");
        utRunSqlOnDriver("create table if not exists group_by_sum(id int, name string) stored as orc");
        utRunSqlOnDriver("insert into group_by_sum values(1, 'Tom')");
        utRunSqlOnDriver("insert into group_by_sum values(2, 'Jerry')");

        utRunSqlOnDriver("select sum(id) from group_by_sum");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_sum");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("3", rs.get(0));
    }

    @Test
    public void testGroupByRollUp() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_roll_up");
        utRunSqlOnDriver("create table if not exists group_by_roll_up(id int, name string) stored as orc");
        utRunSqlOnDriver("insert into group_by_roll_up values(1, 'Tom')");
        utRunSqlOnDriver("insert into group_by_roll_up values(2, 'Jerry')");

        utRunSqlOnDriver("select id, name, count(*) from group_by_roll_up group by rollup(id, name)");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_roll_up");
        Assert.assertEquals(5, rs.size());
        List<String> exceptedResult = new ArrayList<>();
        exceptedResult.add("NULL\tNULL\t2");
        exceptedResult.add("1\tTom\t1");
        exceptedResult.add("2\tNULL\t1");
        exceptedResult.add("2\tJerry\t1");
        exceptedResult.add("1\tNULL\t1");
        Assert.assertEquals(exceptedResult, rs);
    }

    @Test
    public void testGroupByConstantKey() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_constant_key");
        utRunSqlOnDriver("create table if not exists group_by_constant_key(id int, name string, age int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into group_by_constant_key values(1, 'Tom', 15)");
        utRunSqlOnDriver("insert into group_by_constant_key values(2, 'Jerry', 15)");

        utRunSqlOnDriver("select count(*) from group_by_constant_key where age = 15 group by age");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_constant_key");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("2", rs.get(0));
    }

    @Test
    public void testGroupByNonVectorized() throws IOException {
        createNonVectorizedDriver();
        utRunSqlOnDriver("create database if not exists group_by_non_vectorized");
        utRunSqlOnDriver("use group_by_non_vectorized");
        utRunSqlOnDriver("drop table if exists group_by_non_vectorized");
        utRunSqlOnDriver("create table if not exists group_by_non_vectorized(id int, name string, age int) " +
                "stored as orc");
        utRunSqlOnDriver("insert into group_by_non_vectorized values(1, 'Tom', 15)");
        utRunSqlOnDriver("insert into group_by_non_vectorized values(2, 'Jerry', 15)");

        utRunSqlOnDriver("select count(*) from group_by_non_vectorized where age = 15 group by age");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_non_vectorized");
        utRunSqlOnDriver("drop database if exists group_by_non_vectorized cascade");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("2", rs.get(0));
        createVectorizedDriver();
    }


    @Test
    public void testGroupBySumNull() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists group_by_sum");
        utRunSqlOnDriver("create table if not exists group_by_sum(id int, name string) stored as orc");

        utRunSqlOnDriver("select sum(id) from group_by_sum");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists group_by_sum");
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("NULL", rs.get(0));
    }
}
