package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniMapJoinOperatorTest extends CommonTest {
    @Test
    public void testMapJoinTwoTables() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("create table if not exists map_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table1 values(1, 100)");
        utRunSqlOnDriver("insert into map_join_table1 values(2, 90)");

        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("create table if not exists map_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into map_join_table2 values(1, 'Tom')");
        utRunSqlOnDriver("insert into map_join_table2 values(2, 'Jerry')");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score from map_join_table1 t1, map_join_table2 t2 "
                + "where t1.id = t2.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("drop table if exists map_join_table2");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100", rs.get(0));
        Assert.assertEquals("2\tJerry\t90", rs.get(1));
    }

    @Test
    public void testMapJoinThreeTables() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("create table if not exists map_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table1 values(1, 100)");
        utRunSqlOnDriver("insert into map_join_table1 values(2, 90)");

        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("create table if not exists map_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into map_join_table2 values(1, 'Tom')");
        utRunSqlOnDriver("insert into map_join_table2 values(2, 'Jerry')");

        utRunSqlOnDriver("drop table if exists map_join_table3");
        utRunSqlOnDriver("create table if not exists map_join_table3 (id int, age int) stored as orc");
        utRunSqlOnDriver("insert into map_join_table3 values(1, 15)");
        utRunSqlOnDriver("insert into map_join_table3 values(2, 16)");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score, t3.age from map_join_table1 t1, map_join_table2 t2, "
                + "map_join_table3 t3 where t1.id = t2.id and t1.id = t3.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists map_join_table1");
        utRunSqlOnDriver("drop table if exists map_join_table2");
        utRunSqlOnDriver("drop table if exists map_join_table3");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100\t15", rs.get(0));
        Assert.assertEquals("2\tJerry\t90\t16", rs.get(1));
    }
}
