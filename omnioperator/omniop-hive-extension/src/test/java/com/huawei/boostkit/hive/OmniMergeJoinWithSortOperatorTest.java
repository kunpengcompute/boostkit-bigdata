package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniMergeJoinWithSortOperatorTest extends CommonTest{
    @Test
    public void testMergeJoinTwoTableWithSort() throws IOException {
        createNonAutoConvertJoinDriver();
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists merge_join_table1");
        utRunSqlOnDriver("create table if not exists merge_join_table1 (id int, score int) stored as orc");
        utRunSqlOnDriver("insert into merge_join_table1 values(1,  100)");
        utRunSqlOnDriver("insert into merge_join_table1 values(2,  90)");

        utRunSqlOnDriver("drop table if exists merge_join_table2");
        utRunSqlOnDriver("create table if not exists merge_join_table2 (id int, name string) stored as orc");
        utRunSqlOnDriver("insert into merge_join_table2 values(1,  'Tom')");
        utRunSqlOnDriver("insert into merge_join_table2 values(2,  'Jerry')");

        utRunSqlOnDriver("select t1.id, t2.name, t1.score from merge_join_table1 t1, merge_join_table2 t2 "
                + "where t1.id = t2.id order by t1.id");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists merge_join_table1");
        utRunSqlOnDriver("drop table if exists merge_join_table2");
        Assert.assertEquals(2, rs.size());
        Assert.assertEquals("1\tTom\t100", rs.get(0));
        Assert.assertEquals("2\tJerry\t90", rs.get(1));
        createAutoConvertJoinDriver();
    }

    public void createAutoConvertJoinDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.auto.convert.join", "true");
        SessionState.start(conf);
        driver = new Driver(conf);
    }

    public void createNonAutoConvertJoinDriver() {
        driver.close();
        driver.destroy();
        conf.set("hive.auto.convert.join", "false");
        SessionState.start(conf);
        driver = new Driver(conf);
    }
}
