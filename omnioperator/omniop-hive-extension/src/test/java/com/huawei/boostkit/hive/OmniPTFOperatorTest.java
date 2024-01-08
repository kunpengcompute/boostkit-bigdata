package com.huawei.boostkit.hive;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniPTFOperatorTest extends CommonTest {
    @Test
    public void testPTFRank() throws IOException {
        utRunSqlOnDriver("use ut_database");

        utRunSqlOnDriver("drop table if exists ptf_rank");
        utRunSqlOnDriver("create table if not exists ptf_rank (class_id int, name string, score int) stored as orc");
        utRunSqlOnDriver("insert into ptf_rank values(1, 'Tom', 100)");
        utRunSqlOnDriver("insert into ptf_rank values(1, 'Jerry', 90)");
        utRunSqlOnDriver("insert into ptf_rank values(2, 'Bob', 91)");
        utRunSqlOnDriver("insert into ptf_rank values(2, 'Li', 99)");

        utRunSqlOnDriver("select class_id, name, score, rank() over(partition by class_id order by score desc) " +
                "from ptf_rank");
        List<String> rs = new ArrayList<>();
        driver.getResults(rs);
        utRunSqlOnDriver("drop table if exists ptf_rank");
        Assert.assertEquals(4, rs.size());
        Assert.assertEquals("1\tTom\t100\t1", rs.get(0));
        Assert.assertEquals("1\tJerry\t90\t2", rs.get(1));
        Assert.assertEquals("2\tLi\t99\t1", rs.get(2));
        Assert.assertEquals("2\tBob\t91\t2", rs.get(3));
    }
}
