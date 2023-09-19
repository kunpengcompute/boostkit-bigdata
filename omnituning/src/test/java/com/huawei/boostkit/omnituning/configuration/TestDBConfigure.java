/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnituning.configuration;

import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceInitialiseException;
import org.junit.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestDBConfigure extends TestConfiguration {
    @Test(expected = DataSourceInitialiseException.class)
    public void testDBConfigure() {
        DBConfigure.initDatabase(testConfiguration, "user", "passwd");
    }

    @Test
    public void testInitDataBase() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Connection connection = Mockito.mock(Connection.class);
        DataSource dataSource = Mockito.mock(DataSource.class);
        DatabaseConfig dbConfig = Mockito.mock(DatabaseConfig.class);

        when(resultSet.next()).thenReturn(true);
        when(metaData.getTables(any(), any(), any(), any())).thenReturn(resultSet);
        when(connection.getMetaData()).thenReturn(metaData);
        when(dbConfig.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        DBConfigure.checkInit(dbConfig);
    }

    @Test
    public void testNotInitDatabase() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        Connection connection = Mockito.mock(Connection.class);
        DataSource dataSource = Mockito.mock(DataSource.class);
        DatabaseConfig dbConfig = Mockito.mock(DatabaseConfig.class);

        when(resultSet.next()).thenReturn(false);
        when(metaData.getTables(any(), any(), any(), any())).thenReturn(resultSet);
        when(connection.getMetaData()).thenReturn(metaData);
        when(dbConfig.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        DBConfigure.checkInit(dbConfig);
    }
}
