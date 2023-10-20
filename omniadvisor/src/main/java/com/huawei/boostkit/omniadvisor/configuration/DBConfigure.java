/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.configuration;

import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceFactory;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;

public final class DBConfigure {
    private static final Logger LOG = LoggerFactory.getLogger(DBConfigure.class);

    private static final String DB_DEFAULT_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_DRIVER_KEY = "datasource.db.driver";
    private static final String DB_URL_KEY = "datasource.db.url";
    private static final String DB_USERNAME_KEY = "datasource.db.username";
    private static final String DB_PASSWORD_KEY = "datasource.db.password";

    private DBConfigure() {}

    public static void initDatabase(PropertiesConfiguration configuration, String userName, String passWord) {
        Properties databaseProperties = new Properties();
        databaseProperties.put(DB_DRIVER_KEY, configuration.getString(DB_DRIVER_KEY, DB_DEFAULT_DRIVER));
        databaseProperties.put(DB_URL_KEY, configuration.getString(DB_URL_KEY));
        databaseProperties.put(DB_USERNAME_KEY, userName);
        databaseProperties.put(DB_PASSWORD_KEY, passWord);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.loadFromProperties(databaseProperties);

        dbConfig.setDataSource(DataSourceFactory.create(dbConfig.getName(), dbConfig.getDataSourceConfig()));

        checkInit(dbConfig);

        DatabaseFactory.create(dbConfig);
    }

    public static void checkInit(DatabaseConfig dbConfig) {
        boolean isInit;
        try (Connection conn = dbConfig.getDataSource().getConnection();
             ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), null, AppResult.RESULT_TABLE_NAME, null)) {
            isInit = rs.next();
        } catch (SQLException e) {
            throw new OmniAdvisorException(format("Failed to connect to dataSource, %s", e));
        }

        if (!isInit) {
            LOG.info("Analyze result table is not exist, creating it");
            dbConfig.setDdlGenerate(true);
            dbConfig.setDdlRun(true);
        }
    }
}
