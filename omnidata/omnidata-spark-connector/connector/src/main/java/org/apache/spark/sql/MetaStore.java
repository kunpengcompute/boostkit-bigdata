/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql;

import avro.shaded.com.google.common.collect.ImmutableSet;
import com.esotericsoftware.kryo.Kryo;
import io.prestosql.metadata.*;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.transaction.NoOpTransactionManager;
import io.prestosql.transaction.TransactionManager;

import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;

/**
 * MetaStore
 */
public class MetaStore {
    private static final Metadata metadata = initCompiler();
    private static final ConnectorSession connectorSession = initConnectorSession();

    private MetaStore() {
    }

    private static Metadata initCompiler() {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TransactionManager transactionManager = new NoOpTransactionManager();
        return new MetadataManager(new FunctionAndTypeManager(transactionManager, featuresConfig, new HandleResolver(), ImmutableSet.of(), new Kryo()), featuresConfig, new SessionPropertyManager(), new SchemaPropertyManager(), new TablePropertyManager(), new ColumnPropertyManager(), new AnalyzePropertyManager(), transactionManager, null);
    }

    /**
     * get Metadata instance
     *
     * @return Metadata
     */
    public static Metadata getMetadata() {
        return metadata;
    }

    private static ConnectorSession initConnectorSession() {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "null";
            }

            @Override
            public Optional<String> getSource() {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity() {
                return null;
            }

            @Override
            public TimeZoneKey getTimeZoneKey() {
                return TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID());
            }

            @Override
            public Locale getLocale() {
                return Locale.getDefault();
            }

            @Override
            public Optional<String> getTraceToken() {
                return Optional.empty();
            }

            @Override
            public long getStartTime() {
                return 0;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }
        };
    }

    /**
     * get ConnectorSession instance
     *
     * @return ConnectorSession
     */
    public static ConnectorSession getConnectorSession() {
        return connectorSession;
    }
}
