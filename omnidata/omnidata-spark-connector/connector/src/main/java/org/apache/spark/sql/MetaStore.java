/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.AnalyzePropertyManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.HandleResolver;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.TablePropertyManager;
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
 * Used to initialize some common classes
 *
 * @since 2023.04
 */
public class MetaStore {
    private static final Metadata metadata = initCompiler();
    private static final ConnectorSession connectorSession = initConnectorSession();

    private MetaStore() {
    }

    private static Metadata initCompiler() {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TransactionManager transactionManager = new NoOpTransactionManager();
        return new MetadataManager(
                new FunctionAndTypeManager(
                        transactionManager,
                        featuresConfig,
                        new HandleResolver(),
                        ImmutableSet.of(),
                        new Kryo()),
                featuresConfig,
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager,
                null);
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
            public <T extends Object> T getProperty(String name, Class<T> cls) {
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