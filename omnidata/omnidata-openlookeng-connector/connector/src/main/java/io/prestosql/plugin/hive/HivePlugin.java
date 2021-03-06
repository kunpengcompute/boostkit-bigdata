/*
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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.function.ConnectorConfig;
import io.prestosql.spi.queryeditorui.ConnectorUtil;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

@ConnectorConfig(connectorLabel = "Hive: Query data stored in a Hive data warehouse",
        propertiesEnabled = true,
        catalogConfigFilesEnabled = true,
        globalConfigFilesEnabled = true,
        docLink = "https://openlookeng.io/docs/docs/connector/hive.html",
        configLink = "https://openlookeng.io/docs/docs/connector/hive.html#configuration")
public class HivePlugin
        implements Plugin
{
    private final String name;
    private final Optional<HiveMetastore> metastore;

    public HivePlugin()
    {
        this("omnidata-openlookeng", Optional.empty());
    }

    public HivePlugin(String name, Optional<HiveMetastore> metastore)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HiveConnectorFactory(name, HivePlugin.class.getClassLoader(), metastore));
    }

    @Override
    public Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        ConnectorConfig connectorConfig = HivePlugin.class.getAnnotation(ConnectorConfig.class);
        ArrayList<Method> methods = new ArrayList<>();
        methods.addAll(Arrays.asList(StaticMetastoreConfig.class.getDeclaredMethods()));
        methods.addAll(Arrays.asList(HiveConfig.class.getDeclaredMethods()));
        return ConnectorUtil.assembleConnectorProperties(connectorConfig, methods);
    }
}
