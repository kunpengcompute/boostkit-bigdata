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
package io.prestosql.plugin.hive.functions;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;

public class HiveFunctionNamespacePlugin
        implements Plugin
{
    @Override
    public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return ImmutableList.of(new HiveFunctionNamespaceManagerFactory(getClassLoader()));
    }

    private ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HiveFunctionNamespacePlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
