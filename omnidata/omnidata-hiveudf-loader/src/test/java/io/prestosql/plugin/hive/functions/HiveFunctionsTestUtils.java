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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.plugin.memory.MemoryPlugin;
import io.prestosql.server.testing.TestingPrestoServer;

import java.util.HashMap;
import java.util.Map;

public final class HiveFunctionsTestUtils
{
    private HiveFunctionsTestUtils()
    {
    }

    public static TestingPrestoServer createTestingPrestoServer()
            throws Exception
    {
        TempFolder folder = new TempFolder().create();
        Runtime.getRuntime().addShutdownHook(new Thread(folder::close));
        HashMap<String, String> metastoreConfig = new HashMap<>();
        metastoreConfig.put("hetu.metastore.type", "hetufilesystem");
        metastoreConfig.put("hetu.metastore.hetufilesystem.profile-name",
                "default");
        metastoreConfig.put("hetu.metastore.hetufilesystem.path",
                folder.newFolder("metastore").getAbsolutePath());

        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new HetuFileSystemClientPlugin());
        server.installPlugin(new MemoryPlugin());
        server.installPlugin(new HetuMetastorePlugin());
        server.installPlugin(new HiveFunctionNamespacePlugin());
        server.loadMetastore(metastoreConfig);

        server.createCatalog("memory", "memory",
                ImmutableMap.of("memory.spill-path",
                        folder.newFolder("memory-connector")
                                .getAbsolutePath()));

        FunctionAndTypeManager functionAndTypeManager =
                server.getInstance(Key.get(FunctionAndTypeManager.class));
        functionAndTypeManager.loadFunctionNamespaceManager(
                new HetuMetaStoreManager(),
                "hive-functions",
                "hive",
                getNamespaceManagerCreationProperties());
        server.refreshNodes();
        return server;
    }

    public static Map<String, String> getNamespaceManagerCreationProperties()
    {
        HashMap<String, String> namespaceManagerCreationPropertie = new HashMap<>();
        namespaceManagerCreationPropertie.put("external-functions.dir", "test/test");
        return namespaceManagerCreationPropertie;
    }
}
