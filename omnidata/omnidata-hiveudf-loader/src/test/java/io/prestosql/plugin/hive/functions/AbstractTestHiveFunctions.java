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

import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.tests.TestingPrestoClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.util.Optional;

import static io.prestosql.plugin.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractTestHiveFunctions
{
    private static final Logger log = Logger
            .get(AbstractTestHiveFunctions.class);

    protected TestingPrestoServer server;
    protected TestingPrestoClient client;
    protected TypeManager typeManager;
    protected ClassLoader classLoader;

    @BeforeClass
    public void setup()
            throws Exception
    {
        // TODO: Use DistributedQueryRunner to perform query
        server = createTestingPrestoServer();
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(
                        "America/Bahia_Banderas")).build());
        typeManager = server.getInstance(Key.get(TypeManager.class));
        classLoader = Thread.currentThread().getContextClassLoader();

        if (getInitScript().isPresent()) {
            String sql = Files.asCharSource(
                    getInitScript().get(), UTF_8).read();
            Iterable<String> initQueries = Splitter.on("----\n")
                    .omitEmptyStrings().trimResults().split(sql);
            for (@Language("SQL") String query : initQueries) {
                log.debug("Executing %s", query);
                client.execute(query);
            }
        }
    }

    protected Optional<File> getInitScript()
    {
        return Optional.empty();
    }

    public static class Column
    {
        private final Type type;

        private final Object[] values;

        private Column(Type type, Object[] values)
        {
            this.type = type;
            this.values = values;
        }

        public static Column of(Type type, Object... values)
        {
            return new Column(type, values);
        }
    }
}
