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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.type.TypeDeserializer;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle
{
    @Test
    public void testRoundTrip()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestMetadataManager())));
        JsonCodec<HiveTableHandle> codec = new JsonCodecFactory(objectMapperProvider, true).jsonCodec(HiveTableHandle.class);

        HiveTableHandle expected = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
        HiveOffloadExpression expression =
                new HiveOffloadExpression(Collections.emptySet(), new InputReferenceExpression(0, SMALLINT), Optional.empty(), OptionalLong.of(5), Collections.emptyMap());
        expected = expected.withOffloadExpression(expression);

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }
}
