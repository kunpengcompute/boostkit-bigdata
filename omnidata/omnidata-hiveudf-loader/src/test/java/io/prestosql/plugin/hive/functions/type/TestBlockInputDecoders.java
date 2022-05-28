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

package io.prestosql.plugin.hive.functions.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.locationtech.jts.util.Assert;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static org.mockito.Mockito.mock;

public class TestBlockInputDecoders
{
    @Test
    public void testCreateBlockInputDecoder()
    {
        Type mockType = mock(Type.class);
        RowType mockRowType = mock(RowType.class);
        ArrayType mockArrayType = mock(ArrayType.class);
        MapType mockMapType = mock(MapType.class);

        // inspector instanceof ConstantObjectInspector
        ConstantObjectInspector mockConstantObjectInspector
                = mock(ConstantObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockConstantObjectInspector, mockType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches("Unsupported Hive type .*",
                    e.getMessage()));
        }
        // inspector instanceof PrimitiveObjectInspector
        PrimitiveObjectInspector mockPrimitiveObjectInspector
                = mock(PrimitiveObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockPrimitiveObjectInspector, mockType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches("Unsupported Hive type .*",
                    e.getMessage()));
        }
        // inspector instanceof StandardStructObjectInspector
        StandardStructObjectInspector mockStandardStructObjectInspector
                = mock(StandardStructObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockStandardStructObjectInspector, mockRowType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
        // inspector instanceof SettableStructObjectInspector
        SettableStructObjectInspector mockSettableStructObjectInspector = mock(
                SettableStructObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockSettableStructObjectInspector, mockRowType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
        // inspector instanceof StructObjectInspector
        StructObjectInspector mockStructObjectInspector = mock(
                StructObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockStructObjectInspector, mockRowType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
        // inspector instanceof ListObjectInspector
        ListObjectInspector mockListObjectInspector = mock(
                ListObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockListObjectInspector, mockArrayType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
        // inspector instanceof MapObjectInspector
        MapObjectInspector mockMapObjectInspector = mock(
                MapObjectInspector.class);
        try {
            BlockInputDecoders.createBlockInputDecoder(
                    mockMapObjectInspector, mockMapType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
        // throw unsupported type
        try {
            BlockInputDecoders.createBlockInputDecoder(null, mockMapType);
        }
        catch (PrestoException e) {
            Assert.isTrue(Pattern.matches(
                    "Unsupported Hive type .*", e.getMessage()));
        }
    }
}
