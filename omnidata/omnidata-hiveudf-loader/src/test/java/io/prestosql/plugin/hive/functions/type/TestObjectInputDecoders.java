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

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import io.airlift.slice.Slices;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TestRowType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.testing.assertions.Assert;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

import static io.prestosql.plugin.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static io.prestosql.plugin.hive.functions.type.ObjectInputDecoders.createDecoder;
import static io.prestosql.plugin.hive.functions.type.TestObjectEncoders.BYTES;
import static io.prestosql.plugin.hive.functions.type.TestObjectEncoders.PRECIS_LONG_VAL;
import static io.prestosql.plugin.hive.functions.type.TestObjectEncoders.PRECIS_SLICE_VAL;
import static io.prestosql.plugin.hive.functions.type.TestObjectEncoders.SCALE_LONG_VAL;
import static io.prestosql.plugin.hive.functions.type.TestObjectEncoders.SCALE_SLICE_VAL;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.parseIncludeLeadingZerosInPrecision;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestObjectInputDecoders
{
    private TestingPrestoServer server;
    private TypeManager typeManager;

    private static final long ACTUAL_DAYS = 18380L;
    private static final int EXPECTED_YEAR = 2020 - 1900;
    private static final int EXPECTED_MONTH = 4 - 1;
    private static final long BIGINT_VAL = 123456L;
    private static final long INTEGER_VAL = 12345L;
    private static final long SMALLINT_VAL = 1234L;
    private static final long TINYINT_VAL = 123L;
    private static final double REAL_VAL = 0.2;
    private static final double DOUBLE_VAL = 0.1;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void testToDate()
    {
        Date date = DateTimeUtils.createDate(ACTUAL_DAYS);
        assertEquals(date.getYear(), EXPECTED_YEAR);
        assertEquals(date.getMonth(), EXPECTED_MONTH);
    }

    @Test
    public void testPrimitiveObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(BIGINT, typeManager);
        assertTrue(decoder.decode(BIGINT_VAL) instanceof Long);

        decoder = createDecoder(INTEGER, typeManager);
        assertTrue(decoder.decode(INTEGER_VAL) instanceof Integer);

        decoder = createDecoder(SMALLINT, typeManager);
        assertTrue(decoder.decode(SMALLINT_VAL) instanceof Short);

        decoder = createDecoder(TINYINT, typeManager);
        assertTrue(decoder.decode(TINYINT_VAL) instanceof Byte);

        decoder = createDecoder(BOOLEAN, typeManager);
        assertTrue(decoder.decode(true) instanceof Boolean);

        decoder = createDecoder(REAL, typeManager);
        assertTrue(decoder.decode(((float) REAL_VAL)) instanceof Float);

        decoder = createDecoder(DOUBLE, typeManager);
        assertTrue(decoder.decode(DOUBLE_VAL) instanceof Double);
    }

    @Test
    public void testDecimalObjectDecoders()
    {
        ObjectInputDecoder decoder;

        // short decimal
        decoder = createDecoder(createDecimalType(
                PRECIS_LONG_VAL, SCALE_LONG_VAL), typeManager);
        assertTrue(
                decoder.decode(decimal("1.2345678910")) instanceof HiveDecimal);

        // long decimal
        decoder = createDecoder(createDecimalType(
                PRECIS_SLICE_VAL, SCALE_SLICE_VAL), typeManager);
        assertTrue(decoder.decode(decimal(
                "1.281734081274028174012432412423134")) instanceof HiveDecimal);
    }

    @Test
    public void testSliceObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(VARBINARY, typeManager);
        assertTrue(
                decoder.decode(Slices.wrappedBuffer(BYTES)) instanceof byte[]);

        decoder = createDecoder(VARCHAR, typeManager);
        assertTrue(decoder.decode(Slices.utf8Slice(
                "test_varchar")) instanceof String);

        decoder = createDecoder(createCharType(SCALE_LONG_VAL), typeManager);
        assertTrue(decoder.decode(Slices.utf8Slice(
                "test_char")) instanceof String);
    }

    @Test
    public void testBlockObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(new ArrayType(BIGINT), typeManager);
        assertTrue(
                decoder instanceof ObjectInputDecoders.ArrayObjectInputDecoder);
        assertEquals(((ArrayList) decoder.decode(
                createLongArrayBlock())).get(0), 2L);

        decoder = createDecoder(new MapType(
                BIGINT,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class,
                        "throwUnsupportedOperation")), typeManager);
        assertTrue(
                decoder instanceof ObjectInputDecoders.MapObjectInputDecoder);
        HashMap map = (HashMap) decoder.decode(createLongArrayBlock());
        assertEquals(map.get(2L), 1L);
    }

    private Block createLongArrayBlock()
    {
        return new LongArrayBlock(2, Optional.empty(), new long[]{2L, 1L});
    }

    private Object decimal(final String decimalString)
    {
        return parseIncludeLeadingZerosInPrecision(decimalString).getObject();
    }

    @Test
    public void testRowObjectInputDecoder() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = ObjectInputDecoders.class.getDeclaredClasses();

        for (Class c : declaredClasses) {
            if (c.getName().contains("RowObjectInputDecoder")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                Object object = constructor.newInstance(ImmutableList.of(mock(BlockInputDecoder.class)));
                Method method = c.getMethod("decode", Object.class);
                method.setAccessible(true);

                Assert.assertNull(method.invoke(object, (Object) null));
            }
        }
    }
}
