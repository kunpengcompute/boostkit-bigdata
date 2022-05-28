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

import com.google.inject.Key;
import io.airlift.slice.Slice;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TestRowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.testing.assertions.Assert;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import static io.prestosql.plugin.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static io.prestosql.plugin.hive.functions.type.ObjectEncoders.createEncoder;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.GEOMETRY;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.UUID;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestObjectEncoders
{
    private TestingPrestoServer server;
    private TypeManager typeManager;

    private static final long LONG_VAL = 123456L;
    public static final long THREE_LONG_VAL = 3L;
    private static final int INT_VAL = 12345;
    private static final short SHORT_VAL = 1234;
    private static final byte BYTE_VAL = 123;
    private static final double DOUBLE_VAL = 0.1;
    static final int PRECIS_LONG_VAL = 11;
    static final int SCALE_LONG_VAL = 10;
    static final int PRECIS_SLICE_VAL = 34;
    static final int SCALE_SLICE_VAL = 33;
    static final byte[] BYTES = new byte[]{12, 34, 56};

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void testPrimitiveObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = writableLongObjectInspector;
        encoder = createEncoder(BIGINT, inspector);
        assertTrue(encoder.encode(
                new LongWritable(LONG_VAL)) instanceof Long);

        inspector = writableIntObjectInspector;
        encoder = createEncoder(INTEGER, inspector);
        assertTrue(encoder.encode(
                new IntWritable(INT_VAL)) instanceof Long);

        inspector = writableShortObjectInspector;
        encoder = createEncoder(SMALLINT, inspector);
        assertTrue(encoder.encode(
                new ShortWritable(SHORT_VAL)) instanceof Long);

        inspector = writableByteObjectInspector;
        encoder = createEncoder(TINYINT, inspector);
        assertTrue(encoder.encode(
                new ByteWritable(BYTE_VAL)) instanceof Long);

        inspector = writableBooleanObjectInspector;
        encoder = createEncoder(BOOLEAN, inspector);
        assertTrue(encoder.encode(
                new BooleanWritable(true)) instanceof Boolean);

        inspector = writableDoubleObjectInspector;
        encoder = createEncoder(DOUBLE, inspector);
        assertTrue(encoder.encode(
                new DoubleWritable(DOUBLE_VAL)) instanceof Double);

        inspector = writableHiveDecimalObjectInspector;
        encoder = createEncoder(createDecimalType(PRECIS_LONG_VAL,
                SCALE_LONG_VAL), inspector);
        assertTrue(encoder.encode(
                new HiveDecimalWritable("1.2345678910")) instanceof Long);

        encoder = createEncoder(createDecimalType(PRECIS_SLICE_VAL,
                SCALE_SLICE_VAL), inspector);
        assertTrue(encoder.encode(
                new HiveDecimalWritable("1.281734081274028174012432412423134"))
                instanceof Slice);
    }

    @Test
    public void testTextObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = writableBinaryObjectInspector;
        encoder = createEncoder(VARBINARY, inspector);
        assertTrue(encoder.encode(
                new BytesWritable(BYTES)) instanceof Slice);

        inspector = writableStringObjectInspector;
        encoder = createEncoder(VARCHAR, inspector);
        assertTrue(encoder.encode(
                new Text("test_varchar")) instanceof Slice);

        inspector = writableStringObjectInspector;
        encoder = createEncoder(createCharType(SCALE_LONG_VAL), inspector);
        assertTrue(encoder.encode(
                new Text("test_char")) instanceof Slice);
    }

    @Test
    public void testComplexObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = ObjectInspectors.create(new ArrayType(BIGINT), typeManager);
        encoder = createEncoder(new ArrayType(BIGINT), inspector);
        assertTrue(encoder instanceof ObjectEncoders.ListObjectEncoder);
        Object arrayObject = encoder.encode(new Long[]{1L, 2L, THREE_LONG_VAL});
        assertTrue(arrayObject instanceof LongArrayBlock);
        assertEquals(((LongArrayBlock) arrayObject).getLong(0, 0), 1L);
        assertEquals(((LongArrayBlock) arrayObject).getLong(1, 0), 2L);
        assertEquals(((LongArrayBlock) arrayObject).getLong(2, 0),
                THREE_LONG_VAL);

        inspector = ObjectInspectors.create(new MapType(
                VARCHAR,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation")),
                typeManager);
        encoder = createEncoder(new MapType(
                VARCHAR,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class,
                        "throwUnsupportedOperation")), inspector);
        assertTrue(encoder instanceof ObjectEncoders.MapObjectEncoder);
        assertTrue(encoder.encode(new HashMap<String, Long>() {
        }) instanceof SingleMapBlock);
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Unsupported Presto type.*")
    public void testCreateDecoder()
    {
        Type mockType = mock(Type.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        TypeSignature mockTypeSignature = mock(TypeSignature.class);
        when(mockType.getTypeSignature()).thenReturn(mockTypeSignature);
        // throw unsupported type
        when(mockTypeSignature.getBase()).thenReturn(UUID);

        ObjectInputDecoders.createDecoder(mockType, mockTypeManager);
    }

    @Test
    public void testCreateEncoder()
    {
        Type mockType = mock(Type.class);
        ObjectInspector mockObjectInspector = mock(ObjectInspector.class);
        TypeSignature mockTypeSignature = mock(TypeSignature.class);
        when(mockType.getTypeSignature()).thenReturn(mockTypeSignature);

        // case DATE
        when(mockTypeSignature.getBase()).thenReturn(DATE);
        try {
            createEncoder(mockType, mockObjectInspector);
        }
        catch (IllegalArgumentException ignored) {
        }
        // case REAL
        when(mockTypeSignature.getBase()).thenReturn(REAL);
        try {
            createEncoder(mockType, mockObjectInspector);
        }
        catch (IllegalArgumentException ignored) {
        }
        // case TIMESTAMP
        when(mockTypeSignature.getBase()).thenReturn(TIMESTAMP);
        try {
            createEncoder(mockType, mockObjectInspector);
        }
        catch (IllegalArgumentException ignored) {
        }
        // case ROW
        when(mockTypeSignature.getBase()).thenReturn(ROW);
        try {
            createEncoder(mockType, mockObjectInspector);
        }
        catch (IllegalArgumentException ignored) {
        }
        // throw unsupported type
        when(mockTypeSignature.getBase()).thenReturn(GEOMETRY);
        try {
            createEncoder(mockType, mockObjectInspector);
        }
        catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches(
                    "Unsupported Presto type .*", e.getMessage()));
        }
    }

    @Test
    public void testEncode() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        List<RowType.Field> fields = asList(
                RowType.field("bool_col", BOOLEAN),
                RowType.field("double_col", DOUBLE),
                RowType.field("array_col", new ArrayType(VARCHAR)));
        RowType rowType = RowType.from(fields);

        Class<?>[] declaredClasses = ObjectEncoders.class.getDeclaredClasses();

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("public") && c.getName().contains("StructObjectEncoder")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                Object object = constructor.newInstance(rowType, mock(StructObjectInspector.class));
                Method method = c.getMethod("encode", Object.class);
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object, (Object) null), null);
                try {
                    method.invoke(object, new Text("test_char"));
                }
                catch (InvocationTargetException e) {
                    Assert.assertTrue(e.getCause() instanceof IllegalStateException);
                }
            }
        }
    }

    @Test
    public void testCreate() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        List<RowType.Field> fields = asList(
                RowType.field("bool_col", BOOLEAN),
                RowType.field("double_col", DOUBLE),
                RowType.field("array_col", new ArrayType(VARCHAR)));
        RowType rowType = RowType.from(fields);

        Class<?>[] declaredClasses = ObjectEncoders.class.getDeclaredClasses();

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("public") && c.getName().contains("StructObjectEncoder")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                Object object = constructor.newInstance(rowType, mock(StructObjectInspector.class));
                Method method = c.getMethod("create", Type.class, Object.class);
                method.setAccessible(true);
            }
        }
    }
}
