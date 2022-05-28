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
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.assertions.Assert;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.regex.Pattern;

import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE;
import static io.prestosql.plugin.hive.functions.type.PrestoTypes.createDecimalType;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BYTE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DATE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.FLOAT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.LONG;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.SHORT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrestoTypes
{
    private TypeManager mockTypeManager = mock(TypeManager.class);

    @Test
    public void testCreateDecimalType()
    {
        TypeSignature mockTypeSignature = mock(TypeSignature.class);

        try {
            createDecimalType(mockTypeSignature);
        }
        catch (IllegalArgumentException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches(
                    "Invalid decimal type .*", e.getMessage()));
        }
    }

    @Test
    public void testFromObjectInspector()
    {
        ObjectInspector mockObjectInspector = mock(ObjectInspector.class);

        //  case PRIMITIVE
        when(mockObjectInspector.getCategory()).thenReturn(PRIMITIVE);
        try {
            PrestoTypes.fromObjectInspector(mockObjectInspector, mockTypeManager);
        }
        catch (IllegalArgumentException ignored) {
        }
        //  case LIST
        when(mockObjectInspector.getCategory()).thenReturn(LIST);
        try {
            PrestoTypes.fromObjectInspector(mockObjectInspector, mockTypeManager);
        }
        catch (IllegalArgumentException ignored) {
        }
        //  case MAP
        when(mockObjectInspector.getCategory()).thenReturn(MAP);
        try {
            PrestoTypes.fromObjectInspector(mockObjectInspector, mockTypeManager);
        }
        catch (IllegalArgumentException ignored) {
        }
        //  case STRUCT
        when(mockObjectInspector.getCategory()).thenReturn(STRUCT);
        try {
            PrestoTypes.fromObjectInspector(mockObjectInspector, mockTypeManager);
        }
        catch (IllegalArgumentException ignored) {
        }

        // throw unsupported type
        when(mockObjectInspector.getCategory()).thenReturn(UNION);
        try {
            PrestoTypes.fromObjectInspector(mockObjectInspector, mockTypeManager);
        }
        catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Hive type .*",
                    e.getMessage()));
        }
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Unsupported Hive type.*")
    public void testFromPrimitive() throws InstantiationException,
            IllegalAccessException, NoSuchMethodException,
            InvocationTargetException
    {
        Constructor<PrestoTypes> prestoTypesConstructor =
                PrestoTypes.class.getDeclaredConstructor();
        prestoTypesConstructor.setAccessible(true);
        Object instance = prestoTypesConstructor.newInstance();
        Class<PrestoTypes> prestoTypesClass = PrestoTypes.class;
        Method method = prestoTypesClass.getDeclaredMethod(
                "fromPrimitive", PrimitiveObjectInspector.class);
        method.setAccessible(true);
        PrimitiveObjectInspector mockPrimitiveObjectInspector =
                mock(PrimitiveObjectInspector.class);

        // case BOOLEAN
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(BOOLEAN);
        Assert.assertEquals(BooleanType.BOOLEAN,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case BYTE
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(BYTE);
        Assert.assertEquals(TinyintType.TINYINT,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case SHORT
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(SHORT);
        Assert.assertEquals(SmallintType.SMALLINT,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case INT
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(INT);
        Assert.assertEquals(IntegerType.INTEGER,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case LONG
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(LONG);
        Assert.assertEquals(BigintType.BIGINT,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case FLOAT
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(FLOAT);
        Assert.assertEquals(RealType.REAL,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case DOUBLE
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(DOUBLE);
        Assert.assertEquals(DoubleType.DOUBLE,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case STRING
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(STRING);
        Assert.assertEquals(VarcharType.VARCHAR,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case DATE
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(DATE);
        Assert.assertEquals(DateType.DATE,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case TIMESTAMP
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(TIMESTAMP);
        Assert.assertEquals(TimestampType.TIMESTAMP,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // case BINARY
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(BINARY);
        Assert.assertEquals(VarbinaryType.VARBINARY,
                method.invoke(instance, mockPrimitiveObjectInspector));
        // throw unsupported type
        when(mockPrimitiveObjectInspector.getPrimitiveCategory())
                .thenReturn(UNKNOWN);
        try {
            method.invoke(instance, mockPrimitiveObjectInspector);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PrestoException) {
                PrestoException ex = (PrestoException) cause;
                throw new PrestoException(HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE,
                        ex.getMessage());
            }
        }
    }

    @Test
    public void testFromList() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, InstantiationException
    {
        Constructor<PrestoTypes> prestoTypesConstructor =
                PrestoTypes.class.getDeclaredConstructor();
        prestoTypesConstructor.setAccessible(true);
        Object instance = prestoTypesConstructor.newInstance();
        Class<PrestoTypes> prestoTypesClass = PrestoTypes.class;
        Method method = prestoTypesClass.getDeclaredMethod(
                "fromList", ListObjectInspector.class, TypeManager.class);
        method.setAccessible(true);

        ListObjectInspector mockListObjectInspector =
                mock(ListObjectInspector.class);

        try {
            method.invoke(instance, mockListObjectInspector, mockTypeManager);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            org.locationtech.jts.util.Assert.isTrue(cause instanceof NullPointerException);
        }
    }

    @Test
    public void testFromMap() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, InstantiationException
    {
        Constructor<PrestoTypes> prestoTypesConstructor =
                PrestoTypes.class.getDeclaredConstructor();
        prestoTypesConstructor.setAccessible(true);
        Object instance = prestoTypesConstructor.newInstance();
        Class<PrestoTypes> prestoTypesClass = PrestoTypes.class;
        Method method = prestoTypesClass.getDeclaredMethod(
                "fromMap", MapObjectInspector.class, TypeManager.class);
        method.setAccessible(true);

        MapObjectInspector mockMapObjectInspector =
                mock(MapObjectInspector.class);

        try {
            method.invoke(instance, mockMapObjectInspector, mockTypeManager);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            org.locationtech.jts.util.Assert.isTrue(cause instanceof NullPointerException);
        }
    }

    @Test
    public void testFromStruct() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, InstantiationException
    {
        Constructor<PrestoTypes> prestoTypesConstructor =
                PrestoTypes.class.getDeclaredConstructor();
        prestoTypesConstructor.setAccessible(true);
        Object instance = prestoTypesConstructor.newInstance();
        Class<PrestoTypes> prestoTypesClass = PrestoTypes.class;
        Method method = prestoTypesClass.getDeclaredMethod(
                "fromStruct", StructObjectInspector.class, TypeManager.class);
        method.setAccessible(true);

        StructObjectInspector mockStructObjectInspector =
                mock(StructObjectInspector.class);

        try {
            method.invoke(instance, mockStructObjectInspector, mockTypeManager);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            org.locationtech.jts.util.Assert.isTrue(cause instanceof IllegalArgumentException);
        }
    }
}
