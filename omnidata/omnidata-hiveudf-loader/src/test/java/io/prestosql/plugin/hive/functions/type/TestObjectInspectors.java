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
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.UnknownType;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static io.prestosql.client.ClientStandardTypes.ROW;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestObjectInspectors
{
    private TypeManager mockTypeManager = mock(TypeManager.class);

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Unsupported Presto type.*")
    public void testCreate()
    {
        Type mockType = mock(Type.class);
        TypeSignature mockTypeSignature = mock(TypeSignature.class);
        when(mockType.getTypeSignature()).thenReturn(mockTypeSignature);

        // case UnknownType.NAME
        when(mockTypeSignature.getBase()).thenReturn(UnknownType.NAME);
        Assert.assertEquals(javaVoidObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case INTEGER
        when(mockTypeSignature.getBase()).thenReturn(INTEGER);
        Assert.assertEquals(javaIntObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case SMALLINT
        when(mockTypeSignature.getBase()).thenReturn(SMALLINT);
        Assert.assertEquals(javaShortObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case TINYINT
        when(mockTypeSignature.getBase()).thenReturn(TINYINT);
        Assert.assertEquals(javaByteObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case BOOLEAN
        when(mockTypeSignature.getBase()).thenReturn(BOOLEAN);
        Assert.assertEquals(javaBooleanObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case DATE
        when(mockTypeSignature.getBase()).thenReturn(DATE);
        Assert.assertEquals(javaDateObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case DECIMAL
        when(mockTypeSignature.getBase()).thenReturn(DECIMAL);
        try {
            ObjectInspectors.create(mockType, mockTypeManager);
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid decimal type"));
        }
        // case REAL
        when(mockTypeSignature.getBase()).thenReturn(REAL);
        Assert.assertEquals(javaFloatObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case DOUBLE
        when(mockTypeSignature.getBase()).thenReturn(DOUBLE);
        Assert.assertEquals(javaDoubleObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case TIMESTAMP
        when(mockTypeSignature.getBase()).thenReturn(TIMESTAMP);
        Assert.assertEquals(javaTimestampObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case VARBINARY
        when(mockTypeSignature.getBase()).thenReturn(VARBINARY);
        Assert.assertEquals(javaByteArrayObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // case CHAR
        when(mockTypeSignature.getBase()).thenReturn(CHAR);
        Assert.assertEquals(javaStringObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
        // throw unsupported type
        when(mockTypeSignature.getBase()).thenReturn(ROW);
        Assert.assertEquals(javaByteArrayObjectInspector,
                ObjectInspectors.create(mockType, mockTypeManager));
    }

    @Test
    public void testCreateForRow() throws NoSuchMethodException,
            InvocationTargetException, InstantiationException,
            IllegalAccessException
    {
        Constructor<ObjectInspectors> objectInspectorsConstructor =
                ObjectInspectors.class.getDeclaredConstructor();
        objectInspectorsConstructor.setAccessible(true);
        Object instance = objectInspectorsConstructor.newInstance();
        Class<ObjectInspectors> objectInspectorsClass = ObjectInspectors.class;
        Method method = objectInspectorsClass.getDeclaredMethod(
                "createForRow", RowType.class, TypeManager.class);
        method.setAccessible(true);

        RowType mockRowType = mock(RowType.class);

        Object result = method.invoke(instance, mockRowType,
                mockTypeManager).toString();

        Assert.assertEquals("org.apache.hadoop.hive.serde2."
                + "objectinspector.StandardStructObjectInspector<>", result);
    }
}
