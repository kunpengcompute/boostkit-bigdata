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
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.FunctionNamespaceTransactionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHiveFunctionNamespaceManager
{
    private QualifiedObjectName mockQualifiedObjectName = mock(QualifiedObjectName.class);
    private BuiltInFunctionHandle mockBuiltInFunctionHandle = mock(BuiltInFunctionHandle.class);
    private TypeManager mockTypeManager = mock(TypeManager.class);
    private HiveFunctionNamespaceManager hiveFunctionNamespaceManager;

    @BeforeClass
    public void setup()
    {
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager =
                mock(HiveFunctionNamespacePlugin.class);
        HiveFunctionRegistry mockHiveFunctionRegistry =
                mock(HiveFunctionRegistry.class);
        ClassLoader mockClassLoader =
                mockHiveFunctionNamespaceManager.getClass().getClassLoader();

        this.hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager("hive",
                mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);
    }

    @Test
    public void testBeginTransaction()
    {
        Assert.assertTrue(
                hiveFunctionNamespaceManager.beginTransaction().toString().contains("EmptyTransactionHandle"));
    }

    @Test
    public void testCommit()
    {
        // Null function
        hiveFunctionNamespaceManager.commit(mock(FunctionNamespaceTransactionHandle.class));
    }

    @Test
    public void testAbort()
    {
        // Null function
        hiveFunctionNamespaceManager.abort(mock(FunctionNamespaceTransactionHandle.class));
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Cannot create function in hive"
                    + " function namespace: test.test.test")
    public void testCreateFunction()
    {
        SqlInvokedFunction mockSqlInvokedFunction =
                mock(SqlInvokedFunction.class);
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.test.test"),
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));
        when(mockSqlInvokedFunction.getSignature()).thenReturn(signature);

        hiveFunctionNamespaceManager.createFunction(
                mockSqlInvokedFunction, false);
    }

    @Test
    public void testListFunctions()
    {
        Collection<HiveFunction> result =
                hiveFunctionNamespaceManager.listFunctions();

        assertEquals(0, result.size());
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Get function is not supported")
    public void testGetFunctions()
    {
        hiveFunctionNamespaceManager.getFunctions(Optional.empty(),
                mockQualifiedObjectName);
    }

    @Test
    public void testGetFunctionHandle()
    {
        TypeSignature mockReturnType = mock(TypeSignature.class);
        List<TypeSignature> argumentTypes = new ArrayList<>();
        Signature signature = new Signature(mockQualifiedObjectName, SCALAR, emptyList(),
                emptyList(), mockReturnType, argumentTypes, false);

        Assert.assertEquals(hiveFunctionNamespaceManager.getFunctionHandle(
                Optional.empty(), signature),
                new BuiltInFunctionHandle(signature));
    }

    @Test
    public void testCanResolveFunction()
    {
        Assert.assertTrue(hiveFunctionNamespaceManager.canResolveFunction());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testGetFunctionMetadata()
    {
        hiveFunctionNamespaceManager.getFunctionMetadata(mockBuiltInFunctionHandle);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testGetScalarFunctionImplementation()
    {
        hiveFunctionNamespaceManager.getScalarFunctionImplementation(mockBuiltInFunctionHandle);
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp =
                    "Execute function is not supported")
    public void testExecuteFunction()
    {
        FunctionHandle mockFunctionHandle = mock(FunctionHandle.class);
        Page input = new Page(1);
        List<Integer> channels = new ArrayList<>();

        hiveFunctionNamespaceManager.executeFunction(
                mockFunctionHandle, input, channels, mockTypeManager);
    }

    @Test
    public void testResolveFunction()
    {
        try {
            hiveFunctionNamespaceManager.resolveFunction(Optional.empty(),
                    QualifiedObjectName.valueOf("test.test.test"),
                    ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));
        }
        catch (PrestoException e) {
            Assert.assertEquals(e.getMessage(),
                    "Hive udf unsupported namespace test.test. Its schema should be default.");
        }

        try {
            hiveFunctionNamespaceManager.resolveFunction(Optional.empty(),
                    QualifiedObjectName.valueOf("test.default.test"),
                    ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));
        }
        catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testCreateDummyHiveScalarFunction() throws IllegalAccessException,
            NoSuchMethodException, InvocationTargetException
    {
        Class<HiveFunctionNamespaceManager> clas = HiveFunctionNamespaceManager.class;
        Method method = clas.getDeclaredMethod("createDummyHiveScalarFunction", String.class);
        method.setAccessible(true);

        Assert.assertTrue(method.invoke(hiveFunctionNamespaceManager, "test")
                .toString().contains("DummyHiveScalarFunction"));
    }

    @Test
    public void testInnerGetFunctionMetadata() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.test.test"),
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("DummyHiveScalarFunction")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                Object object = constructor.newInstance(signature);
                Method method = c.getMethod("getFunctionMetadata");
                method.setAccessible(true);

                try {
                    method.invoke(object);
                }
                catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IllegalStateException) {
                        Assert.assertEquals(cause.getMessage(), "Get function metadata is not supported");
                    }
                }
            }
        }
    }

    @Test
    public void testGetName() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("FunctionKey")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
                Object object = constructor.newInstance(name, argumentTypes);
                Method method = c.getMethod("getName");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), name);
            }
        }
    }

    @Test
    public void testGetArgumentTypes() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("FunctionKey")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
                Object object = constructor.newInstance(name, argumentTypes);
                Method method = c.getMethod("getArgumentTypes");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), argumentTypes);
            }
        }
    }

    @Test
    public void testHashCode() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("FunctionKey")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
                Object object = constructor.newInstance(name, argumentTypes);
                Method method = c.getMethod("hashCode");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), Objects.hash(name, argumentTypes));
            }
        }
    }

    @Test
    public void testToString() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("FunctionKey")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
                Object object = constructor.newInstance(name, argumentTypes);
                Method method = c.getMethod("toString");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), "FunctionKey{name=test.test.test, arguments=[double]}");
            }
        }
    }

    @Test
    public void testEquals() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveFunctionNamespaceManager.class.getDeclaredClasses();
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");

        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("FunctionKey")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                Constructor constructor = declaredConstructors[0];
                List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
                Object object = constructor.newInstance(name, argumentTypes);
                Method method = c.getMethod("equals", Object.class);
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object, name.getSchemaName()), false);

                method = c.getMethod("equals", Object.class);
                method.setAccessible(true);
                Assert.assertEquals(method.invoke(object, object), true);
            }
        }
    }
}
