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

package io.prestosql.plugin.hive.functions.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.functions.HiveFunctionRegistry;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionImplementationType;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_INITIALIZATION_ERROR;
import static io.prestosql.plugin.hive.functions.scalar.HiveScalarFunction.createHiveScalarFunction;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static org.mockito.Mockito.mock;

public class TestHiveScalarFunction
{
    private Signature signature;
    private FunctionMetadata functionMetadata;
    private ScalarFunctionImplementation implementation;
    private Object instance;
    private QualifiedObjectName mockQualifiedObjectName = mock(QualifiedObjectName.class);
    private InvocationConvention mockInvocationConvention = mock(InvocationConvention.class);
    private MethodHandle mockMethodHandle = mock(MethodHandle.class);
    Class<HiveScalarFunction> clas;

    @BeforeClass
    public void setup() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        this.signature = new Signature(
            QualifiedObjectName.valueOf("test.test.test"),
            FunctionKind.AGGREGATE,
            DoubleType.DOUBLE.getTypeSignature(),
            ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        this.functionMetadata = new FunctionMetadata(mockQualifiedObjectName,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                SCALAR,
                FunctionImplementationType.BUILTIN,
                true,
                true);

        this.implementation = mock(ScalarFunctionImplementation.class);

        Constructor<HiveScalarFunction> constructor = HiveScalarFunction.class.getDeclaredConstructor(
                FunctionMetadata.class, Signature.class, String.class, ScalarFunctionImplementation.class);
        constructor.setAccessible(true);
        this.instance = constructor.newInstance(functionMetadata, signature, "test", implementation);
        this.clas = HiveScalarFunction.class;
    }

    @Test
    public void testCreateHiveScalarFunction() throws ClassNotFoundException
    {
        QualifiedObjectName name = QualifiedObjectName.valueOf("test.test.test");
        List<TypeSignature> argumentTypes = ImmutableList.of(DoubleType.DOUBLE.getTypeSignature());
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        Class<?> functionClass = mockHiveFunctionRegistry.getClass(mockQualifiedObjectName);

        try {
            createHiveScalarFunction(functionClass, name, argumentTypes, mockTypeManager);
        }
        catch (PrestoException e) {
            Assert.assertEquals(HIVE_FUNCTION_INITIALIZATION_ERROR.toString(), e.getMessage());
        }
    }

    @Test
    public void testGetFunctionMetadata() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException
    {
        Method method = clas.getDeclaredMethod("getFunctionMetadata");

        Assert.assertEquals(functionMetadata, method.invoke(instance));
    }

    @Test
    public void testGetJavaScalarFunctionImplementation() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException
    {
        Method method = clas.getDeclaredMethod("getJavaScalarFunctionImplementation");

        Assert.assertEquals(implementation, method.invoke(instance));
    }

    @Test
    public void testIsHidden() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        Method method = clas.getDeclaredMethod("isHidden");

        Assert.assertEquals(false, method.invoke(instance));
    }

    @Test
    public void testGetInvocationConvention() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveScalarFunction.class.getDeclaredClasses();

        Object object = Optional.empty();
        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("HiveScalarFunctionImplementation")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                for (Constructor declaredConstructor : declaredConstructors) {
                    if (Modifier.toString(declaredConstructor.getModifiers()).contains("private")) {
                        Constructor constructor = declaredConstructor;
                        object = constructor.newInstance(mockMethodHandle, mockInvocationConvention);
                    }
                }
                Method method = c.getMethod("getInvocationConvention");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), mockInvocationConvention);
            }
        }
    }

    @Test
    public void testGetMethodHandle() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveScalarFunction.class.getDeclaredClasses();

        Object object = Optional.empty();
        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("HiveScalarFunctionImplementation")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                for (Constructor declaredConstructor : declaredConstructors) {
                    if (Modifier.toString(declaredConstructor.getModifiers()).contains("private")) {
                        Constructor constructor = declaredConstructor;
                        object = constructor.newInstance(mockMethodHandle, mockInvocationConvention);
                    }
                }
                Method method = c.getMethod("getMethodHandle");
                method.setAccessible(true);

                Assert.assertEquals(method.invoke(object), mockMethodHandle);
            }
        }
    }

    @Test
    public void testIsNullable() throws InvocationTargetException, InstantiationException,
            IllegalAccessException, NoSuchMethodException
    {
        Class<?>[] declaredClasses = HiveScalarFunction.class.getDeclaredClasses();

        Object object = Optional.empty();
        for (Class c : declaredClasses) {
            int mod = c.getModifiers();
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private") && c.getName().contains("HiveScalarFunctionImplementation")) {
                Constructor[] declaredConstructors = c.getDeclaredConstructors();
                AccessibleObject.setAccessible(declaredConstructors, true);

                for (Constructor declaredConstructor : declaredConstructors) {
                    if (Modifier.toString(declaredConstructor.getModifiers()).contains("private")) {
                        Constructor constructor = declaredConstructor;
                        object = constructor.newInstance(mockMethodHandle, mockInvocationConvention);
                    }
                }
                Method method = c.getMethod("isNullable");
                method.setAccessible(true);

                try {
                    method.invoke(object);
                }
                catch (InvocationTargetException e) {
                    Assert.assertTrue(e.getCause() instanceof NullPointerException);
                }
            }
        }
    }
}
