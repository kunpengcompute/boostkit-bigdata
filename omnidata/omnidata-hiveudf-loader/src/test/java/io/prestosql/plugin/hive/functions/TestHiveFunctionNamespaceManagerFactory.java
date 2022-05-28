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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.assertions.Assert;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.math.DoubleMath.fuzzyEquals;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("UnknownLanguage")
public class TestHiveFunctionNamespaceManagerFactory
        extends AbstractTestHiveFunctions
{
    private static final String FUNCTION_PREFIX = "hive.default.";
    private static final String TABLE_NAME = "memory.default.function_testing";

    @Override
    protected Optional<File> getInitScript()
    {
        return Optional.of(new File("src/test/sql/function-testing.sql"));
    }

    private static void assertNaN(Object o)
    {
        if (o instanceof Double) {
            assertEquals(o, Double.NaN);
        }
        else if (o instanceof Float) {
            assertEquals((Float) o, Float.NaN);
        }
        else {
            fail("Unexpected " + o);
        }
    }

    private void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);

        if (expectedType.equals(DOUBLE) || expectedType.equals(RealType.REAL)) {
            if (expectedValue == null) {
                assertNaN(actual);
            }
            else {
                assertTrue(fuzzyEquals(((Number) actual).doubleValue(), ((Number) expectedValue).doubleValue(), 0.000001));
            }
        }
        else {
            assertEquals(actual, expectedValue);
        }
    }

    private static String selectF(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")").append(" FROM ").append(TABLE_NAME);
        return builder.toString();
    }

    @Test
    public void testCreate()
    {
        FunctionNamespaceManagerContext mockFunctionNamespaceManagerContext =
                mock(FunctionNamespaceManagerContext.class);
        HiveFunctionNamespaceManagerFactory instance = new HiveFunctionNamespaceManagerFactory(classLoader);
        Map<String, String> config = new HashMap<String, String>();
        try {
            instance.create("test", config, mockFunctionNamespaceManagerContext);
        }
        catch (PrestoException e) {
            assertTrue(Pattern.matches("The configuration .* should contain the parameter " +
                    "external-functions.dir.", e.getMessage()));
        }
        config.put("external-functions.dir", "false");
        config.put("test", "test");

        try {
            instance.create("test", config, mockFunctionNamespaceManagerContext);
        }
        catch (PrestoException e) {
            assertTrue(Pattern.matches("The input path .* is invalid.", e.getMessage()));
        }

        // Test registration, loading and calling UDF
        config.clear();
        config.put("external-functions.dir", System.getProperty("user.dir") + File.separatorChar + "src/test/resources/");
        config.put("UdfTest", "com.test.udf.UdfTest");

        when(mockFunctionNamespaceManagerContext.getTypeManager()).thenReturn(Optional.of(typeManager));
        instance.create("hive", config, mockFunctionNamespaceManagerContext);

        check(selectF("UdfTest", "c_varchar"), VARCHAR, "UdfTest varchar");
    }

    @Test
    public void testGetURLs() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException
    {
        Constructor<HiveFunctionNamespaceManagerFactory> constructor =
                HiveFunctionNamespaceManagerFactory.class.getDeclaredConstructor(ClassLoader.class);
        Object instance = constructor.newInstance(classLoader);

        Class<HiveFunctionNamespaceManagerFactory> clas =
                HiveFunctionNamespaceManagerFactory.class;
        Method method = clas.getDeclaredMethod("getURLs", String.class, List.class);
        method.setAccessible(true);

        Assert.assertNull(method.invoke(instance, HiveFunctionNamespaceManagerFactory.NAME, null));
        Assert.assertNull(method.invoke(instance, "etc/null-dir", null));
    }
}
