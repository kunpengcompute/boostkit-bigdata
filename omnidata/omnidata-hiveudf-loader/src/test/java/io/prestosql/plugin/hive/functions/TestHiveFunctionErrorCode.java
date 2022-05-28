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
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_EXECUTION_ERROR;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_INITIALIZATION_ERROR;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.unsupportedNamespace;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;
import static org.mockito.Mockito.mock;

public class TestHiveFunctionErrorCode
{
    private Throwable mockThrowable = mock(Throwable.class);
    private QualifiedObjectName mockQualifiedObjectName = mock(QualifiedObjectName.class);

    @Test
    public void testFunctionNotFound()
    {
        String name = "test";

        PrestoException result = HiveFunctionErrorCode.functionNotFound(name, mock(ClassNotFoundException.class));

        Assert.assertTrue(result.getErrorCode().toString().startsWith(
                FUNCTION_NOT_FOUND.toString()));
        Assert.assertEquals(result.getMessage(),
                format("Function %s not registered. %s", name, "null"));
    }

    @Test
    public void testInitializationError()
    {
        // Throwable t
        PrestoException result =
                HiveFunctionErrorCode.initializationError(mockThrowable);

        Assert.assertEquals(result.getMessage(),
                HIVE_FUNCTION_INITIALIZATION_ERROR.toString());

        // String filePath, Exception e
        Assert.assertEquals(HiveFunctionErrorCode.initializationError("test",
                        mock(Exception.class)).getMessage(),
                "Fail to read the configuration test. null");
    }

    @Test
    public void testExecutionError()
    {
        PrestoException result =
                HiveFunctionErrorCode.executionError(mockThrowable);

        Assert.assertEquals(result.getMessage(),
                HIVE_FUNCTION_EXECUTION_ERROR.toString());
    }

    @Test
    public void testUnsupportedFunctionType() throws ClassNotFoundException
    {
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        Class<?> functionClass = mockHiveFunctionRegistry.getClass(mockQualifiedObjectName);

        try {
            HiveFunctionErrorCode.unsupportedFunctionType(functionClass);
        }
        catch (NullPointerException ignored) {
        }

        try {
            HiveFunctionErrorCode.unsupportedFunctionType(functionClass, mockThrowable);
        }
        catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testUnsupportedNamespace()
    {
        Assert.assertTrue(unsupportedNamespace(mockQualifiedObjectName).toString()
                .contains("Hive udf unsupported namespace null. Its schema should be default."));
    }

    @Test
    public void testInvalidParatemers()
    {
        Assert.assertTrue(Pattern.matches("The input path .* is invalid. .*",
                HiveFunctionErrorCode.invalidParatemers("test", mock(Exception.class)).getMessage()));
    }
}
