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

import io.airlift.log.Logger;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import java.util.Set;

public final class FunctionRegistry
{
    private static final Logger log = Logger.get(FunctionRegistry.class);

    private FunctionRegistry() {}

    // registry for hive functions
    private static final Registry system = new Registry(true);

    public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException
    {
        return system.getFunctionInfo(functionName);
    }

    public static Set<String> getCurrentFunctionNames()
    {
        return system.getCurrentFunctionNames();
    }

    public static void addFunction(String functionName, Class<?> cls)
    {
        if (GenericUDF.class.isAssignableFrom(cls)) {
            system.registerGenericUDF(functionName, (Class<? extends GenericUDF>) cls);
        }
        else if (UDF.class.isAssignableFrom(cls)) {
            system.registerUDF(functionName, (Class<? extends UDF>) cls, false);
        }
        else {
            log.warn("Function %s is not extended from GenericUDF or UDF, please check.", functionName);
            return;
        }
        log.info("Registry function %s successfully.", functionName);
    }
}
