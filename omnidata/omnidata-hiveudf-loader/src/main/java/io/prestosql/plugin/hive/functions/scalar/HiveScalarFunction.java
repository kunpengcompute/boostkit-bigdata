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

import io.prestosql.plugin.hive.functions.HiveFunction;
import io.prestosql.plugin.hive.functions.gen.ScalarMethodHandles;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionImplementationType;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.functions.scalar.HiveScalarFunctionInvoker.createFunctionInvoker;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Objects.requireNonNull;

public class HiveScalarFunction
        extends HiveFunction
{
    private final ScalarFunctionImplementation implementation;
    private final FunctionMetadata functionMetadata;

    private HiveScalarFunction(FunctionMetadata metadata,
                               Signature signature,
                               String description,
                               ScalarFunctionImplementation implementation)
    {
        super(metadata.getName(),
                signature,
                false,
                metadata.isDeterministic(),
                metadata.isCalledOnNullInput(),
                description);

        this.functionMetadata = requireNonNull(metadata, "metadata is null");
        this.implementation = requireNonNull(implementation, "implementation is null");
    }

    public static HiveScalarFunction createHiveScalarFunction(Class<?> cls, QualifiedObjectName name, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        HiveScalarFunctionInvoker invoker = createFunctionInvoker(cls, name, argumentTypes, typeManager);
        MethodHandle methodHandle = ScalarMethodHandles.generateUnbound(invoker.getSignature(), typeManager).bindTo(invoker);
        Signature signature = invoker.getSignature();
        FunctionMetadata functionMetadata = new FunctionMetadata(name,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                SCALAR,
                FunctionImplementationType.BUILTIN,
                true,
                true);
        InvocationConvention invocationConvention = new InvocationConvention(
                signature.getArgumentTypes().stream().map(t -> BOXED_NULLABLE).collect(toImmutableList()),
                NULLABLE_RETURN,
                false);
        ScalarFunctionImplementation implementation = new HiveScalarFunctionImplementation(methodHandle, invocationConvention);
        return new HiveScalarFunction(functionMetadata, signature, name.getObjectName(), implementation);
    }

    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public ScalarFunctionImplementation getJavaScalarFunctionImplementation()
    {
        return implementation;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    private static class HiveScalarFunctionImplementation
            implements ScalarFunctionImplementation
    {
        private final MethodHandle methodHandle;
        private final InvocationConvention invocationConvention;

        private HiveScalarFunctionImplementation(MethodHandle methodHandle, InvocationConvention invocationConvention)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.invocationConvention = requireNonNull(invocationConvention, "invocationConvention is null");
        }

        @Override
        public InvocationConvention getInvocationConvention()
        {
            return invocationConvention;
        }

        @Override
        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        @Override
        public boolean isNullable()
        {
            return getInvocationConvention().getReturnConvention().equals(NULLABLE_RETURN);
        }
    }
}
