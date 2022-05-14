package com.huawei.boostkit.omnidata.metadata;

import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.*;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class InternalTypeManager implements TypeManager {
    public InternalTypeManager(Metadata metadata) {
    }

    public Type getType(TypeSignature signature) {
        return null;
    }

    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters) {
        return null;
    }

    public List<Type> getTypes() {
        return null;
    }

    public Collection<ParametricType> getParametricTypes() {
        return null;
    }

    public Optional<Type> getCommonSuperType(Type firstType, Type secondType) {
        return Optional.empty();
    }

    public boolean canCoerce(Type actualType, Type expectedType) {
        return false;
    }

    public boolean isTypeOnlyCoercion(Type actualType, Type expectedType) {
        return false;
    }

    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase) {
        return Optional.empty();
    }

    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes) {
        return null;
    }
}

