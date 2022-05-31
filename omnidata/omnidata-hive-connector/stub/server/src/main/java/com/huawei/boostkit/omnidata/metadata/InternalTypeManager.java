/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

