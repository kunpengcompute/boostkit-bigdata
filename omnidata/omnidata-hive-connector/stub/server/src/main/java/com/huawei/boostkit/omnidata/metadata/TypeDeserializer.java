package com.huawei.boostkit.omnidata.metadata;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.io.IOException;

public class TypeDeserializer extends FromStringDeserializer<Type> {
    public TypeDeserializer(TypeManager typeManager) {
        super(Type.class);
    }

    @Override
    protected Type _deserialize(String s, DeserializationContext deserializationContext) throws IOException {
        return null;
    }
}

