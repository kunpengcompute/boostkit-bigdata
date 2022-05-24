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

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static java.util.Objects.requireNonNull;

public final class PrestoTypes
{
    private PrestoTypes() {}

    public static DecimalType createDecimalType(TypeSignature type)
    {
        requireNonNull(type);
        checkArgument(DECIMAL.equals(type.getBase()) && type.getParameters().size() == 2,
                "Invalid decimal type " + type);
        int precision = type.getParameters().get(0).getLongLiteral().intValue();
        int scale = type.getParameters().get(1).getLongLiteral().intValue();
        return DecimalType.createDecimalType(precision, scale);
    }

    public static Type fromObjectInspector(ObjectInspector inspector, TypeManager typeManager)
    {
        if (null == inspector) {
            throw new NullPointerException("inspector is null.");
        }
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return fromPrimitive((PrimitiveObjectInspector) inspector);
            case LIST:
                checkArgument(inspector instanceof ListObjectInspector);
                return fromList(((ListObjectInspector) inspector), typeManager);
            case MAP:
                checkArgument(inspector instanceof MapObjectInspector);
                return fromMap(((MapObjectInspector) inspector), typeManager);
            case STRUCT:
                checkArgument(inspector instanceof StructObjectInspector);
                return fromStruct(((StructObjectInspector) inspector), typeManager);
        }
        throw unsupportedType(inspector);
    }

    private static Type fromPrimitive(PrimitiveObjectInspector inspector)
    {
        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BYTE:
                return TinyintType.TINYINT;
            case SHORT:
                return SmallintType.SMALLINT;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case BINARY:
                return VarbinaryType.VARBINARY;
        }
        PrimitiveTypeInfo typeInfo = inspector.getTypeInfo();
        if (typeInfo instanceof CharTypeInfo) {
            int length = ((CharTypeInfo) typeInfo).getLength();
            return CharType.createCharType(length);
        }
        else if (typeInfo instanceof VarcharTypeInfo) {
            int length = ((VarcharTypeInfo) typeInfo).getLength();
            return VarcharType.createVarcharType(length);
        }
        else if (typeInfo instanceof DecimalTypeInfo) {
            final DecimalTypeInfo decimal = (DecimalTypeInfo) typeInfo;
            return DecimalType.createDecimalType(decimal.getPrecision(),
                    decimal.getScale());
        }
        throw unsupportedType(inspector);
    }

    private static Type fromList(ListObjectInspector inspector, TypeManager typeManager)
    {
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        return new ArrayType(fromObjectInspector(elementInspector, typeManager));
    }

    private static Type fromMap(MapObjectInspector inspector, TypeManager typeManager)
    {
        Type keyType = fromObjectInspector(inspector.getMapKeyObjectInspector(), typeManager);
        Type valueType = fromObjectInspector(inspector.getMapValueObjectInspector(), typeManager);
        return typeManager.getType(new TypeSignature(StandardTypes.MAP,
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    private static Type fromStruct(StructObjectInspector inspector, TypeManager typeManager)
    {
        List<RowType.Field> fields = ((StructObjectInspector) inspector)
                .getAllStructFieldRefs()
                .stream()
                .map(sf -> RowType.field(sf.getFieldName(), fromObjectInspector(sf.getFieldObjectInspector(), typeManager)))
                .collect(Collectors.toList());
        return RowType.from(fields);
    }
}
