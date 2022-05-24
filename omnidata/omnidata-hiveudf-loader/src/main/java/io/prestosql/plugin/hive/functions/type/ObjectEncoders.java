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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DuplicateMapKeyException;
import io.prestosql.spi.block.MapBlockBuilder;
import io.prestosql.spi.block.RowBlockBuilder;
import io.prestosql.spi.block.SingleRowBlockWriter;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public final class ObjectEncoders
{
    private ObjectEncoders() {}

    public static ObjectEncoder createEncoder(Type type, ObjectInspector inspector)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case BIGINT:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Long) o));
            case INTEGER:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Integer) o).longValue());
            case SMALLINT:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Short) o).longValue());
            case TINYINT:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Byte) o).longValue());
            case BOOLEAN:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Boolean) o));
            case DATE:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Date) o).getTime());
            case DECIMAL:
                if (Decimals.isShortDecimal(type)) {
                    DecimalType decimalType = (DecimalType) type;
                    return compose(decimal(inspector), o -> DecimalUtils.encodeToLong((BigDecimal) o, decimalType));
                }
                else if (Decimals.isLongDecimal(type)) {
                    DecimalType decimalType = (DecimalType) type;
                    return compose(decimal(inspector), o -> DecimalUtils.encodeToSlice((BigDecimal) o, decimalType));
                }
                break;
            case REAL:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> floatToRawIntBits(((Number) o).floatValue()));
            case DOUBLE:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> (Double) o);
            case TIMESTAMP:
                checkArgument(inspector instanceof PrimitiveObjectInspector);
                return compose(primitive(inspector), o -> ((Timestamp) o).getTime());
            case VARBINARY:
                if (inspector instanceof BinaryObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.wrappedBuffer(((byte[]) o)));
                }
                break;
            case VARCHAR:
                if (inspector instanceof StringObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.utf8Slice(o.toString()));
                }
                else if (inspector instanceof HiveVarcharObjectInspector) {
                    return compose(o -> ((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(o).getValue(),
                            o -> Slices.utf8Slice(((String) o)));
                }
                break;
            case CHAR:
                if (inspector instanceof StringObjectInspector) {
                    return compose(primitive(inspector), o -> Slices.utf8Slice(o.toString()));
                }
                else if (inspector instanceof HiveCharObjectInspector) {
                    return compose(o -> ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(o).getValue(),
                            o -> Slices.utf8Slice(((String) o)));
                }
                break;
            case ROW:
                return StructObjectEncoder.create(type, inspector);
            case ARRAY:
                return ListObjectEncoder.create(type, inspector);
            case MAP:
                return MapObjectEncoder.create(type, inspector);
        }
        throw unsupportedType(type);
    }

    private static ObjectEncoder compose(Function<Object, Object> inspector, Function<Object, Object> encoder)
    {
        return o -> {
            if (o != null) {
                Object inspected = inspector.apply(o);
                if (inspected != null) {
                    return encoder.apply(inspected);
                }
            }
            return null;
        };
    }

    private static Function<Object, Object> primitive(ObjectInspector inspector)
    {
        return ((PrimitiveObjectInspector) inspector)::getPrimitiveJavaObject;
    }

    private static Function<Object, Object> decimal(ObjectInspector inspector)
    {
        return o -> ((HiveDecimalObjectInspector) inspector).getPrimitiveJavaObject(o).bigDecimalValue();
    }

    public static class ListObjectEncoder
            implements ObjectEncoder
    {
        private final ListObjectInspector listInspector;
        private final Type elementType;
        private final BlockObjectWriter writer;

        public static ListObjectEncoder create(Type type, ObjectInspector inspector)
        {
            checkArgument(inspector instanceof ListObjectInspector && type instanceof ArrayType);

            Type elementType = ((ArrayType) type).getElementType();
            ListObjectInspector listInspector = (ListObjectInspector) inspector;
            ObjectEncoder elementEncoder = createEncoder(elementType,
                    listInspector.getListElementObjectInspector());
            return new ListObjectEncoder(listInspector, elementType, elementEncoder);
        }

        private ListObjectEncoder(ListObjectInspector listInspector, Type elementType, ObjectEncoder elementEncoder)
        {
            this.listInspector = requireNonNull(listInspector, "listInspector is null");
            this.elementType = requireNonNull(elementType, "elementType is null");
            this.writer = new SimpleBlockObjectWriter(elementEncoder, elementType);
        }

        @Override
        public Object encode(Object o)
        {
            if (o == null) {
                return null;
            }
            final int length = listInspector.getListLength(o);
            final BlockBuilder blockBuilder = elementType.createBlockBuilder(null, length);
            for (int i = 0; i < length; i++) {
                writer.write(blockBuilder, listInspector.getListElement(o, i));
            }
            return blockBuilder.build();
        }
    }

    public static class MapObjectEncoder
            implements ObjectEncoder
    {
        private final MapType mapType;
        private final MapObjectInspector mapObjectInspector;
        private final BlockObjectWriter keyWriter;
        private final BlockObjectWriter valueWriter;

        public static MapObjectEncoder create(Type type, Object inspector)
        {
            checkArgument(type instanceof MapType &&
                    inspector instanceof MapObjectInspector);
            return new MapObjectEncoder(((MapType) type), ((MapObjectInspector) inspector));
        }

        private MapObjectEncoder(MapType type, MapObjectInspector inspector)
        {
            this.mapType = requireNonNull(type, "mapType is null");
            this.mapObjectInspector = requireNonNull(inspector, "inspector is null");
            Type keyType = type.getKeyType();
            Type valueType = type.getValueType();
            ObjectEncoder keyEncoder = createEncoder(keyType, inspector.getMapKeyObjectInspector());
            ObjectEncoder valueEncoder = createEncoder(valueType, inspector.getMapValueObjectInspector());
            this.keyWriter = requireNonNull(createBlockObjectWriter(keyEncoder, keyType), "keyWriter is null");
            this.valueWriter = requireNonNull(createBlockObjectWriter(valueEncoder, valueType), "valueWriter is null");
        }

        @Override
        public Object encode(Object object)
        {
            if (object == null) {
                return null;
            }
            Map<?, ?> rawMap = mapObjectInspector.getMap(object);

            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, rawMap.size());
            BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
            for (Entry<?, ?> entry : rawMap.entrySet()) {
                if (entry.getKey() == null) {
                    mapBlockBuilder.closeEntry();
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
                }
                // TODO check indeterminate
                keyWriter.write(blockBuilder, entry.getKey());
                valueWriter.write(blockBuilder, entry.getValue());
            }
            try {
                mapBlockBuilder.closeEntryStrict();
            }
            catch (DuplicateMapKeyException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
            }
            return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
        }
    }

    public static class StructObjectEncoder
            implements ObjectEncoder
    {
        private final RowType type;
        private final StructObjectInspector inspector;
        private final List<BlockObjectWriter> fieldWriters;

        public static StructObjectEncoder create(Type type, Object inspector)
        {
            checkArgument((type instanceof RowType) && (inspector instanceof StructObjectInspector));
            return new StructObjectEncoder((RowType) type, (StructObjectInspector) inspector);
        }

        private static BlockObjectWriter createFieldBlockObjectWriter(Type type, StructField field)
        {
            ObjectEncoder encoder = createEncoder(type, field.getFieldObjectInspector());
            return createBlockObjectWriter(encoder, type);
        }

        public StructObjectEncoder(RowType type, StructObjectInspector inspector)
        {
            this.type = requireNonNull(type, "type is null");
            this.inspector = requireNonNull(inspector, "inspector is null");
            this.fieldWriters = Streams.zip(
                    type.getFields().stream().map(RowType.Field::getType),
                    inspector.getAllStructFieldRefs().stream(),
                    StructObjectEncoder::createFieldBlockObjectWriter)
                    .collect(Collectors.toList());
        }

        @Override
        public Object encode(Object object)
        {
            if (object == null) {
                return null;
            }

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) pageBuilder.getBlockBuilder(0);
            SingleRowBlockWriter blockBuilder = rowBlockBuilder.beginBlockEntry();
            List<Object> fieldObjects = inspector.getStructFieldsDataAsList(object);
            final int totalNumField = fieldWriters.size();
            final int numField = fieldObjects.size();
            for (int i = 0; i < totalNumField; i++) {
                fieldWriters.get(i).write(blockBuilder, i < numField ? fieldObjects.get(i) : null);
            }
            rowBlockBuilder.closeEntry();
            pageBuilder.declarePosition();
            return type.getObject(rowBlockBuilder, rowBlockBuilder.getPositionCount() - 1);
        }
    }

    private static BlockObjectWriter createBlockObjectWriter(ObjectEncoder encoder, Type type)
    {
        return new SimpleBlockObjectWriter(encoder, type);
    }

    private interface BlockObjectWriter
    {
        void write(BlockBuilder out, Object object);
    }

    private static class SimpleBlockObjectWriter
            implements BlockObjectWriter
    {
        private final ObjectEncoder objectEncoder;
        private final Type objectType;

        private SimpleBlockObjectWriter(ObjectEncoder objectEncoder, Type objectType)
        {
            this.objectEncoder = requireNonNull(objectEncoder, "objectEncoder is null");
            this.objectType = requireNonNull(objectType, "objectType is null");
        }

        @Override
        public void write(BlockBuilder out, Object object)
        {
            if (object != null) {
                Object encoded = objectEncoder.encode(object);
                if (encoded != null) {
                    writeNativeValue(objectType, out, encoded);
                    return;
                }
            }
            out.appendNull();
        }
    }
}
