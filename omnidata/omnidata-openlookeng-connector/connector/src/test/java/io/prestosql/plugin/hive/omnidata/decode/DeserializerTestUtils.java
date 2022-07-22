/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.plugin.hive.omnidata.decode;

import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.omnidata.exception.OmniDataException;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RowBlockBuilder;
import io.prestosql.spi.block.SingleRowBlockWriter;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

/**
 * Deserializer Test Utils
 *
 * @since 2022-07-18
 */
public class DeserializerTestUtils
{
    public static final Metadata METADATA = createTestMetadataManager();

    private DeserializerTestUtils()
    {}

    /**
     * Return a map type
     * @param keyType keyType
     * @param valueType valueType
     * @return type
     */
    public static MapType<?, ?> mapType(Type keyType, Type valueType)
    {
        Type type = METADATA.getFunctionAndTypeManager()
                .getParameterizedType(StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()),
                                TypeSignatureParameter.of(valueType.getTypeSignature())));
        if (type instanceof MapType) {
            return (MapType) type;
        }
        throw new OmniDataException("Except Map type");
    }

    /**
     * create Long sequence block
     *
     * @param start start
     * @param end end
     * @return block
     */
    public static Block<?> createLongSequenceBlock(int start, int end)
    {
        BlockBuilder<?> builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }

    /**
     * Get Test PageBuilder
     *
     * @return pageBuilder
     */
    public static PageBuilder getTestPageBuilder()
    {
        // generate rowType
        List<Type> fieldTypes = new ArrayList<>();
        fieldTypes.add(DOUBLE);
        fieldTypes.add(BIGINT);
        RowType rowType = RowType.anonymous(fieldTypes);
        //generate a page
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        typesBuilder.add(INTEGER, DOUBLE, BOOLEAN, BIGINT, VARCHAR, SMALLINT, DATE, TINYINT, REAL, rowType);

        ImmutableList<Type> types = typesBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(types);

        fillUpPageBuilder(pageBuilder);

        // RowType
        BlockBuilder<?> builder = pageBuilder.getBlockBuilder(9);
        if (!(builder instanceof RowBlockBuilder)) {
            throw new OmniDataException("Except RowBlockBuilder but found " + builder.getClass());
        }
        RowBlockBuilder<?> rowBlockBuilder = (RowBlockBuilder) builder;
        SingleRowBlockWriter<?> singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
        DOUBLE.writeDouble(singleRowBlockWriter, 1.0);
        BIGINT.writeLong(singleRowBlockWriter, 1);
        rowBlockBuilder.closeEntry();

        singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
        singleRowBlockWriter.appendNull();
        singleRowBlockWriter.appendNull();
        rowBlockBuilder.closeEntry();

        pageBuilder.declarePositions(2);

        return pageBuilder;
    }

    private static void fillUpPageBuilder(PageBuilder pageBuilder)
    {
        BlockBuilder<?> blockBuilder = pageBuilder.getBlockBuilder(0);
        blockBuilder.writeInt(1);
        blockBuilder.appendNull();

        // DOUBLE
        blockBuilder = pageBuilder.getBlockBuilder(1);
        blockBuilder.writeLong(Double.doubleToLongBits(1.0));
        blockBuilder.appendNull();

        // BOOLEAN false
        blockBuilder = pageBuilder.getBlockBuilder(2);
        blockBuilder.writeByte(0);
        blockBuilder.appendNull();

        // LONG
        blockBuilder = pageBuilder.getBlockBuilder(3);
        blockBuilder.writeLong(1);
        blockBuilder.appendNull();

        // VARCHAR
        blockBuilder = pageBuilder.getBlockBuilder(4);
        blockBuilder.writeBytes(wrappedBuffer("test".getBytes(StandardCharsets.UTF_8)), 0, "test".length());
        blockBuilder.closeEntry();
        blockBuilder.appendNull();

        // SMALLINT
        blockBuilder = pageBuilder.getBlockBuilder(5);
        blockBuilder.writeShort(1);
        blockBuilder.appendNull();

        // DATE
        blockBuilder = pageBuilder.getBlockBuilder(6);
        blockBuilder.writeInt(1);
        blockBuilder.appendNull();

        // TINYINT
        blockBuilder = pageBuilder.getBlockBuilder(7);
        blockBuilder.writeByte(1);
        blockBuilder.appendNull();

        // REAL
        blockBuilder = pageBuilder.getBlockBuilder(8);
        blockBuilder.writeInt(Float.floatToIntBits((float) 1.0));
        blockBuilder.appendNull();
    }
}
