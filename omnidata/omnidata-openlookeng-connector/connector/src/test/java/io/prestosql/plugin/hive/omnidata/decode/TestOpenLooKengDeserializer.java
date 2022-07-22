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
import com.huawei.boostkit.omnidata.serialize.OmniDataBlockEncodingSerde;
import io.airlift.compress.Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.plugin.hive.omnidata.decode.impl.OpenLooKengDecoding;
import io.prestosql.plugin.hive.omnidata.decode.impl.OpenLooKengDeserializer;
import io.prestosql.plugin.hive.omnidata.decode.type.ArrayDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.DateDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.DecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToByteDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToFloatDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToIntDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToShortDecodeType;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.prestosql.plugin.hive.omnidata.decode.DeserializerTestUtils.METADATA;
import static io.prestosql.plugin.hive.omnidata.decode.DeserializerTestUtils.createLongSequenceBlock;
import static io.prestosql.plugin.hive.omnidata.decode.DeserializerTestUtils.getTestPageBuilder;
import static io.prestosql.plugin.hive.omnidata.decode.DeserializerTestUtils.mapType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Block deserializer test
 *
 * @since 2022-07-19
 */
public class TestOpenLooKengDeserializer
{
    /**
     * Test all types
     */
    @Test
    public void testAllSupportTypesDeserializer()
    {
        Page page = getTestPageBuilder().build();

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(page);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();
        Page deserializedPage = deserializer.deserialize(serializedPage);

        assertEquals(2, deserializedPage.getPositionCount());
        assertEquals(10, deserializedPage.getChannelCount());

        assertEquals(deserializedPage.getBlock(0).getSizeInBytes(), 10);
        assertEquals(deserializedPage.getBlock(1).getSizeInBytes(), 18);
        assertEquals(deserializedPage.getBlock(2).getSizeInBytes(), 4);
        assertEquals(deserializedPage.getBlock(3).getSizeInBytes(), 18);
        assertEquals(deserializedPage.getBlock(4).getSizeInBytes(), 14);
        assertEquals(deserializedPage.getBlock(5).getSizeInBytes(), 6);
        assertEquals(deserializedPage.getBlock(6).getSizeInBytes(), 10);
        assertEquals(deserializedPage.getBlock(7).getSizeInBytes(), 4);
        assertEquals(deserializedPage.getBlock(8).getSizeInBytes(), 10);
        assertEquals(deserializedPage.getBlock(9).getSizeInBytes(), 46);
    }

    /**
     * Test Compressed deserializer
     */
    @Test
    public void testCompressedDeserializer()
    {
        long[] values = new long[] {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 0};
        boolean[] valueIsNull = new boolean[] {false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, true};
        LongArrayBlock longArrayBlock = new LongArrayBlock(16, Optional.of(valueIsNull), values);
        Page longArrayPage = new Page(longArrayBlock);

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), true);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(longArrayPage);

        // deserialize page
        DecodeType[] decodeTypes = new DecodeType[] {
                new LongToIntDecodeType(), new LongToShortDecodeType(), new LongToByteDecodeType(),
                new LongToFloatDecodeType(), new DateDecodeType()
        };

        OpenLooKengDecoding blockDecoding = new OpenLooKengDecoding();
        Decompressor decompressor = new ZstdDecompressor();
        int failedTimes = 0;
        for (int i = 0; i < decodeTypes.length; i++) {
            // to decode type
            Slice slice = OpenLooKengDeserializer.decompressPage(serializedPage, decompressor);
            SliceInput input = slice.getInput();
            int numberOfBlocks = input.readInt();
            Block<?>[] blocks = new Block<?>[numberOfBlocks];
            try {
                blocks[0] = blockDecoding.decode(Optional.of(decodeTypes[i]), input);
            }
            catch (OmniDataException e) {
                failedTimes++;
            }
        }

        assertEquals(failedTimes, decodeTypes.length);
    }

    /**
     * Test Int128Type deserializer
     */
    @Test
    public void testInt128TypeDeserializer()
    {
        Int128ArrayBlock int128ArrayBlock =
                new Int128ArrayBlock(0, Optional.empty(), new long[0]);
        Page int128ArrayPage = new Page(int128ArrayBlock);

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(int128ArrayPage);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate exception
        Page page = deserializer.deserialize(serializedPage);

        assertEquals(0, page.getSizeInBytes());
    }

    /**
     * Test RunLengthType deserializer
     */
    @Test
    public void testRunLengthTypeDeserializer()
    {
        RunLengthEncodedBlock<?> runLengthEncodedBlock =
                new RunLengthEncodedBlock<>(createLongSequenceBlock(4, 5), 100);
        Page runLengthPage = new Page(runLengthEncodedBlock);

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(runLengthPage);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate ColumnVector
        Page page = deserializer.deserialize(serializedPage);

        assertEquals(9, page.getSizeInBytes());
    }

    /**
     * Test ArrayType deserializer
     */
    @Test
    public void testArrayTypeDeserializer()
    {
        // generate a page
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        typeBuilder.add(new ArrayType<>(BIGINT));

        ImmutableList<Type> types = typeBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(types);
        BlockBuilder<?> blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder<?> elementBlockBuilder = BIGINT.createBlockBuilder(null, 2);
        for (int i = 0; i < 2; i++) {
            BIGINT.writeLong(elementBlockBuilder, 1);
        }
        blockBuilder.appendStructure(elementBlockBuilder.build());

        pageBuilder.declarePositions(1);
        Page page = pageBuilder.build();

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(page);

        //deserialize page
        DecodeType[] decodeTypes = new DecodeType[] {new ArrayDecodeType<>(new LongDecodeType())};
        DecodeType firstType = decodeTypes[0];
        if (firstType instanceof ArrayDecodeType) {
            assertEquals(((ArrayDecodeType) firstType).getElementType().getClass(), LongDecodeType.class);
        }
        else {
            throw new OmniDataException("except arrayType");
        }
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate exception
        Page deserialized = deserializer.deserialize(serializedPage);

        assertEquals(23, deserialized.getSizeInBytes());
    }

    /**
     * Test MapType deserializer
     */
    @Test(expectedExceptions = {UnsupportedOperationException.class, RuntimeException.class})
    public void testMapTypeDeserializer()
    {
        // generate a page
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        MapType<?, ?> mapType = mapType(BIGINT, BIGINT);

        typeBuilder.add(mapType);

        ImmutableList<Type> types = typeBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(types);
        BlockBuilder<?> blockBuilder = pageBuilder.getBlockBuilder(0);
        blockBuilder.appendNull();

        pageBuilder.declarePositions(1);
        Page page = pageBuilder.build();

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(METADATA.getFunctionAndTypeManager().getBlockEncodingSerde(),
                false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(page);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate exception
        deserializer.deserialize(serializedPage);
    }

    /**
     * Test SingleMapType deserializer
     */
    @Test(expectedExceptions = {UnsupportedOperationException.class, RuntimeException.class})
    public void testSingleMapTypeDeserializer()
    {
        // generate a page
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        MapType<?, ?> mapType = mapType(BIGINT, BIGINT);

        typeBuilder.add(mapType);

        ImmutableList<Type> types = typeBuilder.build();
        PageBuilder pageBuilder = new PageBuilder(types);
        BlockBuilder<?> blockBuilder = pageBuilder.getBlockBuilder(0);
        blockBuilder.appendNull();

        pageBuilder.declarePositions(1);
        Page page = pageBuilder.build();
        Block<?> block = page.getBlock(0);

        Block<?> elementBlock = mapType.getObject(block, 0);
        assertTrue(elementBlock instanceof SingleMapBlock);

        Page singleMapPage = new Page(elementBlock);

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(METADATA.getFunctionAndTypeManager().getBlockEncodingSerde(),
                false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(singleMapPage);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate exception
        deserializer.deserialize(serializedPage);
    }

    /**
     * Test DictionaryType deserializer
     */
    @Test
    public void testDictionaryTypeDeserializer()
    {
        int[] ids = new int[100];
        Arrays.setAll(ids, index -> index % 10);
        Block<?> dictionary = createLongSequenceBlock(0, 10);
        DictionaryBlock<?> dictionaryBlock = new DictionaryBlock<>(dictionary, ids);
        Page dictionaryPage = new Page(dictionaryBlock);

        // serialize page
        PagesSerdeFactory factory = new PagesSerdeFactory(new OmniDataBlockEncodingSerde(), false);
        PagesSerde pagesSerde = factory.createPagesSerde();
        SerializedPage serializedPage = pagesSerde.serialize(dictionaryPage);

        // deserialize page
        OpenLooKengDeserializer deserializer = new OpenLooKengDeserializer();

        // generate exception
        Page page = deserializer.deserialize(serializedPage);

        assertEquals(490, page.getSizeInBytes());
    }
}
