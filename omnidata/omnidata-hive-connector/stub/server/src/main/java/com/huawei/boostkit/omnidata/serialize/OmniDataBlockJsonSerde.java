/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.io.IOException;

/**
 * Block Json Serde
 *
 * @since 2021-07-31
 */
public final class OmniDataBlockJsonSerde {
    private static final int ESTIMATED_SIZE = 64;

    private OmniDataBlockJsonSerde() {}

    /**
     * Block Json Serializer
     *
     * @since 2021-07-31
     */
    public static class Serializer extends JsonSerializer<Block> {


        public Serializer(BlockEncodingSerde serde) { }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException { }
    }

    /**
     * Deserializer
     *
     * @since 2021-07-31
     */
    public static class Deserializer extends JsonDeserializer<Block> {


        public Deserializer(BlockEncodingSerde encodingSerde) { }

        @Override
        public Block deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            return null;
        }
    }
}

