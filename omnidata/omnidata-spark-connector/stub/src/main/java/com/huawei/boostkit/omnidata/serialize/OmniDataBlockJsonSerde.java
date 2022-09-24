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

