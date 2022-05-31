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

package org.apache.hadoop.hive.ql.omnidata.serialize;

import com.huawei.boostkit.omnidata.metadata.InternalTypeManager;
import com.huawei.boostkit.omnidata.metadata.Metadata;
import com.huawei.boostkit.omnidata.metadata.TypeDeserializer;
import com.huawei.boostkit.omnidata.serialize.OmniDataBlockEncodingSerde;
import com.huawei.boostkit.omnidata.serialize.OmniDataBlockJsonSerde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Object mapper provider
 *
 * @since 2020-07-31
 */
public class NdpObjectMapperProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NdpObjectMapperProvider.class);

    private final JsonFactory jsonFactory;

    private final Set<Module> modules = new HashSet<>();

    public NdpObjectMapperProvider() {
        this.jsonFactory = new JsonFactory();

        modules.add(new Jdk8Module());
        modules.add(new GuavaModule());
        modules.add(new JavaTimeModule());
        modules.add(new ParameterNamesModule());

        SimpleModule module = new SimpleModule();
        module.addSerializer(Block.class, new OmniDataBlockJsonSerde.Serializer(new OmniDataBlockEncodingSerde()));
        module.addDeserializer(Block.class, new OmniDataBlockJsonSerde.Deserializer(new OmniDataBlockEncodingSerde()));
        module.addDeserializer(Type.class, new TypeDeserializer(new InternalTypeManager(new Metadata())));
        modules.add(module);

        try {
            getClass().getClassLoader().loadClass("org.joda.time.DateTime");
            modules.add(new JodaModule());
        } catch (ClassNotFoundException ignored) {
            // ignore this exception
            LOG.warn("Can't find class org.joda.time.DateTime.");
        }
    }

    /**
     * get Object mapper
     *
     * @return objectMapper
     */
    public ObjectMapper get() {
        ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
        objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDefaultPropertyInclusion(
            JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.ALWAYS));

        objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
        objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);
        objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);

        for (Module module : modules) {
            objectMapper.registerModule(module);
        }

        return objectMapper;
    }
}
