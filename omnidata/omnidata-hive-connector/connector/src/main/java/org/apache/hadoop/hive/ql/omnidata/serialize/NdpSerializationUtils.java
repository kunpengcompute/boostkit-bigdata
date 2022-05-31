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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Npd serialization tool
 *
 * @since 2022-01-28
 */
public class NdpSerializationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NdpSerializationUtils.class);

    public static String serializeNdpPredicateInfo(NdpPredicateInfo ndpPredicateInfo) {
        try {
            ObjectMapper mapper = new NdpObjectMapperProvider().get();
            return mapper.writeValueAsString(ndpPredicateInfo);
        } catch (JsonProcessingException e) {
            LOG.error("serializeNdpPredicateInfo() failed", e);
        }
        return "";
    }

    public static NdpPredicateInfo deserializeNdpPredicateInfo(String predicateStr) {
        try {
            if (predicateStr != null && predicateStr.length() > 0) {
                ObjectMapper mapper = new NdpObjectMapperProvider().get();
                return mapper.readValue(predicateStr, NdpPredicateInfo.class);
            } else {
                return new NdpPredicateInfo(false);
            }
        } catch (IOException e) {
            throw new RuntimeException("deserializeNdpPredicateInfo() failed", e);
        }
    }

}
