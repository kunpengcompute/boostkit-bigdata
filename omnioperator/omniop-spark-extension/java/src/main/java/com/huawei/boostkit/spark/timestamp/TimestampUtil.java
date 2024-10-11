/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.timestamp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.util.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimestampUtil {

    private static final long MICROS_PER_SECOND = 1000_000L;

    private static final String JULIAN_GREGORIAN_FILE = "julian-gregorian-rebase-micros.json";

    private volatile static TimestampUtil instance;

    private final Map<String, JulianGregorianRebase> julianMap = new HashMap<>();

    private TimestampUtil() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            URL file = Utils.getSparkClassLoader().getResource(JULIAN_GREGORIAN_FILE);
            List<JulianGregorianRebase> julianObjects = objectMapper.readValue(file,
                    new TypeReference<List<JulianGregorianRebase>>() {});
            julianObjects.forEach(julianObject -> {
                for (int i = 0; i < julianObject.getSwitches().length; i++) {
                    julianObject.getSwitches()[i] = julianObject.getSwitches()[i] * MICROS_PER_SECOND;
                    julianObject.getDiffs()[i] = julianObject.getDiffs()[i] * MICROS_PER_SECOND;
                }
                julianMap.put(julianObject.getTz(), julianObject);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public JulianGregorianRebase getJulianObject(String timeZone) {
        return julianMap.get(timeZone);
    }

    public static TimestampUtil getInstance() {
        if (instance != null) {
            return instance;
        }
        synchronized (TimestampUtil.class) {
            if (instance != null) {
                return instance;
            }
            instance = new TimestampUtil();
        }
        return instance;
    }
}

