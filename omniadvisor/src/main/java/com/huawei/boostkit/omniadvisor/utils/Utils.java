/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.utils;

import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import net.minidev.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Utils {
    private Utils() {}

    public static Map<String, String> loadParamsFromConf(String configParamName, Map<String, String> conf) {
        URL fileURL = requireNonNull(Thread.currentThread().getContextClassLoader().getResource(configParamName),
                format("Tez param config file %s is not found", configParamName));
        Map<String, String> params = new HashMap<>();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fileURL.getPath()), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                params.put(line, conf.getOrDefault(line, ""));
            }
        } catch (IOException e) {
            throw new OmniAdvisorException(e);
        }
        return params;
    }

    public static String parseMapToJsonString(Map<String, String> map) {
        JSONObject json = new JSONObject();
        json.putAll(map);
        return json.toJSONString();
    }
}
