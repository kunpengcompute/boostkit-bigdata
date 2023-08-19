/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.omnituning.tez.utils;

import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.utils.MathUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TimelineUtils {
    private static final Logger logger = LoggerFactory.getLogger(TimelineUtils.class);

    private static final Random RANDOM_GENERATOR = new Random();
    private static final AtomicInteger THREAD_ID = new AtomicInteger(1);

    private static final ThreadLocal<Integer> LOCAL_THREAD_ID = ThreadLocal.withInitial(THREAD_ID::getAndIncrement);

    private static final ThreadLocal<Long> LOCAL_LAST_UPDATED = new ThreadLocal<>();
    private static final ThreadLocal<Long> LOCAL_UPDATE_INTERVAL = new ThreadLocal<>();

    private static final ThreadLocal<AuthenticatedURL> LOCAL_AUTH_URL = ThreadLocal.withInitial(AuthenticatedURL::new);

    private static final ThreadLocal<AuthenticatedURL.Token> LOCAL_AUTH_TOKEN =
            ThreadLocal.withInitial(() -> {
                LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
                // Random an interval for each executor to avoid update token at the same time
                LOCAL_UPDATE_INTERVAL.set(MathUtils.MINUTE_IN_MS * 30 + RANDOM_GENERATOR.nextLong()
                        % (3 * MathUtils.MINUTE_IN_MS));
                logger.info("Executor " + LOCAL_THREAD_ID.get() + " update interval "
                        + LOCAL_UPDATE_INTERVAL.get() * 1.0 / MathUtils.MINUTE_IN_MS);
                return new AuthenticatedURL.Token();
            });

    private static final ThreadLocal<ObjectMapper> LOCAL_MAPPER = ThreadLocal.withInitial(ObjectMapper::new);

    public static JsonNode readJsonNode(URL url) throws IOException, AuthenticationException {
        HttpURLConnection connection = LOCAL_AUTH_URL.get().openConnection(url, LOCAL_AUTH_TOKEN.get());
        return LOCAL_MAPPER.get().readTree(connection.getInputStream());
    }

    public static void updateAuthToken() {
        long curTime = System.currentTimeMillis();
        if (curTime - LOCAL_LAST_UPDATED.get() > LOCAL_UPDATE_INTERVAL.get()) {
            logger.info("Executor " + LOCAL_THREAD_ID.get() + " updates its AuthenticatedToken.");
            LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
            LOCAL_AUTH_URL.set(new AuthenticatedURL());
            LOCAL_LAST_UPDATED.set(curTime);
        }
    }

    public static String getSHA256String(String inputString) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] result = sha256.digest(inputString.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(result);
        } catch (NoSuchAlgorithmException e) {
            throw new OmniTuningException("Cannot find digest SHA-256", e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
        }
        return hexString.toString();
    }
}
