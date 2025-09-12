package com.huawei.omniruntime.flink.runtime.api.graph.json.configuration;

import com.huawei.omniruntime.flink.runtime.api.graph.json.StreamConfigPOJO;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamConfigHelper
 *
 * @version 1.0.0
 * @since 2025/04/03
 */
public class StreamConfigHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StreamConfigHelper.class);

    /**
     * retrieveChainedConfig
     *
     * @param taskConfig taskConfig
     * @param cl cl
     * @return Map<Integer,StreamConfigPOJO>
     */
    public static Map<Integer, StreamConfigPOJO> retrieveChainedConfig(StreamConfig taskConfig,
                                                                       ClassLoader cl) {
        Map<Integer, StreamConfig> chainedConfigs =
                taskConfig.getTransitiveChainedTaskConfigsWithSelf(cl);

        Map<Integer, StreamConfigPOJO> chainedConfigsPOJO = new HashMap<>();

        for (Map.Entry<Integer, StreamConfig> entry : chainedConfigs.entrySet()) {
            StreamConfig streamConfig = entry.getValue();
            Integer chainId = entry.getKey();

            StreamConfigPOJO streamConfigPOJO = new StreamConfigPOJO(streamConfig, cl);

            chainedConfigsPOJO.put(chainId, streamConfigPOJO);
        }

        return chainedConfigsPOJO;
    }
}
