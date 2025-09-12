package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ValidateExpandOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateExpandOPStrategy.class);

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        if (operatorInfoMap.containsKey("projects")) {
            List<Map> projectsJson = (List<Map>) operatorInfoMap.get("projects");
            if (CollectionUtil.isNullOrEmpty(projectsJson)) {
                return false;
            } else {
                boolean result = false;
                AbstractValidateOperatorStrategy strategy = ValidateOperatorStrategyFactory.getStrategy("Calc");
                for (Map projects : projectsJson) {
                    result = strategy.executeValidateOperator(projects);
                }
                return result;
            }
        } else {
            LOG.info("Missing projects field.");
            return false;
        }
    }

    private Map<String, Object> toJsonMap(String projectsJson) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = null;
        try {
            jsonMap = objectMapper.readValue(projectsJson, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            LOG.warn("operatorDescription read jsonString to Map failed!", e);
        }
        return jsonMap;
    }
}
