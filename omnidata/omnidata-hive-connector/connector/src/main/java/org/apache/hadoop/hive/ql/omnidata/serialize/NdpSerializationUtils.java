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
