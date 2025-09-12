package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.runtime.keyselector.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

@JsonInclude(JsonInclude.Include.ALWAYS)
public class StreamPartitionerPOJO {

    private static final Logger LOG = LoggerFactory.getLogger(StreamPartitionerPOJO.class);

    public static final String FORWARD = "forward";
    public static final String REBALANCE = "rebalance";
    public static final String RESCALE = "rescale";
    public static final String GLOBAL = "global";
    public static final String HASH = "hash";
    public static final String NONE = "none";


    private String partitionerName;
    private List<KeyFieldInfoPOJO> hashFields = new ArrayList<>();

    public StreamPartitionerPOJO() {};

    public StreamPartitionerPOJO(StreamPartitioner partitioner) {
        if (partitioner instanceof ForwardPartitioner) {
            partitionerName = FORWARD;
        } else if (partitioner instanceof RebalancePartitioner) {
            partitionerName = REBALANCE;
        } else if (partitioner instanceof RescalePartitioner) {
            partitionerName = RESCALE;
        } else if (partitioner instanceof GlobalPartitioner) {
            partitionerName = GLOBAL;
        } else if (partitioner instanceof KeyGroupStreamPartitioner) {
            partitionerName = HASH;
            hashFields = generateHashPartitionInfoFor((KeyGroupStreamPartitioner) partitioner);
        } else {
            partitionerName = NONE;
        }
    }

    private static List<KeyFieldInfoPOJO> generateHashPartitionInfoFor(KeyGroupStreamPartitioner keyGroupStreamPartitioner) {

        List<KeyFieldInfoPOJO> hashPartitionFieldInfoList = new ArrayList<>();
        try {
            Field keySelectorField = KeyGroupStreamPartitioner.class.getDeclaredField("keySelector");
            keySelectorField.setAccessible(true);
            checkState(keySelectorField.get(keyGroupStreamPartitioner) instanceof KeySelector);
            KeySelector keySelector = (KeySelector) keySelectorField.get(keyGroupStreamPartitioner);
            if (keySelector instanceof BinaryRowDataKeySelector) {
                BinaryRowDataKeySelector binaryRowDataKeySelector = (BinaryRowDataKeySelector) keySelector;
                RowType rowType = binaryRowDataKeySelector.getProducedType().toRowType();
                List<RowType.RowField> rowFieldList = rowType.getFields();
                int[] keyFields = binaryRowDataKeySelector.getKeyFields();
                for (int fieldIdx = 0; fieldIdx < rowFieldList.size(); fieldIdx++) {
                    KeyFieldInfoPOJO fieldInfo = new KeyFieldInfoPOJO();
                    RowType.RowField rowField = rowFieldList.get(fieldIdx);
                    String fieldName = rowField.getName();
                    String fieldTypeName = rowField.getType().getTypeRoot().name();
                    fieldInfo.setFieldName(fieldName);
                    fieldInfo.setFieldTypeName(fieldTypeName);
                    fieldInfo.setFieldIndex(keyFields[fieldIdx]);
                    hashPartitionFieldInfoList.add(fieldInfo);
                }
            }
        } catch (Exception e) {
            LOG.error("can not get hash partition field info .................");
        }
        return hashPartitionFieldInfoList;
    }

    public String getPartitionerName() {
        return partitionerName;
    }

    public void setPartitionerName(String partitionerName) {
        this.partitionerName = partitionerName;
    }

    public List<KeyFieldInfoPOJO> getHashFields() {
        return hashFields;
    }

    public void setHashFields(List<KeyFieldInfoPOJO> hashFields) {
        this.hashFields = hashFields;
    }

    @Override
    public String toString() {
        return "StreamPartitionerPOJO{"
                + "partitionerName='" + partitionerName + '\'' +
                ", hashFields=" + hashFields
                + '}';
    }
}
