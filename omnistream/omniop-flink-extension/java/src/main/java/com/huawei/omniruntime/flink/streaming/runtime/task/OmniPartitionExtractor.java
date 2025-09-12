package com.huawei.omniruntime.flink.streaming.runtime.task;


import com.huawei.omniruntime.flink.runtime.execution.OmniEnvironment;
import com.huawei.omniruntime.flink.runtime.io.network.api.writer.PartitionWriterDelegate;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.*;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.*;
import org.apache.flink.table.runtime.keyselector.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class OmniPartitionExtractor {

    private static final Logger log = LoggerFactory.getLogger(OmniPartitionExtractor.class);

    public static String extractPartitionInfo(
            RecordWriterDelegate recordWriterDelegate)
            throws NoSuchFieldException, IllegalAccessException {

        JSONObject partitionBody = new JSONObject("partition", "{}");

        if (recordWriterDelegate instanceof SingleRecordWriter) {
            RecordWriter recordWriter = recordWriterDelegate.getRecordWriter(0);
            //ResultPartitionWriter resultPartitionWriter = resultPartitionWriter;
            int channelNumber = getNumberOfChannels(recordWriter);
            JSONObject partitionInfo = new JSONObject();
            partitionInfo.put("channelNumber", channelNumber);

            if (recordWriter instanceof ChannelSelectorRecordWriter) {
                ChannelSelector channelSelector = getChannelSelectorFromWriter((ChannelSelectorRecordWriter<?>) recordWriter);
                if (channelSelector instanceof ForwardPartitioner) {
                    partitionInfo.put("partitionName", "forward");

                } else if (channelSelector instanceof RebalancePartitioner) {
                    partitionInfo.put("partitionName", "rebalance");


                } else if (channelSelector instanceof RescalePartitioner) {
                    partitionInfo.put("partitionName", "rescale");


                } else if (channelSelector instanceof KeyGroupStreamPartitioner) {
                    partitionInfo.put("partitionName", "hash");
                    List<JSONObject> hashPartitionFields = generateHashPartitionInfoFor((KeyGroupStreamPartitioner) channelSelector);
                    partitionInfo.put("hashFields", hashPartitionFields);

                } else {
                    //todo
                    partitionInfo.put("partitionName", "none");
                }

                partitionBody.put("partition", partitionInfo);

            } else {
                //todo broadcasting
            }


        } else if (recordWriterDelegate instanceof MultipleRecordWriters) {
            //todo
        } else {
            //todo
            //belongs to NonRecordWriter
        }

        return partitionBody.toString();
    }

    public static String extractPartitionInfo(
            StreamConfig streamConfig,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
        List<NonChainedOutput> nonChainedOutputs = streamConfig.getOperatorNonChainedOutputs(
                Thread.currentThread().getContextClassLoader());
        JSONObject partitionBody = new JSONObject("partition", "{}");
        if (nonChainedOutputs.size() == 1) {
            StreamPartitioner<?> channelSelector = nonChainedOutputs.get(0).getPartitioner();
            int channelNumber = resultPartitionDeploymentDescriptors.get(0).getNumberOfSubpartitions();
            JSONObject partitionInfo = new JSONObject();
            partitionInfo.put("channelNumber", channelNumber);
            if (channelSelector instanceof ForwardPartitioner) {
                partitionInfo.put("partitionName", "forward");
            } else if (channelSelector instanceof RebalancePartitioner) {
                partitionInfo.put("partitionName", "rebalance");
            } else if (channelSelector instanceof RescalePartitioner) {
                partitionInfo.put("partitionName", "rescale");
            } else if (channelSelector instanceof KeyGroupStreamPartitioner) {
                partitionInfo.put("partitionName", "hash");
                List<JSONObject> hashPartitionFields = generateHashPartitionInfoFor((KeyGroupStreamPartitioner) channelSelector);
                partitionInfo.put("hashFields", hashPartitionFields);
            } else {
                //todo broadcasting
                partitionInfo.put("partitionName", "none");
            }
            partitionBody.put("partition", partitionInfo);
        } else if (nonChainedOutputs.size() > 1) {
            //todo
        } else {
            //todo
            //belongs to NonRecordWriter
        }
        return partitionBody.toString();
    }

    private static ChannelSelector getChannelSelectorFromWriter(ChannelSelectorRecordWriter<?> recordWriter)
            throws NoSuchFieldException, IllegalAccessException {

        Field channelSelector = ChannelSelectorRecordWriter.class.getDeclaredField("channelSelector");
        channelSelector.setAccessible(true);
        return (ChannelSelector) channelSelector.get(recordWriter);
    }

    private static List<JSONObject> generateHashPartitionInfoFor(KeyGroupStreamPartitioner keyGroupStreamPartitioner) {

        List<JSONObject> hashPartitionFieldInfoList = new ArrayList<>();
        try {
            Field keySelectorField = KeyGroupStreamPartitioner.class.getDeclaredField("keySelector");
            keySelectorField.setAccessible(true);
            KeySelector keySelector = (KeySelector) keySelectorField.get(keyGroupStreamPartitioner);
            if (keySelector instanceof BinaryRowDataKeySelector) {
                BinaryRowDataKeySelector binaryRowDataKeySelector = (BinaryRowDataKeySelector) keySelector;
                RowType rowType = binaryRowDataKeySelector.getProducedType().toRowType();
                List<RowType.RowField> rowFieldList = rowType.getFields();
                for (int fieldIdx = 0; fieldIdx < rowFieldList.size(); fieldIdx++) {
                    JSONObject fieldInfo = new JSONObject();
                    RowType.RowField rowField = rowFieldList.get(fieldIdx);
                    String fieldName = rowField.getName();
                    String fieldTypeName = rowField.getType().getTypeRoot().name();
                    fieldInfo.put("fieldName", fieldName);
                    fieldInfo.put("fieldTypeName", fieldTypeName);
                    fieldInfo.put("fieldIndex", fieldIdx);
                    hashPartitionFieldInfoList.add(fieldInfo);
                }
            }
        } catch (Exception e) {
            log.error("can not get hash partition field info .................");
        }
        return hashPartitionFieldInfoList;


    }

    public static int getNumberOfChannels(RecordWriter<?> recordWriter) throws NoSuchFieldException, IllegalAccessException {
        Field numberOfChannelsField = RecordWriter.class.getDeclaredField("numberOfChannels");
        numberOfChannelsField.setAccessible(true);
        return numberOfChannelsField.getInt(recordWriter);
    }


    public static StreamPartitioner getHashPartitionInfoFromEnvironment(OmniEnvironment environment) {
        try {
            StreamPartitioner streamPartitioner = new StreamConfig(environment.getTaskConfiguration()).getOutEdgesInOrder(environment.getUserCodeClassLoader().asClassLoader()).get(0).getPartitioner();
            return streamPartitioner;
        }catch (IndexOutOfBoundsException e){
            //sink task does not have partitioner
            return null;
        }catch (Exception e){
            //todo for other potential exceptions , return null as well
            return  null;
        }

    }

    public static boolean ifOnlyBigIntKeyInHashPartitioner(OmniEnvironment environment) {
        StreamPartitioner streamPartitioner = getHashPartitionInfoFromEnvironment(environment);
        if (streamPartitioner instanceof KeyGroupStreamPartitioner) {
            List<JSONObject> hashFieldInfos = generateHashPartitionInfoFor((KeyGroupStreamPartitioner) streamPartitioner);
            for (JSONObject hashField : hashFieldInfos) {
                if (!hashField.get("fieldTypeName").equals("BIGINT")) {
                    return false;
                }
            }
            return true;
        } else {
            return true;
        }
    }


}
