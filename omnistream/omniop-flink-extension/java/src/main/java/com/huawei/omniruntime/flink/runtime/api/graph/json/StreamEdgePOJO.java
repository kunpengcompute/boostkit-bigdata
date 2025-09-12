package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.streaming.api.graph.StreamEdge;

/**
 * StreamEdgePOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class StreamEdgePOJO {
    private int sourceId;
    private int targetId;

    private int typeNumber;

    private StreamPartitionerPOJO partitioner;

    private long bufferTimeout;

    public StreamEdgePOJO() {
    }

    public StreamEdgePOJO(StreamEdge streamEdge) {
        this.sourceId = streamEdge.getSourceId();
        this.targetId = streamEdge.getTargetId();
        this.typeNumber = streamEdge.getTypeNumber();
        this.bufferTimeout = streamEdge.getBufferTimeout();
        this.partitioner = new StreamPartitionerPOJO(streamEdge.getPartitioner());
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public int getTargetId() {
        return targetId;
    }

    public void setTargetId(int targetId) {
        this.targetId = targetId;
    }

    public int getTypeNumber() {
        return typeNumber;
    }

    public void setTypeNumber(int typeNumber) {
        this.typeNumber = typeNumber;
    }

    public StreamPartitionerPOJO getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(StreamPartitionerPOJO partitioner) {
        this.partitioner = partitioner;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setBufferTimeout(long bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    @Override
    public String toString() {
        return "StreamEdgePOJO{"
                + "sourceId=" + sourceId
                + ", targetId=" + targetId
                + ", typeNumber=" + typeNumber
                + ", partitioner=" + partitioner
                + ", bufferTimeout=" + bufferTimeout
                + '}';
    }
}
