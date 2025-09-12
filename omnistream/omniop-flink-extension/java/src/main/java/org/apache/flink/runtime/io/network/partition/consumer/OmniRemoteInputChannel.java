package org.apache.flink.runtime.io.network.partition.consumer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;

/**
 * OmniRemoteInputChannel
 *
 * @since 2025-04-27
 */
public class OmniRemoteInputChannel {
    private RemoteInputChannel remoteInputChannel;
    private boolean isConnected = false;
    private boolean dataReceived = false;

    public OmniRemoteInputChannel(RemoteInputChannel remoteInputChannel) {
        this.remoteInputChannel = remoteInputChannel;
    }

    public RemoteInputChannel getRemoteInputChannel() {
        return remoteInputChannel;
    }

    public void setRemoteInputChannel(RemoteInputChannel remoteInputChannel) {
        this.remoteInputChannel = remoteInputChannel;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    /**
     * getChannelIndex
     *
     * @return int
     */
    public int getChannelIndex() {
        return remoteInputChannel.getChannelIndex();
    }

    /**
     * getGateIndex
     *
     * @return int
     */
    public int getGateIndex() {
        return remoteInputChannel.getChannelInfo().getGateIdx();
    }

    /**
     * getNextBuffer
     *
     * @return Optional<InputChannel.BufferAndAvailability>
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     * @throws IOException IOException
     */
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException, IOException {
        return remoteInputChannel.getNextBuffer();
    }

    /**
     * getSingleInputGateConsumedSubpartitionIndex
     *
     * @return int
     */
    public int getSingleInputGateConsumedSubpartitionIndex() {
        Class clazz = SingleInputGate.class;
        try {
            Field field = clazz.getDeclaredField("consumedSubpartitionIndex");
            field.setAccessible(true);
            return field.getInt(this.remoteInputChannel.inputGate);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDataReceived(boolean dataReceived) {
        this.dataReceived = dataReceived;
    }
    public boolean isDataReceived() {
        return dataReceived;
    }
}
