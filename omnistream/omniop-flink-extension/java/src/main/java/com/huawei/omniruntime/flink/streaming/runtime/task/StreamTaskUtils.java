package com.huawei.omniruntime.flink.streaming.runtime.task;

import com.huawei.omniruntime.flink.utils.JsonUtils;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;

import java.util.List;

public class StreamTaskUtils {
    public  static long[] convertChannelInfo (List<InputChannelInfo> channelInfos) {
        return channelInfos.stream()
                .mapToLong( channelInfo ->
                        (long) channelInfo.getGateIdx() << 32 | channelInfo.getInputChannelIdx())
                .toArray();
    }

    public static String convertChannelInfoToJson (List<InputChannelInfo> channelInfos) {
        long[] channelInfo = convertChannelInfo(channelInfos);
        return JsonUtils.longToJsonString(channelInfo, "input_channels");
    }
}
