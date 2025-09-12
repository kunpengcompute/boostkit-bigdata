package com.huawei.omniruntime.flink.utils;

import com.huawei.omniruntime.flink.streaming.runtime.task.StreamTaskUtils;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonConverterTest {
    private List<InputChannelInfo> channelInfoList;
    private long [] expected;
    String  jsonStringExpected;

    private static List<InputChannelInfo> initChannelInfoList(int[][] data) {
        List<InputChannelInfo> objectList = new ArrayList<>();
        for (int[] pair : data) {
            objectList.add(new InputChannelInfo(pair[0], pair[1]));
        }
        return objectList;
    }


    @BeforeEach
    void setUp() {
        int[][] data = {{1, 1}, {1, 2}, {2, 1}, {2, 2}};
        channelInfoList = initChannelInfoList(data);
        expected = new long[]{4294967297L, 4294967298L, 8589934593L, 8589934594L};
        jsonStringExpected = "{\"input_channels\":[4294967297,4294967298,8589934593,8589934594]}";
    }

    @AfterEach
    void tearDown() {
        // Perform cleanup actions here (optional)
    }

    @Test
    void testListToArray() {
        long [] result  = StreamTaskUtils.convertChannelInfo(channelInfoList);
        System.out.println(Arrays.toString(result));
        assertArrayEquals(expected, result);
    }

    @Test
    void testListToJson() {
        long [] channels  = StreamTaskUtils.convertChannelInfo(channelInfoList);
        String channelJson = JsonUtils.longToJsonString(channels, "input_channels");
        System.out.println(channelJson);
        assertEquals(jsonStringExpected, channelJson);
    }
}
