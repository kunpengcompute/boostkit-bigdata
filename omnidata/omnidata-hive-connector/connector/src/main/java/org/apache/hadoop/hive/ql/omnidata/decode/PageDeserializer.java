package org.apache.hadoop.hive.ql.omnidata.decode;

import com.huawei.boostkit.omnidata.decode.Deserializer;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;

import io.airlift.slice.SliceInput;
import io.hetu.core.transport.execution.buffer.SerializedPage;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * PageDeserializer
 */
public class PageDeserializer implements Deserializer<List<ColumnVector[]>> {

    private final PageDecoding decoding;

    private final DecodeType[] columnTypes;

    public PageDeserializer(DecodeType[] columnTypes) {
        this.columnTypes = columnTypes;
        decoding = new PageDecoding();
    }

    @Override
    public List<ColumnVector[]> deserialize(SerializedPage page) {
        if (page.isCompressed() || page.isEncrypted()) {
            throw new UnsupportedOperationException(
                    "unsupported HiveDeserializer isMarkerPage or compressed or encrypted page ");
        }
        SliceInput input = page.getSlice().getInput();
        int numberOfBlocks = input.readInt();
        checkArgument(numberOfBlocks >= 0, "decode failed, numberOfBlocks < 0");
        List<ColumnVector[]> columnVectors = new ArrayList<>();

        for (int i = 0; i < numberOfBlocks; i++) {
            ColumnVector[] result = decoding.decode(Optional.of(columnTypes[i]), input);
            if (result == null) {
                return null;
            }
            columnVectors.add(result);
        }
        return transform(columnVectors, numberOfBlocks);
    }

    private List<ColumnVector[]> transform(List<ColumnVector[]> columnVectors, int numberOfBlocks) {
        List<ColumnVector[]> newColumnVectors = new ArrayList<>();
        for (int i = 0; i < columnVectors.get(0).length; i++) {
            ColumnVector[] result = new ColumnVector[numberOfBlocks];
            for (int j = 0; j < numberOfBlocks; j++) {
                result[j] = columnVectors.get(j)[i];
            }
            newColumnVectors.add(result);
        }
        return newColumnVectors;
    }

}