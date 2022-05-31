/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnidata.spark;

import static io.airlift.slice.Slices.wrappedIntArray;

import com.huawei.boostkit.omnidata.decode.Deserializer;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import com.huawei.boostkit.omnidata.decode.type.DoubleDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongDecodeType;
import com.huawei.boostkit.omnidata.exception.OmniDataException;

import io.airlift.slice.SliceInput;
import io.hetu.core.transport.execution.buffer.SerializedPage;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.util.Optional;

/**
 * Deserialize serialized page to spark writableColumnVector array
 *
 * @since 2021-03-30
 */
public class PageDeserializer implements Deserializer<WritableColumnVector[]> {
    private final PageDecoding decoding;

    private final DecodeType[] columnTypes;

    private final int[] columnOrders;

    public PageDeserializer(DecodeType[] columnTypes, int[] columnOrders) {
        this.columnTypes = columnTypes;
        this.decoding = new PageDecoding();
        this.columnOrders = columnOrders;
    }

    @Override
    public WritableColumnVector[] deserialize(SerializedPage page) {
        if (page.isEncrypted()) {
            throw new UnsupportedOperationException("unsupported compressed page.");
        }
        SliceInput sliceInput = page.getSlice().getInput();
        int numberOfBlocks = sliceInput.readInt();
        int returnLength = columnOrders.length;
        WritableColumnVector[] columnVectors = new WritableColumnVector[returnLength];
        int returnId = 0;
        for (int i = 0; i < numberOfBlocks; i++) {
            // handle avg
            if ("RowDecodeType".equals(Optional.of(columnTypes[i]).get().getClass().getSimpleName())) {
                decoding.decode(Optional.of(columnTypes[i]), sliceInput);
                int numFields = sliceInput.readInt();
                columnVectors[columnOrders[returnId++]] =
                        decoding.decode(Optional.of(new DoubleDecodeType()), sliceInput).get();
                columnVectors[columnOrders[returnId++]] =
                        decoding.decode(Optional.of(new LongDecodeType()), sliceInput).get();
                try {
                    int positionCount = sliceInput.readInt();
                    int[] fieldBlockOffsets = new int[positionCount + 1];
                    sliceInput.readBytes(wrappedIntArray(fieldBlockOffsets));
                    boolean[] rowIsNull =
                            decoding.decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);
                } catch (IndexOutOfBoundsException e) {
                    throw new OmniDataException(e.getMessage());
                }
            } else {
                Optional<WritableColumnVector> optionalResult =
                        decoding.decode(Optional.of(columnTypes[i]), sliceInput);
                WritableColumnVector columnResult = optionalResult.orElse(null);
                columnVectors[columnOrders[returnId++]] = columnResult;
            }
        }
        return columnVectors;
    }

}
