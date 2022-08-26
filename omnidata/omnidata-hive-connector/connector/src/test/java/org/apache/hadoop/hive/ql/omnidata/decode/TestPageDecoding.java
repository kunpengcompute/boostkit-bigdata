/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.decode;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.RowType;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.omnidata.decode.type.DecodeType;
import org.apache.hadoop.hive.ql.omnidata.decode.type.LongToByteDecodeType;
import org.apache.hadoop.hive.ql.omnidata.decode.type.LongToFloatDecodeType;
import org.apache.hadoop.hive.ql.omnidata.decode.type.LongToIntDecodeType;
import org.apache.hadoop.hive.ql.omnidata.decode.type.LongToShortDecodeType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/**
 * Test PageDecoding
 *
 * @since 2022-08-24
 */
public class TestPageDecoding {
    private PageDecoding pageDecoding;

    private DecodeType type;

    private SliceInput sliceInput;

    @Before
    public void init() {
        pageDecoding = new PageDecoding();
        type = mock(DecodeType.class);
        sliceInput = mock(SliceInput.class);
        when(sliceInput.readInt()).thenReturn(1);
    }

    @Test
    public void testDecodeArray() {
        try {
            ColumnVector[] columnVectors = pageDecoding.decodeArray(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors.length, 0);
        } catch (UnsupportedOperationException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void testDecodeRunLength() throws InvocationTargetException, IllegalAccessException {
        // Double
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Double.TYPE));
        ColumnVector[] columnVectors1 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors1.length, 1);

        // Float
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Float.TYPE));
        ColumnVector[] columnVectors2 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors2.length, 1);

        // Integer
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Integer.TYPE));
        ColumnVector[] columnVectors3 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors3.length, 1);

        // Long
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Long.TYPE));
        ColumnVector[] columnVectors4 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors4.length, 1);

        // Byte
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Byte.TYPE));
        ColumnVector[] columnVectors5 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors5.length, 1);

        // Boolean
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Boolean.TYPE));
        try {
            ColumnVector[] columnVectors6 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors6.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // Short
        when(type.getJavaType()).thenReturn(Optional.ofNullable(Short.TYPE));
        try {
            ColumnVector[] columnVectors7 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors7.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // String
        when(type.getJavaType()).thenReturn(Optional.of(String.class));
        try {
            ColumnVector[] columnVectors8 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors8.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // RowType
        try {
            when(type.getJavaType()).thenReturn(Optional.of(RowType.class));
            ColumnVector[] columnVectors9 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertNull(columnVectors9);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getClass(), IllegalAccessException.class);
        }

        // DateType
        when(type.getJavaType()).thenReturn(Optional.of(DateType.class));
        ColumnVector[] columnVectors10 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
        Assert.assertEquals(columnVectors10.length, 1);

        // LongToIntDecodeType
        try {
            when(type.getJavaType()).thenReturn(Optional.of(LongToIntDecodeType.class));
            ColumnVector[] columnVectors11 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors11.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // LongToShortDecodeType
        try {
            when(type.getJavaType()).thenReturn(Optional.of(LongToShortDecodeType.class));
            ColumnVector[] columnVectors12 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors12.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // LongToByteDecodeType
        try {
            when(type.getJavaType()).thenReturn(Optional.of(LongToByteDecodeType.class));
            ColumnVector[] columnVectors13 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors13.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }

        // LongToFloatDecodeType
        try {
            when(type.getJavaType()).thenReturn(Optional.of(LongToFloatDecodeType.class));
            ColumnVector[] columnVectors14 = pageDecoding.decodeRunLength(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors14.length, 1);
        } catch (InvocationTargetException | IllegalAccessException e) {
            Assert.assertEquals(e.getMessage(), "decode failed null");
        }
    }

    @Test
    public void testDecodeDictionary() {
        try {
            ColumnVector[] columnVectors = pageDecoding.decodeDictionary(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors.length, 0);
        } catch (UnsupportedOperationException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void testDecodeInt128Array() {
        try {
            ColumnVector[] columnVectors = pageDecoding.decodeInt128Array(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors.length, 0);
        } catch (UnsupportedOperationException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void testDecodeMap() {
        try {
            ColumnVector[] columnVectors = pageDecoding.decodeMap(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors.length, 0);
        } catch (UnsupportedOperationException e) {
            Assert.assertNull(e.getMessage());
        }
    }

    @Test
    public void testDecodeSingleMap() {
        try {
            ColumnVector[] columnVectors = pageDecoding.decodeSingleMap(Optional.ofNullable(type), sliceInput);
            Assert.assertEquals(columnVectors.length, 0);
        } catch (UnsupportedOperationException e) {
            Assert.assertNull(e.getMessage());
        }
    }
}