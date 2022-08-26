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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.junit.Test;

/**
 * TestPageDeRunLength
 *
 * @since 2022-08-24
 */
public class TestPageDeRunLength {
    @Test
    public void testDecompressIntArray() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new LongColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressIntArray(1, columnVector);
    }

    @Test
    public void testDecompressShortArray() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new LongColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressShortArray(1, columnVector);
    }

    @Test
    public void testDecompressLongArray() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new LongColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressLongArray(1, columnVector);
    }

    @Test
    public void testDecompressFloatArray() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new DoubleColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressFloatArray(1, columnVector);
    }

    @Test
    public void testDecompressDoubleArray() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new DoubleColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressDoubleArray(1, columnVector);
    }

    @Test
    public void testDecompressVariableWidth() {
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        ColumnVector columnVector = new BytesColumnVector(1);
        columnVector.isNull[0] = true;
        pageDeRunLength.decompressVariableWidth(1, columnVector);
    }
}