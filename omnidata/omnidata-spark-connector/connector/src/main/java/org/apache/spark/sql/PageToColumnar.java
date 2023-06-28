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

package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static org.apache.commons.lang.StringUtils.rightPad;

/**
 * PageToColumnar
 */
public class PageToColumnar implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PageToColumnar.class);

    private static final String ORC_HIVE = "hive";

    private static final String METADATA_CHAR = "char";

    StructType structType;
    Seq<Attribute> outPut;

    public PageToColumnar(StructType structType, Seq<Attribute> outPut) {
        this.structType = structType;
        this.outPut = outPut;
    }

    public List<Object> transPageToColumnar(Iterator<WritableColumnVector[]> writableColumnVectors,
                                            boolean isVectorizedReader, boolean isOperatorCombineEnabled, Seq<Attribute> sparkOutPut, String orcImpl) {
        if (isOperatorCombineEnabled) {
            LOG.debug("OmniRuntime PushDown column info: OmniColumnVector transform to Columnar");
        }
        List<Object> internalRowList = new ArrayList<>();
        List<Attribute> outputColumnList = JavaConverters.seqAsJavaList(sparkOutPut);
        while (writableColumnVectors.hasNext()) {
            WritableColumnVector[] columnVector = writableColumnVectors.next();
            if (columnVector == null) {
                continue;
            }
            int positionCount = columnVector[0].getElementsAppended();
            if (positionCount <= 0) {
                continue;
            }
            if (isVectorizedReader) {
                ColumnarBatch columnarBatch = new ColumnarBatch(columnVector);
                columnarBatch.setNumRows(positionCount);
                internalRowList.add(columnarBatch);
            } else {
                for (int j = 0; j < positionCount; j++) {
                    procVectorForOrcHive(columnVector, orcImpl,  outputColumnList, j);
                    MutableColumnarRow mutableColumnarRow =
                            new MutableColumnarRow(columnVector);
                    mutableColumnarRow.rowId = j;
                    internalRowList.add(mutableColumnarRow);
                }
            }
        }
        return internalRowList;
    }
    public void procVectorForOrcHive(WritableColumnVector[] columnVectors, String orcImpl, List<Attribute> outputColumnList, int rowId) {
        if (orcImpl.equals(ORC_HIVE)) {
            for (int i = 0; i < columnVectors.length; i++) {
                if (columnVectors[i].dataType() instanceof StringType) {
                    Attribute attribute = outputColumnList.get(i);
                    Metadata metadata = attribute.metadata();
                    putPaddingChar(columnVectors[i], metadata, rowId);
                }
            }
        }
    }

    public void putPaddingChar(WritableColumnVector columnVector, Metadata metadata, int rowId) {
        if (CharVarcharUtils.getRawTypeString(metadata).isDefined()) {
            String metadataStr = CharVarcharUtils.getRawTypeString(metadata).get();
            Pattern pattern = Pattern.compile("(?<=\\()\\d+(?=\\))");
            Matcher matcher = pattern.matcher(metadataStr);
            String len = String.valueOf(UNBOUNDED_LENGTH);
            while(matcher.find()){
                len = matcher.group();
            }
            if (metadataStr.startsWith(METADATA_CHAR)) {
                String vecStr = columnVector.getUTF8String(rowId).toString();
                String vecStrPad = rightPad(vecStr, Integer.parseInt(len), ' ');
                byte[] bytes = vecStrPad.getBytes(StandardCharsets.UTF_8);
                if (columnVector instanceof OnHeapColumnVector) {
                    columnVector.putByteArray(rowId, bytes, 0, bytes.length);
                } else {
                    columnVector.putBytes(rowId, bytes.length, bytes, 0);
                }
            }
        }
    }
}