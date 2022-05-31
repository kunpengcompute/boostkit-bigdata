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

package org.apache.hadoop.hive.ql.omnidata.operator.aggregation;

/**
 * Ndp Arithmetic Expression Info
 * <p>
 * This class obtains variables such as colId based on the Hive expression string and checks whether the value of colId is a number.
 *
 * @since 2022-01-22
 */
public class NdpArithmeticExpressionInfo {
    private boolean[] isVal;

    private int[] colId;

    private String[] colType;

    private String[] colValue;

    public NdpArithmeticExpressionInfo(String vectorExpressionParameters) {
        // vectorExpressionParameters : col 2:bigint, col 6:bigint
        String[] parameters = vectorExpressionParameters.split(", ");
        int length = parameters.length;
        isVal = new boolean[length];
        colId = new int[length];
        colType = new String[length];
        colValue = new String[length];
        for (int j = 0; j < length; j++) {
            if (parameters[j].startsWith("val ")) {
                isVal[j] = true;
                colValue[j] = parameters[j].substring("val ".length());
            } else {
                isVal[j] = false;
                colId[j] = Integer.parseInt(parameters[j].split(":")[0].substring("col ".length()));
                colType[j] = parameters[j].split(":")[1];
            }
        }
    }

    public boolean[] getIsVal() {
        return isVal;
    }

    public int[] getColId() {
        return colId;
    }

    public String[] getColType() {
        return colType;
    }

    public String[] getColValue() {
        return colValue;
    }

}