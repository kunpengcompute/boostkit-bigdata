/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * OmniVectorDesc.
 */
@Explain(displayName = "Omni Vector Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniVectorDesc extends AbstractOperatorDesc {
    private static final long serialVersionUID = 1L;
    private boolean isToVector;

    public OmniVectorDesc(boolean isToVector) {
        this.isToVector = isToVector;
    }

    public boolean getIsToVector() {
        return this.isToVector;
    }

    @Explain(displayName = "Convert Type", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
            Explain.Level.EXTENDED})
    public String getConvertType() {
        if (isToVector) {
            return "From row to omni vector";
        }
        return "From omni vector to row";
    }

    @Override
    public boolean isSame(OperatorDesc other) {
        if (getClass().getName().equals(other.getClass().getName())) {
            OmniVectorDesc otherDesc = (OmniVectorDesc) other;
            return getIsToVector() == otherDesc.getIsToVector();
        }
        return false;
    }
}