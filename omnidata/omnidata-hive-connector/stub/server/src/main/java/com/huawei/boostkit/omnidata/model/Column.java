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

package com.huawei.boostkit.omnidata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.Type;

public class Column {
    public Column(int fieldId, String name, Type type, boolean isPartiontionKey, Object partiontionKeyValues) {
    }

    public Column(int fieldId, String name, Type type) {
    }


    public int getFieldId() {
        return 0;
    }

    @JsonProperty
    public String getName() {
        return "";
    }

    public Type getType() {
        return null;
    }

    public boolean isPartitionKey() {
        return false;
    }

    public Object getPartitionKeyValue() {
        return null;
    }
    public static class Builder {
        private int fieldId;

        private String name;

        private Type type;

        private boolean isPartitionKey;

        private Object partitionKeyValue;

        public Builder setFieldId(int fieldId) {
            this.fieldId = fieldId;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public Builder setPartitionKey(boolean partitionKey) {
            isPartitionKey = partitionKey;
            return this;
        }

        public Builder setPartitionKeyValue(Object partitionKeyValue) {
            this.partitionKeyValue = partitionKeyValue;
            return this;
        }

        /**
         * build column
         *
         * @return Column
         */
        public Column build() {
            return new Column(fieldId, name, type, isPartitionKey, partitionKeyValue);
        }
    }
}

