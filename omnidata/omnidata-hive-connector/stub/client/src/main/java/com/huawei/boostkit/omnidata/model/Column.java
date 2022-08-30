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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * Column information
 *
 * @since 2021-03-30
 */
public class Column {
    private int fieldId;

    private String name;

    private Type type;

    private boolean isPartitionKey;

    private Object partitionKeyValue;

    public Column(int fieldId, String name, Type type) {
        this(fieldId, name, type, false, null);
    }

    @JsonCreator
    public Column(
            @JsonProperty("fieldId") int fieldId,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("partitionKey") boolean isPartitionKey,
            @JsonProperty("partitionKeyValue") Object partitionKeyValue) {
        this.fieldId = fieldId;
        this.name = requireNonNull(name);
        this.type = requireNonNull(type);
        this.isPartitionKey = isPartitionKey;
        this.partitionKeyValue = partitionKeyValue;
    }

    @JsonProperty
    public int getFieldId() {
        return fieldId;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    @JsonProperty
    public Object getPartitionKeyValue() {
        return partitionKeyValue;
    }

    /**
     * Builder
     *
     * @since 2021-03-30
     */
    public static class Builder {
        private int fieldId;

        private String name;

        private Type type;

        private boolean isPartitionKey;

        private Object partitionKeyValue;

        /**
         * set column fieldId
         *
         * @param fieldId column fieldId
         * @return Builder
         */
        public Builder setFieldId(int fieldId) {
            this.fieldId = fieldId;
            return this;
        }

        /**
         * set column type
         *
         * @param name column name
         * @return Builder
         */
        public Builder setName(String name) {
            requireNonNull(name);
            this.name = name;
            return this;
        }

        /**
         * set column type
         *
         * @param type column type
         * @return Builder
         */
        public Builder setType(Type type) {
            requireNonNull(type);
            this.type = type;
            return this;
        }

        /**
         * set partition key
         *
         * @param isPartitionedKey whether column is partitioned
         * @return Builder
         */
        public Builder setPartitionKey(boolean isPartitionedKey) {
            isPartitionKey = isPartitionedKey;
            return this;
        }

        /**
         * set partition key value
         *
         * @param partitionKeyValue column partition key value, this value may be null.
         * @return Builder
         */
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