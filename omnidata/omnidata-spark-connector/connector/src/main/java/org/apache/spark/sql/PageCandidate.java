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

/**
 * 构造Page传输参数
 */
public class PageCandidate {

    public String filePath;

    public Long startPos;

    public Long splitLen;

    public int columnOffset;

    public String sdiHosts;

    private String fileFormat;

    public int maxFailedTimes;

    private int taskTimeout;

    public PageCandidate(String filePath, Long startPos, Long splitLen, int columnOffset,
                         String sdiHosts, String fileFormat, int maxFailedTimes, int taskTimeout) {
        this.filePath = filePath;
        this.startPos = startPos;
        this.splitLen = splitLen;
        this.columnOffset = columnOffset;
        this.sdiHosts = sdiHosts;
        this.fileFormat = fileFormat;
        this.maxFailedTimes = maxFailedTimes;
        this.taskTimeout = taskTimeout;
    }

    public Long getStartPos() {
        return startPos;
    }

    public Long getSplitLen() {
        return splitLen;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getColumnOffset() {
        return columnOffset;
    }

    public String getSdiHosts() {
        return sdiHosts;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public int getMaxFailedTimes() {
        return maxFailedTimes;
    }

    public int getTaskTimeout() {
        return taskTimeout;
    }
}
