/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.timestamp;

import java.util.Arrays;

public class JulianGregorianRebase {
    private String tz;

    private long[] switches;

    private long[] diffs;

    public String getTz() {
        return tz;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public long[] getSwitches() {
        return switches;
    }

    public void setSwitches(long[] switches) {
        this.switches = switches;
    }

    public long[] getDiffs() {
        return diffs;
    }

    public void setDiffs(long[] diffs) {
        this.diffs = diffs;
    }

    @Override
    public String toString() {
        return "JulianGregorianRebase{" +
                "tz='" + tz + '\'' +
                ", switches=" + Arrays.toString(switches) +
                ", diffs=" + Arrays.toString(diffs) +
                '}';
    }
}
