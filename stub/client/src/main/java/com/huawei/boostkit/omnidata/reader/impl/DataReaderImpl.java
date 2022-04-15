/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.reader.impl;

import com.huawei.boostkit.omnidata.decode.Deserializer;
import com.huawei.boostkit.omnidata.model.TaskSource;

import java.util.Properties;

public class DataReaderImpl<T> {

    public DataReaderImpl(Properties transProperties, TaskSource taskSource, Deserializer deserializer) {

    }

    public boolean isFinished() {
        return true;
    }


    public T getNextPageBlocking() {
        return null;
    }

    public void close() {
    }
}

