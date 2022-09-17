/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.transfer;

/**
 * Property for omniData communication
 *
 * @since 2021-08-27
 */
public class OmniDataProperty {
    private OmniDataProperty() {}

    /**
     * constant string for "omnidata.client.target.list"
     */
    public static final String OMNIDATA_CLIENT_TARGET_LIST = "omnidata.client.target.list";

    /**
     * delimiter string for "omnidata.client.target.list"
     * examples: 192.0.2.1:80,192.0.2.2:80
     */
    public static final String HOSTADDRESS_DELIMITER = ",";

    /**
     * constant string for "omnidata.client.target"
     */
    public static final String OMNIDATA_CLIENT_TARGET = "omnidata.client.target";

    /**
     * constant string for "omnidata.client.task.timeout"
     */
    public static final String OMNIDATA_CLIENT_TASK_TIMEOUT = "omnidata.client.task.timeout";
}

