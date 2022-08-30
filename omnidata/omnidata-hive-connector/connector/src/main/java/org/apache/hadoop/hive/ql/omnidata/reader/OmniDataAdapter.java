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

package org.apache.hadoop.hive.ql.omnidata.reader;

import static org.apache.hadoop.hive.ql.omnidata.OmniDataUtils.addPartitionValues;

import com.huawei.boostkit.omnidata.exception.OmniDataException;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsOrcDataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource;
import com.huawei.boostkit.omnidata.reader.impl.DataReaderImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.decode.PageDeserializer;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusManager;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Queue;

/**
 * Obtains data from OmniData through OmniDataAdapter and converts the data into Hive List<ColumnVector[]>.
 */
public class OmniDataAdapter implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OmniDataAdapter.class);

    /**
     * The maximum number of retry times is 4.
     */
    private static final int TASK_FAILED_TIMES = 4;

    private TaskSource taskSource;

    private List<String> omniDataHosts;

    private PageDeserializer deserializer;

    public OmniDataAdapter(Configuration conf, FileSplit fileSplit, NdpPredicateInfo ndpPredicateInfo,
                           PageDeserializer deserializer) {
        this.deserializer = deserializer;
        String path = fileSplit.getPath().toString();
        long start = fileSplit.getStart();
        long length = fileSplit.getLength();
        // data source information, for connecting to data source.
        DataSource dataSource;
        if (ndpPredicateInfo.getDataFormat().toLowerCase(Locale.ENGLISH).contains("parquet")) {
            dataSource = new HdfsParquetDataSource(path, start, length, false);
        } else {
            dataSource = new HdfsOrcDataSource(path, start, length, false);
        }
        if (ndpPredicateInfo.getHasPartitionColumn()) {
            addPartitionValues(ndpPredicateInfo, path, HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
        }
        this.omniDataHosts = NdpStatusManager.getOmniDataHosts(conf, fileSplit,
                OmniDataConf.getOmniDataReplicationNum(conf));
        this.taskSource = new TaskSource(dataSource, ndpPredicateInfo.getPredicate(), 1048576);
    }

    public Queue<ColumnVector[]> getBatchFromOmniData() throws UnknownHostException {
        Queue<ColumnVector[]> pages = new LinkedList<>();
        int failedTimes = 0;
        Properties properties = new Properties();
        //  If the OmniData task fails due to an exception, the task will look for the next available OmniData host
        for (String omniDataHost : omniDataHosts) {
            String ipAddress = InetAddress.getByName(omniDataHost).getHostAddress();
            properties.put("omnidata.client.target.list", ipAddress);
            DataReaderImpl<List<ColumnVector[]>> dataReader = null;
            try {
                dataReader = new DataReaderImpl<>(properties, taskSource, deserializer);
                do {
                    List<ColumnVector[]> page = dataReader.getNextPageBlocking();
                    if (page != null) {
                        pages.addAll(page);
                    }
                } while (!dataReader.isFinished());
                dataReader.close();
                break;
            } catch (OmniDataException omniDataException) {
                LOGGER.warn("OmniDataAdapter failed node info [hostname :{}]", omniDataHost);
                failedTimes++;
                pages.clear();
                if (dataReader != null) {
                    dataReader.close();
                }
            } catch (Exception e) {
                LOGGER.error("OmniDataAdapter getBatchFromOmnidata() has error:", e);
                failedTimes++;
                pages.clear();
                if (dataReader != null) {
                    dataReader.close();
                }
            }
        }
        int retryTime = Math.min(TASK_FAILED_TIMES, omniDataHosts.size());
        if (failedTimes >= retryTime) {
            LOGGER.warn("No OmniData Server to connect, task has tried {} times.", retryTime);
            throw new TaskExecutionException("No OmniData Server to connect");
        }
        return pages;
    }
}