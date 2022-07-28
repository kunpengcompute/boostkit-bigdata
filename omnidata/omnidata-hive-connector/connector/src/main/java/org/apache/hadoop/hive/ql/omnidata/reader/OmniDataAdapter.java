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
import com.huawei.boostkit.omnidata.exception.OmniErrorCode;
import com.huawei.boostkit.omnidata.model.Predicate;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsDataSource;
import com.huawei.boostkit.omnidata.reader.impl.DataReaderImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.config.NdpConf;
import org.apache.hadoop.hive.ql.omnidata.decode.PageDeserializer;
import org.apache.hadoop.hive.ql.omnidata.decode.type.DecodeType;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusManager;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


/**
 * Obtains data from OmniData through OmniDataAdapter and converts the data into Hive List<ColumnVector[]>.
 * If the OmniData task fails due to an exception, the task will be retried.
 * The maximum number of retry times is 4.
 */
public class OmniDataAdapter implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OmniDataAdapter.class);

    private static final int TASK_FAILED_TIMES = 4;

    private DataSource dataSource;

    private Queue<ColumnVector[]> batchVectors;

    private NdpPredicateInfo ndpPredicateInfo;

    private List<String> omniDataHosts;

    private int ndpReplicationNum;

    public OmniDataAdapter(DataSource dataSource, Configuration conf, FileSplit fileSplit,
                           NdpPredicateInfo ndpPredicateInfo) {
        this.dataSource = dataSource;
        if (dataSource instanceof HdfsDataSource && ndpPredicateInfo.getHasPartitionColumn()) {
            this.ndpPredicateInfo = addPartitionValues(ndpPredicateInfo, ((HdfsDataSource) dataSource).getPath(),
                    HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
        } else {
            this.ndpPredicateInfo = ndpPredicateInfo;
        }
        ndpReplicationNum = NdpConf.getNdpReplicationNum(conf);
        omniDataHosts = getOmniDataHosts(conf, fileSplit);
    }

    private List<String> getOmniDataHosts(Configuration conf, FileSplit fileSplit) {
        List<String> omniDataHosts = new ArrayList<>();
        List<String> dataNodeHosts = getDataNodeHosts(conf, fileSplit);
        // shuffle
        Collections.shuffle(dataNodeHosts);
        dataNodeHosts.forEach(dn -> {
            // possibly null
            if (conf.get(dn) != null) {
                omniDataHosts.add(conf.get(dn));
            }
        });
        // add a random available datanode
        String randomDataNodeHost = NdpStatusManager.getRandomAvailableDataNodeHost(conf, dataNodeHosts);
        if (randomDataNodeHost.length() > 0 && conf.get(randomDataNodeHost) != null) {
            omniDataHosts.add(conf.get(randomDataNodeHost));
        }
        return omniDataHosts;
    }

    private List<String> getDataNodeHosts(Configuration conf, FileSplit fileSplit) {
        List<String> hosts = new ArrayList<>();
        try {
            BlockLocation[] blockLocations = fileSplit.getPath()
                    .getFileSystem(conf)
                    .getFileBlockLocations(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength());
            for (BlockLocation block : blockLocations) {
                for (String host : block.getHosts()) {
                    if ("localhost".equals(host)) {
                        List<String> dataNodeHosts = new ArrayList<>(
                                Arrays.asList(conf.get(NdpStatusManager.NDP_DATANODE_HOSTNAMES)
                                        .split(NdpStatusManager.NDP_DATANODE_HOSTNAME_SEPARATOR)));
                        if (dataNodeHosts.size() > ndpReplicationNum) {
                            hosts.addAll(dataNodeHosts.subList(0, ndpReplicationNum));
                        } else {
                            hosts.addAll(dataNodeHosts);
                        }
                        return hosts;
                    } else {
                        hosts.add(host);
                    }
                    if (ndpReplicationNum == hosts.size()) {
                        return hosts;
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("OmniDataAdapter getDataNodeHosts() failed", e);
        }
        return hosts;
    }

    public Queue<ColumnVector[]> getBatchFromOmniData() throws UnknownHostException {
        Predicate predicate = ndpPredicateInfo.getPredicate();
        TaskSource taskSource = new TaskSource(dataSource, predicate, 1048576);
        DecodeType[] columnTypes = new DecodeType[ndpPredicateInfo.getDecodeTypes().size()];
        for (int index = 0; index < columnTypes.length; index++) {
            String codeType = ndpPredicateInfo.getDecodeTypes().get(index);
            if (ndpPredicateInfo.getDecodeTypesWithAgg().get(index)) {
                columnTypes[index] = OmniDataUtils.transOmniDataAggDecodeType(codeType);
            } else {
                columnTypes[index] = OmniDataUtils.transOmniDataDecodeType(codeType);
            }
        }

        PageDeserializer deserializer = new PageDeserializer(columnTypes);

        Queue<ColumnVector[]> pages = new LinkedList<>();
        int failedTimes = 0;
        Properties properties = new Properties();
        for (String omniDataHost : omniDataHosts) {
            String ipAddress = InetAddress.getByName(omniDataHost).getHostAddress();
            properties.put("omnidata.client.target.list", ipAddress);
            DataReaderImpl<List<ColumnVector[]>> dataReader = null;
            try {
                dataReader = new DataReaderImpl<>(properties, taskSource,
                        deserializer);
                do {
                    List<ColumnVector[]> page = dataReader.getNextPageBlocking();
                    if (page != null) {
                        pages.addAll(page);
                    }
                } while (!dataReader.isFinished());
                break;
            } catch (OmniDataException omniDataException) {
                LOGGER.warn("OmniDataAdapter failed node info [hostname :{}]", omniDataHost);
                OmniErrorCode errorCode = omniDataException.getErrorCode();
                switch (errorCode) {
                    case OMNIDATA_INSUFFICIENT_RESOURCES:
                        LOGGER.warn(
                                "OMNIDATA_INSUFFICIENT_RESOURCES: OmniData Server's push down queue is full, begin to find next OmniData-server");
                        break;
                    case OMNIDATA_UNSUPPORTED_OPERATOR:
                        LOGGER.warn("OMNIDATA_UNSUPPORTED_OPERATOR: Exist unsupported operator");
                        break;
                    case OMNIDATA_GENERIC_ERROR:
                        LOGGER.warn(
                                "OMNIDATA_GENERIC_ERROR: Current OmniData Server unavailable, begin to find next OmniData Server");
                        break;
                    case OMNIDATA_NOT_FOUND:
                        LOGGER.warn(
                                "OMNIDATA_NOT_FOUND: Current OmniData Server not found, begin to find next OmniData Server");
                        break;
                    case OMNIDATA_INVALID_ARGUMENT:
                        LOGGER.warn("OMNIDATA_INVALID_ARGUMENT: Exist unsupported operator or datatype");
                        break;
                    case OMNIDATA_IO_ERROR:
                        LOGGER.warn(
                                "OMNIDATA_IO_ERROR: Current OmniData Server io exception, begin to find next OmniData Server");
                        break;
                    default:
                        LOGGER.warn("OmniDataException: OMNIDATA_ERROR.");
                }
                failedTimes++;
            } catch (Exception e) {
                LOGGER.error("OmniDataAdapter getBatchFromOmnidata() has error:", e);
                failedTimes++;
            } finally {
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

    public boolean nextBatchFromOmniData(VectorizedRowBatch batch) throws UnknownHostException {
        if (batchVectors == null) {
            batchVectors = getBatchFromOmniData();
        }
        if (!batchVectors.isEmpty()) {
            ColumnVector[] batchVector = batchVectors.poll();
            // channelCount: column, positionCount: row
            int channelCount = batchVector.length;
            int positionCount = batchVector[0].isNull.length;
            if (ndpPredicateInfo.getIsPushDownAgg()) {
                // agg raw return
                System.arraycopy(batchVector, 0, batch.cols, 0, channelCount);
            } else {
                for (int i = 0; i < channelCount; i++) {
                    int columnId = ndpPredicateInfo.getOutputColumns().get(i);
                    batch.cols[columnId] = batchVector[i];
                }
            }
            batch.size = positionCount;
            return true;
        }
        return false;
    }

}