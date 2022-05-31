/*
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

package org.apache.hadoop.hive.ql.exec.tez;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsOrcDataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.tez.tools.KeyValueInputMerger;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.config.NdpConf;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.reader.OmniDataReader;
import org.apache.hadoop.hive.ql.omnidata.serialize.NdpSerializationUtils;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.split.TezGroupedSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.lib.MRReaderMapred;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Process input from tez LogicalInput and write output - for a map plan Just pump the records
 * through the query plan.
 */

public class MapRecordSource implements RecordSource {

    public static final Logger LOG = LoggerFactory.getLogger(MapRecordSource.class);
    private ExecMapperContext execContext;
    private AbstractMapOperator mapOp;
    private KeyValueReader reader;
    private final boolean grouped = false;
    private boolean aggOptimized = false;
    private ArrayList<Map<String, Object>> omniDataReaders = new ArrayList<>();

    void init(JobConf jconf, AbstractMapOperator mapOp, KeyValueReader reader) throws IOException {
        execContext = mapOp.getExecContext();
        this.mapOp = mapOp;
        if (reader instanceof KeyValueInputMerger) {
            KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
            kvMerger.setIOCxt(execContext.getIoCxt());
        }
        this.reader = reader;
        aggOptimized = Boolean.parseBoolean(jconf.get(NdpConf.NDP_AGG_OPTIMIZED_ENABLE));
        if (aggOptimized) {
            createOmniDataReader(jconf);
        }
    }

    private void createOmniDataReader(JobConf jconf) {
        String ndpPredicateInfoStr = ((TableScanDesc) mapOp.getChildOperators()
                .get(0)
                .getConf()).getNdpPredicateInfoStr();
        NdpPredicateInfo ndpPredicateInfo = NdpSerializationUtils.deserializeNdpPredicateInfo(ndpPredicateInfoStr);
        List<InputSplit> inputSplits = ((TezGroupedSplit) ((MRReaderMapred) reader).getSplit()).getGroupedSplits();
        for (InputSplit inputSplit : inputSplits) {
            if (inputSplit instanceof FileSplit) {
                String path = ((FileSplit) inputSplit).getPath().toString();
                long start = ((FileSplit) inputSplit).getStart();
                long length = ((FileSplit) inputSplit).getLength();
                String tableName = mapOp.getConf().getAliases().get(0);
                String inputFormat = mapOp.getConf()
                        .getAliasToPartnInfo()
                        .get(tableName)
                        .getInputFileFormatClass()
                        .getSimpleName();
                DataSource dataSource;
                if (inputFormat.toLowerCase(Locale.ENGLISH).contains("parquet")) {
                    dataSource = new HdfsParquetDataSource(path, start, length, false);
                } else {
                    dataSource = new HdfsOrcDataSource(path, start, length, false);
                }
                Map<String, Object> adapterInfo = new HashMap<String, Object>() {{
                    put("dataSource", dataSource);
                    put("jconf", jconf);
                    put("inputSplit", inputSplit);
                    put("ndpPredicateInfo", ndpPredicateInfo);
                }};
                omniDataReaders.add(adapterInfo);
            }
        }
    }

    @Override
    public final boolean isGrouped() {
        return grouped;
    }

    @Override
    public boolean pushRecord() throws HiveException {
        execContext.resetRow();

        if (aggOptimized) {
            return pushOmniDataRecord();
        } else {
            return pushRawRecord();
        }

    }

    private boolean pushOmniDataRecord() throws HiveException {
        ExecutorService executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("omnidata-hive-thread-%d").build());
        ArrayList<Future<List<VectorizedRowBatch>>> results = new ArrayList<>();
        omniDataReaders.forEach(ai -> {
            Future<List<VectorizedRowBatch>> future = executorService.submit(
                    new OmniDataReader((DataSource) ai.get("dataSource"), (Configuration) ai.get("jconf"),
                            (FileSplit) ai.get("inputSplit"), (NdpPredicateInfo) ai.get("ndpPredicateInfo")));
            results.add(future);
        });
        for (Future<List<VectorizedRowBatch>> future : results) {
            try {
                List<VectorizedRowBatch> rowBatches = future.get();
                for (VectorizedRowBatch rowBatch : rowBatches) {
                    mapOp.process(rowBatch);
                }
            } catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
            }
        }
        executorService.shutdown();
        return false;
    }

    private boolean pushRawRecord() throws HiveException {
        try {
            if (reader.next()) {
                Object value;
                try {
                    value = reader.getCurrentValue();
                } catch (IOException e) {
                    closeReader();
                    throw new HiveException(e);
                }
                return processRow(value);
            }
        } catch (IOException e) {
            closeReader();
            throw new HiveException(e);
        }
        return false;
    }

    private boolean processRow(Object value) {
        try {
            if (mapOp.getDone()) {
                return false; // done
            } else {
                // Since there is no concept of a group, we don't invoke
                // startGroup/endGroup for a mapper
                mapOp.process((Writable) value);
            }
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                // Don't create a new object if we are already out of memory
                throw (OutOfMemoryError) e;
            } else {
                LOG.error(StringUtils.stringifyException(e));
                closeReader();
                throw new RuntimeException(e);
            }
        }
        return true; // give me more
    }

    private void closeReader() {
        if (!(reader instanceof MRReader)) {
            LOG.warn("Cannot close " + (reader == null ? null : reader.getClass()));
            return;
        }
        if (reader instanceof KeyValueInputMerger) {
            // cleanup
            KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
            kvMerger.clean();
        }

        LOG.info("Closing MRReader on error");
        MRReader mrReader = (MRReader)reader;
        try {
            mrReader.close();
        } catch (IOException ex) {
            LOG.error("Failed to close the reader; ignoring", ex);
        }
    }
}