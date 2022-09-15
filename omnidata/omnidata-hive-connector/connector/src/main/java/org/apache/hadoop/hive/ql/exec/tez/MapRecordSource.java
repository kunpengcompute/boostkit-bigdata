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


import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.tez.tools.KeyValueInputMerger;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.decode.PageDeserializer;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.reader.OmniDataReader;
import org.apache.hadoop.hive.ql.omnidata.serialize.NdpSerializationUtils;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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

    private final Logger LOG = LoggerFactory.getLogger(MapRecordSource.class);

    /**
     * Maximum number of concurrent connections in the omnidata thread pool
     */
    public static final int MAX_THREAD_NUMS = 16;

    private ExecMapperContext execContext;

    private AbstractMapOperator mapOp;

    private KeyValueReader reader;

    private final boolean grouped = false;

    private TypeInfo[] rowColumnTypeInfos;

    private ArrayList<FileSplit> fileSplits = new ArrayList<>();

    private NdpPredicateInfo ndpPredicateInfo;

    private boolean isOmniDataPushDown = false;

    private JobConf jconf;

    private ExecutorService executorService;

    void init(JobConf jconf, AbstractMapOperator mapOp, KeyValueReader reader) throws IOException {
        execContext = mapOp.getExecContext();
        this.mapOp = mapOp;
        if (reader instanceof KeyValueInputMerger) {
            KeyValueInputMerger kvMerger = (KeyValueInputMerger) reader;
            kvMerger.setIOCxt(execContext.getIoCxt());
        }
        this.reader = reader;
        if (mapOp.getChildOperators().get(0).getConf() instanceof TableScanDesc) {
            TableScanDesc tableScanDesc = (TableScanDesc) mapOp.getChildOperators().get(0).getConf();
            this.ndpPredicateInfo = NdpSerializationUtils.deserializeNdpPredicateInfo(
                    tableScanDesc.getNdpPredicateInfoStr());
            this.isOmniDataPushDown = ndpPredicateInfo.getIsPushDown();
            if (isOmniDataPushDown) {
                this.jconf = jconf;
                this.rowColumnTypeInfos = tableScanDesc.getRowColumnTypeInfos();
                initOmniData();
            }
        }
    }

    /**
     * Save InputSplit to fileSplits.
     * Create a thread pool (executorService) based on parameters.
     */
    private void initOmniData() {
        List<InputSplit> inputSplits = ((TezGroupedSplit) ((MRReaderMapred) reader).getSplit()).getGroupedSplits();
        int splitSize = inputSplits.size();
        // init fileSplits
        inputSplits.forEach(is -> fileSplits.add((FileSplit) is));
        int threadNums = OmniDataConf.getOmniDataOptimizedThreadNums(jconf);
        // Need to limit the maximum number of threads to avoid out of memory.
        threadNums = Math.min(splitSize, threadNums);
        // init executorService
        executorService = new ThreadPoolExecutor(threadNums, threadNums, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat("omnidata-hive-optimized-thread-%d").build());
    }

    @Override
    public final boolean isGrouped() {
        return grouped;
    }

    @Override
    public boolean pushRecord() throws HiveException {
        execContext.resetRow();
        if (isOmniDataPushDown) {
            return pushOmniDataRecord();
        } else {
            return pushRawRecord();
        }
    }

    private boolean pushOmniDataRecord() throws HiveException {
        // create DecodeType
        DecodeType[] columnTypes = new DecodeType[ndpPredicateInfo.getDecodeTypes().size()];
        for (int index = 0; index < columnTypes.length; index++) {
            String codeType = ndpPredicateInfo.getDecodeTypes().get(index);
            // If the returned data type is Agg, use transOmniDataAggDecodeType() method.
            if (ndpPredicateInfo.getDecodeTypesWithAgg().get(index)) {
                columnTypes[index] = OmniDataUtils.transOmniDataAggDecodeType(codeType);
            } else {
                columnTypes[index] = OmniDataUtils.transOmniDataDecodeType(codeType);
            }
        }
        PageDeserializer deserializer = new PageDeserializer(columnTypes);
        ArrayList<Future<Queue<VectorizedRowBatch>>> results = new ArrayList<>();
        fileSplits.forEach(fs -> {
            Future<Queue<VectorizedRowBatch>> future = executorService.submit(
                    new OmniDataReader(jconf, fs, deserializer, ndpPredicateInfo, rowColumnTypeInfos));
            results.add(future);
        });
        for (Future<Queue<VectorizedRowBatch>> future : results) {
            try {
                Queue<VectorizedRowBatch> rowBatches = future.get();
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
        MRReader mrReader = (MRReader) reader;
        try {
            mrReader.close();
        } catch (IOException ex) {
            LOG.error("Failed to close the reader; ignoring", ex);
        }
    }
}