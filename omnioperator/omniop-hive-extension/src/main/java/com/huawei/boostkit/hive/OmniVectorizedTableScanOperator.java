/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.reader.VecBatchWrapper;

import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Iterator;

public class OmniVectorizedTableScanOperator extends OmniTableScanOperator {
    private static final long serialVersionUID = 1L;

    /**
     * Kryo ctor.
     */
    protected OmniVectorizedTableScanOperator() {
        super();
    }

    public OmniVectorizedTableScanOperator(TableScanOperator tableScanOperator) {
        super(tableScanOperator);
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector, boolean isVectorized) throws HiveException {
        this.runTimeNumRows += ((VecBatchWrapper) row).getVecBatch().getRowCount();
        if (getDone()) {
            ((VecBatchWrapper) row).getVecBatch().releaseAllVectors();
            ((VecBatchWrapper) row).getVecBatch().close();
            return;
        }
        VecBatch vecBatch = ((VecBatchWrapper) row).getVecBatch();
        forward(vecBatch);
    }
}