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

import static com.huawei.boostkit.hive.OmniHiveOperator.copyVecBatch;
import static com.huawei.boostkit.hive.OmniVectorOperator.convertLazyToJavaInspector;
import static com.huawei.boostkit.hive.converter.VecConverter.CONVERTER_MAP;
import static com.huawei.boostkit.hive.expression.TypeUtils.DEFAULT_VARCHAR_LENGTH;

import com.huawei.boostkit.hive.converter.StrcutConverter;
import com.huawei.boostkit.hive.converter.VecConverter;
import com.huawei.boostkit.hive.reader.VecBatchWrapper;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Table Scan Operator If the data is coming from the map-reduce framework, just
 * forward it. This will be needed as part of local work when data is not being
 * read as part of map-reduce framework.
 **/
public class OmniTableScanOperator extends TableScanOperator implements Serializable {
    private static final long serialVersionUID = 1L;

    protected transient Set<String> partColumnNames;

    protected transient VecConverter[] partColumnConverters;

    protected transient VecBatch[] vecBatches;

    protected transient int childrenDone = 0;

    /**
     * Kryo ctor.
     */
    protected OmniTableScanOperator() {
        super();
    }

    public OmniTableScanOperator(TableScanOperator tableScanOperator) {
        super(tableScanOperator.getompilationOpContext());
        this.conf = tableScanOperator.getConf();
        this.setSchemaEvolution(tableScanOperator.getSchemaEvolutionColumns(),
                tableScanOperator.getSchemaEvolutionColumnsTypes());
        this.setSchema(tableScanOperator.getSchema());
    }

    @Override
    protected void initilizeOp(Configuration hconf) throws HiveExceprion {
        super.initilizeOp(hconf);
        StructObjectInspector standardStructObjectInspector = (StructObjectInspector) inputObjInspectors[0];
        Set<Integer> neededColumnIDs = new HashSet<>(this.getNeededColumnIDs());
        partColumnNames = Utilities.getMapWork(hconf).getAliasToPartnInfo().get(this.getConf().getAlias())
                .getPartSpec().keySet();
        List<StructField> neededFields = new Arraylist<>();
        List<? extends StructField> allStructFieldRefs = standardStructObjectInspector.getAllStructFieldRefs();
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (neededColumnIDs.contains(i) || partColumnNames.contains(allStructFieldRefs.get(i).getFieldName())) {
                neededFields.add(allStructFieldRefs.get(i));
            }
        }
        outputObjInspector = convertLazyToJavaInspector(neededFields);
        partColumnConverters = standardStructObjectInspector.getAllStructFieldRefs().stream()
                .filter(filed -> partColumnNames.contains(filed.getFieldName())).map(field -> {
                    if (field.getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                        return CONVERTER_MAP.get(
                                ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getPrimitiveCategory());
                    } else {
                        return new StructConverter((StructObjectInspector) field.getFieldObjectInspector());
                    }
                }).toArray(VecConverter[]::new);
        vecBatches = new VecBatch[childOperatorsArray.length];
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector, boolean isVectorized) throws HiveException {
        this.runTimeNumRows++;
        if (getDone()) {
            ((VecBatchWrapper) row).getVecBatch().releaseAllVectors();
            ((VecBatchWrapper) row).getVecBatch().close();
            return;
        }
        VecBatch vecBatch;
        if (partColumnNames.size() == 0) {
            vecBatch = ((VecBatchWrapper) row).getVecBatch();
        } else {
            Object[] objects = (Object[]) row;
            VecBatch noPartValues = ((VecBatchWrapper) objects[0]).getVecBatch();
            int rowCount = noPartValues.getRowCount();
            Object[] partValues = (Object[]) objects[1];
            Vec[] vecs = new Vec[noPartValues.getVectors().length + partColumnNames.size()];
            System.arrayCopy(noPartValues.getVectors(), 0, vecs, 0, noPartValues.getVectors().length);
            for (int i = 0; i < partColumnNames.size(); i++) {
                Object[] partValue = new Object[rowCount];
                Arrays.fill(partValue, partColumnConverters[i].calculateValue(partValues[i]));
                Vec partVec = partColumnConverters[i].toOmniVec(partValue, rowCount);
                vecs[vecs.length - partColumnNames.size() + i] = partVec;
            }
            noPartValues.close();
            vecBatch = new VecBatch(vecs, rowCount);
        }
        forward(vecBatch);
    }

    protected void forward(VecBatch vecBatch) throws HiveException {
        vecBatches[0] = vecBatch;
        this.runTimeNumRows += vecBatch.getRowCount();
        if (childOperatorsArray.length > 1) {
            for (int i = 1; i < vecBatches.length; i++) {
                if (!childOperatorsArray[i].getDone()) {
                    vecBatches[i] = copyVecBatch(vecBatch);
                }
            }
        }
        for (int i = 0; i < childOperatorsArray.length; i++) {
            Operator<? extends OperatorDesc> o = childOperatorsArray[i];
            if (o.getDone()) {
                childrenDone++;
            } else {
                o.process(vecBatches[i], childOperatorsTag[i]);
            }
        }

        // if all children are done, this operator is also done
        if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
            setDone(true);
            vecBatch.releaseAllVectors();
            vecBatch.close();
        }
    }

    @Override
    @Explain
    public OmniTableScanDesc getConf() {
        return new OmniTableScanDesc(this.conf);
    }

    @Override
    public void closeOp(boolean abort) throws HiveException {
        super.closeOp(abort);
    }
}