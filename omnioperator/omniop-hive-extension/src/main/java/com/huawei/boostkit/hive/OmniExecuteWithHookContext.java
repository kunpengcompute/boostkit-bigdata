/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive;

import static com.huawei.boostkit.hive.expression.TypeUtils.checkUnsupportedTimestamp;
import static com.huawei.boostkit.hive.expression.TypeUtils.checkOmniJsonWhiteList;
import static com.huawei.boostkit.hive.expression.TypeUtils.checkUnsupportedArithmetic;
import static com.huawei.boostkit.hive.expression.TypeUtils.checkUnsupportedCast;
import static com.huawei.boostkit.hive.expression.TypeUtils.convertHiveTypeToOmniType;
import static com.huawei.boostkit.hive.expression.TypeUtils.isValidConversion;
import static com.huawei.boostkit.hive.expression.TypeUtils.isValidFilterExpression;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.CHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DATE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DECIMAL;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.LONG;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.SHORT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.VARCHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.VOID;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.reader.OmniOrcInputFormat;
import com.huawei.boostkit.hive.reader.OmniParquetInputFormat;
import com.huawei.boostkit.hive.reader.VecBatchWrapperSerde;
import com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe;

import nova.hetu.omniruntime.constants.FunctionType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinBaseOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.VectorTableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class OmniExecuteWithHookContext implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(OmniExecuteWithHookContext.class.getName());
    public static final Set<OperatorType> OMNI_OPERATOR = new HashSet<>(Arrays.asList(OperatorType.JOIN,
            OperatorType.MAPJOIN, OperatorType.MERGEJOIN, OperatorType.GROUPBY, OperatorType.SELECT,
            OperatorType.FILTER, OperatorType.PTF, OperatorType.TABLESCAN, OperatorType.REDUCESINK));
    public static final Set<Integer> SUPPORTED_JOIN = new HashSet<>(Arrays.asList(JoinDesc.INNER_JOIN,
            JoinDesc.LEFT_OUTER_JOIN, JoinDesc.FULL_OUTER_JOIN, JoinDesc.LEFT_SEMI_JOIN));
    private static final Set<PrimitiveObjectInspector.PrimitiveCategory> SUPPORTED_TYPE = new HashSet<>(Arrays.asList(BOOLEAN,
            SHORT, INT, LONG, DOUBLE, STRING, DATE, DECIMAL, VARCHAR, CHAR, VOID));

    private static final int DECIMAL64_MAX_PRECISION = 18;

    private final Set<Operator> groupByOperatorNeedSort = new HashSet<>();

    // indicate the parents task name of map join operators which can not replace
    private final Set<String> parentOfMapJoinOpCannotReplace = new HashSet<>();
    private final List<Operator> mergeJoinNeedSort = new ArrayList<>();
    private final List<Operator<? extends OperatorDesc>> replaceable = new ArrayList<>();
    private final List<OperatorInfo> replaceableInfo = new ArrayList<>();

    private final Set<BaseWork> reduceSinkReplaceableWork = new HashSet<>();

    private Set<String> reduceSinkReplaceableWorkName = new HashSet<>();

    private final Set<BaseWork> inputUnReplaceableWork = new HashSet<>();

    private TezWork tezWork;

    private OmniHiveConf omniHiveConf;

    private void clearGlobalVar() {
        groupByOperatorNeedSort.clear();
        mergeJoinNeedSort.clear();
        replaceable.clear();
        replaceableInfo.clear();
        parentOfMapJoinOpCannotReplace.clear();
        reduceSinkReplaceableWork.clear();
        reduceSinkReplaceableWorkName.clear();
        inputUnReplaceableWork.clear();
        tezWork = null;
    }

    private boolean isSupportSqlType(QueryPlan queryPlan) {
        if (queryPlan.getQueryProperties() != null && queryPlan.getQueryProperties().isQuery() && queryPlan.getOperationName().equals("QUERY")) {
            return true;
        }
        return queryPlan.getRootTasks().get(0) instanceof ExplainTask;
    }

    public void run(HookContext hookContext) throws Exception {
        omniHiveConf = new OmniHiveConf(hookContext.getConf());
        clearGlobalVar();
        QueryPlan queryPlan = hookContext.getQueryPlan();
        if (queryPlan.getRootTasks().isEmpty()) {
            return;
        }
        if (!isSupportSqlType(queryPlan)) {
            return;
        }
        String engine = HiveConf.getVar(hookContext.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
        if (engine.equals("mr")) {
            throw new RuntimeException("can't support mr engine");
        }
        boolean cboEnable = HiveConf.getBoolVar(hookContext.getConf(), HiveConf.ConfVars.HIVE_CBO_ENABLED);
        boolean cboRetPath = HiveConf.getBoolVar(hookContext.getConf(), HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP);
        if (!cboEnable) {
            return;
        }
        if (cboRetPath) {
            return;
        }
        setTezWork(queryPlan);
        if (tezWork == null || checkDataType()) {
            return;
        }
        if (OMNI_OPERATOR.contains(OperatorType.REDUCESINK)) {
            initMapJoinOpToWork();
            initReduceSinkReplaceable();
            initMergeJoinNeedSort();
        }
        traverseTezWorks();
        for (OperatorInfo operatorInfo : replaceableInfo) {
            replaceWithOmniOperator(operatorInfo);
        }
        if (OMNI_OPERATOR.contains(OperatorType.REDUCESINK)) {
            replaceSerializationLib();
            replaceSerializationLibForMapJoinOperator();
            setReduceSinkChildWorkUnVectorized();
        }
    }

    private void setTezWork(QueryPlan queryPlan) {
        Task rootTask = queryPlan.getRootTasks().get(0);
        if (rootTask instanceof ExplainTask && !((ExplainTask) rootTask).getWork().getRootTasks().isEmpty()) {
            tezWork = (TezWork) ((ExplainTask) rootTask).getWork().getRootTasks().get(0).getWork();
        } else if (rootTask instanceof TezTask) {
            tezWork = (TezWork) rootTask.getWork();
        }
    }


    private void initMapJoinOpToWork() {
        for (BaseWork work : tezWork.getAllWork()) {
            if (work instanceof UnionWork) {
                continue;
            }
            for (Operator<? extends OperatorDesc> op : work.getAllOperators()) {
                if (op.getType().equals(OperatorType.MAPJOIN) && !isReplaceable(op, true)) {
                    MapJoinDesc conf = (MapJoinDesc) op.getConf();
                    Map<Integer, String> parentToInput = conf.getParentToInput();
                    parentOfMapJoinOpCannotReplace.addAll(parentToInput.values());
                }
            }
        }
    }

    private void initReduceSinkReplaceable() {
        for (BaseWork work : tezWork.getAllWork()) {
            if (work instanceof UnionWork) {
                continue;
            }
            if (getReduceSinkReplaceable(work)) {
                reduceSinkReplaceableWork.add(work);
            } else {
                inputUnReplaceableWork.addAll(tezWork.getChildren(work));
                for (BaseWork child : tezWork.getChildren(work)) {
                    reduceSinkReplaceableWork.removeAll(tezWork.getParents(child));
                }
                dealUnionWork(work);
            }
        }
        reduceSinkReplaceableWorkName = reduceSinkReplaceableWork.stream().map(BaseWork::getName).collect(Collectors.toSet());
    }

    private void dealUnionWork(BaseWork work) {
        for (BaseWork parent : tezWork.getParents(work)) {
            if (!(tezWork.getEdgeType(parent, work) == TezEdgeProperty.EdgeType.CONTAINS)) {
                continue;
            }
            inputUnReplaceableWork.add(parent);
            reduceSinkReplaceableWork.remove(parent);
            for (BaseWork parentChild : tezWork.getChildren(parent)) {
                if (tezWork.getEdgeType(parent, parentChild) == TezEdgeProperty.EdgeType.CONTAINS) {
                    reduceSinkReplaceableWork.remove(parentChild);
                }
            }
        }
    }

    private boolean getReduceSinkReplaceable(BaseWork work) {
        boolean reduceSinkCanReplace = false;
        if (parentOfMapJoinOpCannotReplace.contains(work.getName()) || !reduceSinkOperatorReplaceable(work)) {
            return false;
        }
        for (BaseWork child : tezWork.getChildren(work)) {
            TezEdgeProperty.EdgeType edgeType = tezWork.getEdgeType(work, child);
            if (edgeType == TezEdgeProperty.EdgeType.CONTAINS) {
                continue;
            }
            reduceSinkCanReplace = replaceableForChild(work, child);
            if (!reduceSinkCanReplace) {
                return false;
            }
        }
        // deal with Union work
        for (BaseWork parent : tezWork.getParents(work)) {
            TezEdgeProperty.EdgeType edgeType = tezWork.getEdgeType(parent, work);
            if (edgeType == TezEdgeProperty.EdgeType.CONTAINS && !inputUnReplaceableWork.contains(parent) && getReduceSinkReplaceable(parent)) {
                reduceSinkCanReplace = true;
                reduceSinkReplaceableWork.add(parent);
            }
        }
        return reduceSinkCanReplace;
    }

    private boolean replaceableForChild(BaseWork work, BaseWork child) {
        if (inputUnReplaceableWork.contains(child)) {
            return false;
        }
        TezEdgeProperty.EdgeType edgeType = tezWork.getEdgeType(work, child);
        boolean reduceSinkReplaceable = false;
        if (isSupportEdgeType(edgeType) && isReplaceable(child.getAnyRootOperator(), true)) {
            reduceSinkReplaceable = true;
        }
        if (edgeType == TezEdgeProperty.EdgeType.SIMPLE_EDGE && child instanceof ReduceWork) {
            reduceSinkReplaceable = groupByEdgeReplaceable(work, child);
        }
        if (child instanceof MergeJoinWork) {
            Operator reducer = ((MergeJoinWork) child).getMainWork().getAnyRootOperator();
            if (reducer.getType() == null) {
                reducer = (Operator) reducer.getChildOperators().get(0);
            }
            if (reducer.getType() == OperatorType.MERGEJOIN && isReplaceable(reducer, false)) {
                reduceSinkReplaceable = true;
            }
        }
        return reduceSinkReplaceable;
    }

    private boolean groupByEdgeReplaceable(BaseWork work, BaseWork child) {
        Operator reducer = ((ReduceWork) child).getReducer();
        if (reducer.getType() == null) {
            // is VectorOperator
            reducer = (Operator) reducer.getChildOperators().get(0);
        }
        if (reducer.getType().equals(OperatorType.GROUPBY) && isReplaceable(reducer, true)) {
            tezWork.getEdgeProperty(work, child).setEdgeType(TezEdgeProperty.EdgeType.CUSTOM_SIMPLE_EDGE);
            return true;
        }
        return false;
    }

    private boolean reduceSinkOperatorReplaceable(BaseWork work) {
        if (workUnReplaceable(work)) {
            return false;
        }
        for (Operator<? extends OperatorDesc> operator : work.getAllOperators()) {
            if (!operator.getType().equals(OperatorType.REDUCESINK)) {
                continue;
            }
            ReduceSinkDesc reduceSinkDesc = (ReduceSinkDesc) operator.getConf();
            if (reduceSinkDescUnReplaceable(reduceSinkDesc)) {
                return false;
            }
        }
        return true;
    }

    private boolean workUnReplaceable(BaseWork work) {
        if (work instanceof MapWork && !tableScanReplaceable((MapWork) work)) {
            return true;
        }
        if (work instanceof MergeJoinWork) {
            BaseWork mainWork = ((MergeJoinWork) work).getMainWork();
            if (!(mainWork instanceof ReduceWork) || ((ReduceWork) ((MergeJoinWork) work).getMainWork()).getReducer().getType() != OperatorType.MERGEJOIN) {
                return true;
            }
        }
        return false;
    }

    private boolean reduceSinkDescUnReplaceable(ReduceSinkDesc reduceSinkDesc) {
        boolean unReplaceable = false;
        if (reduceSinkDesc.getKeyCols().isEmpty() && !reduceSinkDesc.getReducerTraits().contains(ReduceSinkDesc.ReducerTraits.UNIFORM)
                && reduceSinkDesc.getPartitionCols().size() > 0) {
            unReplaceable = true;
        }
        if (!isUDFSupport(reduceSinkDesc.getKeyCols(), false) ||
                !isUDFSupport(reduceSinkDesc.getValueCols(), false) ||
                !isUDFSupport(reduceSinkDesc.getPartitionCols(), false)) {
            unReplaceable = true;
        }

        if (reduceSinkDesc.getTopN() != -1 || !reduceSinkDesc.getDistinctColumnIndices().isEmpty()) {
            unReplaceable = true;
        }
        for (ExprNodeDesc exprNodeDesc : reduceSinkDesc.getPartitionCols()) {
            if (exprNodeDesc instanceof ExprNodeConstantDesc) {
                unReplaceable = true;
            }
        }
        return unReplaceable;
    }

    private void initMergeJoinNeedSort() {
        for (BaseWork work : tezWork.getAllWork()) {
            if (!(work instanceof MergeJoinWork)) {
                continue;
            }
            Operator reducer = ((MergeJoinWork) work).getMainWork().getAnyRootOperator();
            if (reducer.getType() != OperatorType.MERGEJOIN) {
                continue;
            }
            for (BaseWork parent : tezWork.getParents(work)) {
                TezEdgeProperty.EdgeType edgeType = tezWork.getEdgeType(parent, work);
                boolean needSort = reduceSinkReplaceableWork.contains(parent);
                if (reduceSinkReplaceableWork.contains(parent)) {
                    addMergeJoinNeedSort(edgeType, reducer);
                }
                replaceSimpleEdge(needSort, work, edgeType);
            }
        }
    }

    private void traverseTezWorks() {
        for (BaseWork work : tezWork.getAllWork()) {
            traverseEachWork(work);
        }
    }

    private void traverseEachWork(BaseWork work) {
        if (work instanceof UnionWork) {
            return;
        }
        if (work instanceof MapWork && !tableScanReplaceable((MapWork) work)) {
            return;
        }
        if (work instanceof MergeJoinWork) {
            BaseWork mainWork = ((MergeJoinWork) work).getMainWork();
            if (!(mainWork instanceof ReduceWork) || ((ReduceWork) ((MergeJoinWork) work).getMainWork()).getReducer().getType() != OperatorType.MERGEJOIN) {
                return;
            }
        }
        boolean groupNeedSort = getGroupNeedSort(work);
        boolean reduceSinkCanReplace = reduceSinkReplaceableWork.contains(work);
        VectorizedRowBatchCtx vectorizedRowBatchCtx = work.getVectorizedRowBatchCtx();
        for (Operator<? extends OperatorDesc> op : work.getAllOperators()) {
            traverseTezOperator(op, work, groupNeedSort, reduceSinkCanReplace, vectorizedRowBatchCtx);
        }
    }

    private boolean checkDataType() {
        for (BaseWork work : tezWork.getAllWork()) {
            if (hasUnsupportedDataType(work)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUnsupportedDataType(BaseWork work) {
        boolean hasUnsupported = false;
        if (work instanceof MapWork) {
            hasUnsupported = scanHasUnsupported(work);
        }
        if (hasUnsupported) {
            return true;
        }
        // If project generates new type ,also need to check
        hasUnsupported = selectHasUnsupported(work);
        return hasUnsupported;
    }

    private boolean selectHasUnsupported(BaseWork work) {
        for (Operator op : work.getAllOperators()) {
            if (!op.getType().equals(OperatorType.SELECT)) {
                continue;
            }
            for (ExprNodeDesc exprNodeDesc : ((SelectDesc) op.getConf()).getColList()) {
                if (fieldTypeUnsupported(exprNodeDesc.getTypeInfo())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean scanHasUnsupported(BaseWork work) {
        MapWork mapWork = (MapWork) work;
        for (Operator op : mapWork.getAliasToWork().values()) {
            if (!op.getType().equals(OperatorType.TABLESCAN)) {
                continue;
            }
            RowSchema rowSchema = op.getSchema();
            TableScanDesc tableScanDesc = (TableScanDesc) op.getConf();
            List<String> fieldNames = new ArrayList<>();
            fieldNames.addAll(tableScanDesc.getNeededColumns());
            LinkedHashMap<String, String> partSpec = mapWork.getAliasToPartnInfo().get(tableScanDesc.getAlias()).getPartSpec();
            if (partSpec != null) {
                fieldNames.addAll(partSpec.keySet());
            }
            if (fieldNames.isEmpty()) {
                continue;
            }
            for (String field : fieldNames) {
                TypeInfo typeInfo = rowSchema.getColumnInfo(field).getType();
                if (fieldTypeUnsupported(typeInfo)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean fieldTypeUnsupported(TypeInfo rowSchema) {
        TypeInfo typeInfo = rowSchema;
        if (!(typeInfo instanceof PrimitiveTypeInfo)) {
            return true;
        }
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        if (!SUPPORTED_TYPE.contains(primitiveTypeInfo.getPrimitiveCategory())) {
            return true;
        }
        return false;
    }

    private boolean tableScanReplaceable(MapWork work) {
        for (PartitionDesc partitionDesc : work.getPathToPartitionInfo().values()) {
            if (!checkInputFormat(partitionDesc.getInputFileFormatClassName())) {
                return false;
            }
        }
        for (Operator<? extends OperatorDesc> op : work.getAliasToWork().values()) {
            if (!op.getType().equals(OperatorType.TABLESCAN)) {
                continue;
            }
            return tableScanSupport(op);
        }
        return true;
    }

    private boolean tableScanSupport(Operator<? extends OperatorDesc> op) {
        TableScanDesc tableScanDesc = (TableScanDesc) op.getConf();
        if (tableScanDesc.getNeededColumns().isEmpty() || tableScanDesc.getAcidOperationalProperties() != null) {
            return false;
        }
        Table tableMetadata = tableScanDesc.getTableMetadata();
        if (tableMetadata != null && (!checkInputFormat(tableMetadata.getInputFormatClass().getName()) || tableMetadata.getParameters().getOrDefault("transactional", "").equals("true"))) {
            return false;
        }
        if (tableScanDesc.isVectorized()) {
            TypeInfo[] columnTypeInfos = ((VectorTableScanDesc) tableScanDesc.getVectorDesc()).getProjectedColumnTypeInfos();
            for (int id : tableScanDesc.getNeededColumnIDs()) {
                if (columnTypeInfos[id].getTypeName() == "timestamp") {
                    return false;
                }
            }
        } else if (tableMetadata != null && tableMetadata.getCols() != null) {
            List<FieldSchema> colList = tableMetadata.getCols();
            for (int id : tableScanDesc.getNeededColumnIDs()) {
                if (colList.get(id).getType() == "timestamp") {
                    return false;
                }
            }
        }
        List<Operator<? extends OperatorDesc>> childOperators = op.getChildOperators();
        for (Operator childOperator : childOperators) {
            if (childOperator.getType() != null && childOperator.getType().equals(OperatorType.REDUCESINK) && reduceSinkDescUnReplaceable((ReduceSinkDesc) childOperator.getConf())) {
                return false;
            }
        }
        return true;
    }

    private boolean checkInputFormat(String format) {
        if (format.equals(OrcInputFormat.class.getName()) || format.equals(MapredParquetInputFormat.class.getName())) {
            return true;
        }
        return false;
    }

    private void traverseTezOperator(Operator op, BaseWork work, boolean groupNeedSort, boolean reduceSinkCanReplace, VectorizedRowBatchCtx vectorizedRowBatchCtx) {
        if (!currentReplaceable(op, reduceSinkCanReplace)) {
            return;
        }
        if (work instanceof MapWork && op.getType().equals(OperatorType.TABLESCAN) && isReplaceable(op, false)) {
            replaceInputFormat(work);
            MapWork mapWork = (MapWork) work;
            for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : mapWork.getAliasToWork().entrySet()) {
                if (entry.getValue().equals(op)) {
                    mapWork.getAliasToWork().put(entry.getKey(), replaceWithOmniOperator(new OperatorInfo(op, work.getVectorMode(), vectorizedRowBatchCtx, reduceSinkCanReplace), 0));
                    replaceable.add(op);
                }
            }
        } else if (work instanceof ReduceWork && ((ReduceWork) work).getReducer() == op) {
            if (op.getType() != null && op.getType().equals(OperatorType.GROUPBY) && groupNeedSort) {
                groupByOperatorNeedSort.add(op);
            }
            ReduceWork reduceWork = (ReduceWork) work;
            reduceWork.setReducer(replaceWithOmniOperator(new OperatorInfo(op, work.getVectorMode(), vectorizedRowBatchCtx, reduceSinkCanReplace), reduceWork.getTagToInput().size()));
            replaceable.add(op);
        } else if (work instanceof MergeJoinWork && ((ReduceWork) ((MergeJoinWork) work).getMainWork()).getReducer() == op) {
            ReduceWork reduceWork = (ReduceWork) ((MergeJoinWork) work).getMainWork();
            reduceWork.setReducer(replaceWithOmniOperator(new OperatorInfo(op, work.getVectorMode(), vectorizedRowBatchCtx, reduceSinkCanReplace), reduceWork.getTagToInput().size()));
            replaceable.add(op);
        } else if (isReplaceable(op, reduceSinkCanReplace)) {
            if (op.getType() != null && op.getType().equals(OperatorType.GROUPBY) && groupNeedSort) {
                groupByOperatorNeedSort.add(op);
            }
            replaceableInfo.add(new OperatorInfo(op, work.getVectorMode(), vectorizedRowBatchCtx, reduceSinkCanReplace));
            replaceable.add(op);
        }
    }

    private boolean isReplaceable(Operator<? extends OperatorDesc> operator, boolean reduceSinkCanReplace) {
        if (replaceable.contains(operator)) {
            return true;
        }

        return reCheckReplaceable(operator, reduceSinkCanReplace);
    }

    private boolean reCheckReplaceable(Operator<? extends OperatorDesc> operator, boolean reduceSinkCanReplace) {
        if (operator.getType() != null && operator.getType().equals(OperatorType.MAPJOIN)) {
            if (checkMapJoinInput(operator) && currentReplaceable(operator, reduceSinkCanReplace)) {
                return true;
            }
        }
        return currentReplaceable(operator, reduceSinkCanReplace);
    }

    private boolean currentReplaceable(Operator<? extends OperatorDesc> operator, boolean reduceSinkCanReplace) {
        if (operator.getName().contains("OMNI")) return true;
        if (replaceable.contains(operator) || operator.getType() == null) {
            return true;
        }
        if (!OMNI_OPERATOR.contains(operator.getType())) {
            return false;
        }
        if (!omniHiveConf.isEnableOperator(operator.getType())) {
            return false;
        }
        switch (operator.getType()) {
            case MAPJOIN:
                return mapJoinReplaceable(operator);
            case MERGEJOIN:
                return joinReplaceable(operator);
            case SELECT:
                if (!isUDFSupport(((SelectDesc) operator.getConf()).getColList(), false)) {
                    return false;
                }
                if (OmniSelectOperator.isInvalidSelectColumn(((SelectDesc) operator.getConf()).getColList())) {
                    return false;
                }
                boolean replaceable = true;
                for (Operator parent : operator.getParentOperators()) {
                    replaceable = replaceable && currentReplaceable(parent, reduceSinkCanReplace);
                }
                if (!operator.getChildOperators().isEmpty() && operator.getChildOperators().get(0).getType().equals(OperatorType.PTF)
                        && !currentReplaceable(operator.getChildOperators().get(0), false)) {
                    replaceable = false;
                }
                return replaceable;
            case FILTER:
                List<ExprNodeDesc> colList = Collections.singletonList(((FilterDesc) operator.getConf()).getPredicate());
                if ((!isUDFSupport(colList, true) && !isLegalTimestamp(colList)) || !isLegalFilter(colList)) {
                    return false;
                }
                boolean result = true;
                for (Operator parent : operator.getParentOperators()) {
                    result = result && currentReplaceable(parent, reduceSinkCanReplace);
                }
                for (Operator child : operator.getChildOperators()) {
                    if (child.getType() != null && child.getType().equals(OperatorType.SELECT)) {
                        SelectDesc conf = (SelectDesc) child.getConf();
                        result = result && isUDFSupport(conf.getColList(), false) && isLegalFilter(conf.getColList());
                    }
                }
                return result;
            case PTF:
                return PTFReplaceable(operator);
            case GROUPBY:
                return groupByReplaceable(operator, reduceSinkCanReplace);
            case REDUCESINK:
                return reduceSinkCanReplace;
            case TABLESCAN:
                return tableScanSupport(operator);
            default:
                return true;
        }
    }

    private boolean PTFSupportedExpression(List<PTFExpressionDef> expressions) {
        for (PTFExpressionDef expression : expressions) {
            ExprNodeDesc exprNode = expression.getExprNode();
            if (exprNode instanceof ExprNodeGenericFuncDesc) {
                return false;
            }
            if (exprNode instanceof ExprNodeColumnDesc && !SUPPORTED_TYPE
                    .contains(((PrimitiveTypeInfo) exprNode.getTypeInfo()).getPrimitiveCategory())) {
                return false;
            }
        }
        return true;
    }

    private boolean PTFSupportedAgg(List<WindowFunctionDef> windowFunctionDefs) {
        for (int i = 0; i < windowFunctionDefs.size(); i++) {
            WindowFunctionDef windowFunctionDef = windowFunctionDefs.get(i);
            BoundaryDef start = windowFunctionDef.getWindowFrame().getStart();
            BoundaryDef end = windowFunctionDef.getWindowFrame().getEnd();
            if (start.getAmt() != Integer.MAX_VALUE && end.getAmt() != Integer.MAX_VALUE) {
                return false;
            }
            FunctionType windowFunctionType = TypeUtils.getWindowFunctionType(windowFunctionDef);
            List<PTFExpressionDef> args = windowFunctionDefs.get(i).getArgs();
            boolean isCountAll = (windowFunctionDef.getName().equals("count") && windowFunctionDef.isStar());
            if (!PTFSupportedFunction(windowFunctionType, args, isCountAll)) {
                return false;
            }
        }
        return true;
    }

    private boolean PTFSupportedFunction(FunctionType windowFunctionType, List<PTFExpressionDef> args, boolean isCountAll) {
        if (args != null) {
            if (windowFunctionType == null || isPTFAggUnsupported(windowFunctionType, args)) {
                return false;
            }
            if ((!isCountAll && args.size() > 1) || (!(args.get(0).getExprNode() instanceof ExprNodeColumnDesc))) {
                return false;
            }
        }
        return true;
    }

    private boolean PTFReplaceable(Operator<? extends OperatorDesc> operator) {
        PTFDesc conf = (PTFDesc) operator.getConf();
        List<PTFExpressionDef> expressions = new ArrayList<>(conf.getFuncDef().getPartition().getExpressions());
        expressions.addAll(conf.getFuncDef().getOrder().getExpressions());
        if ((!PTFSupportedExpression(expressions)) || (!(conf.getFuncDef() instanceof WindowTableFunctionDef))) {
            return false;
        }
        List<WindowFunctionDef> windowFunctionDefs = ((WindowTableFunctionDef) conf.getFuncDef()).getWindowFunctions();
        for (WindowFunctionDef functionDef : windowFunctionDefs) {
            if (functionDef.getArgs() == null) {
                continue;
            }
            for (PTFExpressionDef expressionDef : functionDef.getArgs()) {
                if (expressionDef.getExprNode() != null && expressionDef.getExprNode().getTypeInfo().getTypeName() == "timestamp") {
                    return false;
                }
            }
        }
        if (!PTFSupportedAgg(windowFunctionDefs)) {
            return false;
        }
        return true;
    }

    private boolean mapJoinReplaceable(Operator<? extends OperatorDesc> operator) {
        MapJoinDesc mapJoinDesc = (MapJoinDesc) operator.getConf();
        if (mapJoinDesc.isBucketMapJoin()) {
            return false;
        }
        if (mapJoinDesc.getNullSafes() != null) {
            for (boolean nullsafe : mapJoinDesc.getNullSafes()) {
                if (nullsafe) {
                    return false;
                }
            }
        }
        Map<Byte, List<ExprNodeDesc>> keys = mapJoinDesc.getKeys();
        List<ExprNodeDesc> exprNodeDescList = new ArrayList<>();
        for (List<ExprNodeDesc> exprNodeDescs : keys.values()) {
            exprNodeDescList.addAll(exprNodeDescs);
        }
        for (List<ExprNodeDesc> value : mapJoinDesc.getFilters().values()) {
            exprNodeDescList.addAll(value);
        }
        if (!isUDFSupport(exprNodeDescList, false) || joinCondUnsupported(mapJoinDesc)) {
            return false;
        }
        return joinReplaceable(operator);
    }

    private boolean joinCondUnsupported(MapJoinDesc mapJoinDesc) {
        JoinCondDesc[] joinCondDescs = mapJoinDesc.getConds();
        if (joinCondDescs.length >= 2) {
            if (joinCondDescs[0].getType() == JoinDesc.LEFT_OUTER_JOIN && joinCondDescs[1].getType() == JoinDesc.LEFT_SEMI_JOIN) {
                return true;
            }
        }
        return false;
    }

    private boolean groupByReplaceable(Operator<? extends OperatorDesc> operator, boolean reduceSinkCanReplace) {
        GroupByDesc groupByDesc = (GroupByDesc) operator.getConf();
        if (groupByDesc.isGroupingSetsPresent() && groupByDesc.getMode() == GroupByDesc.Mode.PARTIALS) {
            return false;
        }
        if (!isUDFSupport(groupByDesc.getKeys(), false)) {
            return false;
        }
        if (isChildPTFOperatorUnReplaceable(operator, reduceSinkCanReplace)) {
            return false;
        }
        for (AggregationDesc aggregator : groupByDesc.getAggregators()) {
            if (aggregator.getDistinct()) {
                return false;
            }
            if (TypeUtils.getAggFunctionTypeFromName(aggregator) == null) {
                return false;
            }
            if (!isUDFSupport(aggregator.getParameters(), false)) {
                return false;
            }
            if (aggregator.getGenericUDAFName().equals("avg") && aggregator.getMode() == GenericUDAFEvaluator.Mode.PARTIAL1) {
                return false;
            }
            for (ExprNodeDesc parameter : aggregator.getParameters()) {
                if (aggParameterNotSupport(aggregator, parameter)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isChildPTFOperatorUnReplaceable(Operator<? extends OperatorDesc> operator, boolean reduceSinkCanReplace) {
        Queue<Operator<? extends OperatorDesc>> operators = new LinkedBlockingQueue<>(operator.getChildOperators());
        while (!operators.isEmpty()) {
            Operator<? extends OperatorDesc> op = operators.poll();
            if (op instanceof PTFOperator && !currentReplaceable(op, reduceSinkCanReplace)) {
                return true;
            }
            operators.addAll(op.getChildOperators());
        }
        return false;
    }

    private boolean aggParameterNotSupport(AggregationDesc aggregator, ExprNodeDesc parameter) {
        if (aggregator.getGenericUDAFName().equals("sum") || aggregator.getGenericUDAFName().equals("avg")) {
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) parameter.getTypeInfo()).getPrimitiveCategory();
            return primitiveCategory == STRING || primitiveCategory == CHAR || primitiveCategory == VARCHAR;
        }
        return false;
    }

    private boolean joinReplaceable(Operator<? extends OperatorDesc> operator) {
        JoinDesc joinDesc = (JoinDesc) operator.getConf();
        JoinCondDesc[] joinCondDescs = joinDesc.getConds();
        Map<Byte, List<ExprNodeDesc>> keys;
        if (joinDesc instanceof CommonMergeJoinDesc) {
            keys = ((CommonMergeJoinDesc) joinDesc).getKeys();
        } else  {
            keys = ((MapJoinDesc) joinDesc).getKeys();
        }
        List<ExprNodeDesc> exprNodeDescList = new ArrayList<>();
        for (List<ExprNodeDesc> exprNodeDescs : keys.values()) {
            exprNodeDescList.addAll(exprNodeDescs);
        }
        if (!isUDFSupport(exprNodeDescList, false)) {
            return false;
        }
        for (JoinCondDesc joinCondDesc : joinCondDescs) {
            if (!SUPPORTED_JOIN.contains(joinCondDesc.getType()) || joinCondDesc.getType() == JoinDesc.FULL_OUTER_JOIN && fullJoinUnReplaceable(joinDesc)) {
                return false;
            }
        }
        if (joinCondDescs.length >= 2) {
            for (List<ExprNodeDesc> filters : joinDesc.getFilters().values()) {
                if (!filters.isEmpty()) {
                    return false;
                }
            }
        }
        return checkResidulFilter(joinDesc);
    }

    private boolean fullJoinUnReplaceable(JoinDesc joinDesc) {
        if (joinDesc.getKeysString().get("0") == null || joinDesc.getResidualFilterExprs() != null) {
            return true;
        }
        if (joinDesc.getNullSafes() != null) {
            for (boolean nullsafe : joinDesc.getNullSafes()) {
                if (nullsafe) {
                    return true;
                }
            }
        }
        int filterNum = 0;
        for (List<ExprNodeDesc> filters : joinDesc.getFilters().values()) {
            if (!filters.isEmpty()) {
                ++filterNum;
            }
        }
        return filterNum >= 2;
    }

    private boolean checkResidulFilter(JoinDesc joinDesc) {
        if (joinDesc.getResidualFilterExprs() == null) {
            return true;
        }
        return isUDFSupport(joinDesc.getResidualFilterExprs(), false);
    }

    private boolean isPTFAggUnsupported(FunctionType windowFunctionType, List<PTFExpressionDef> expressions) {
        for (PTFExpressionDef expression : expressions) {
            ObjectInspector oi = expression.getOI();
            if (!(oi instanceof PrimitiveObjectInspector)) {
                continue;
            }
            PrimitiveTypeInfo primitiveTypeInfo = ((PrimitiveObjectInspector) oi).getTypeInfo();
            if (windowFunctionType == OMNI_AGGREGATION_TYPE_AVG && primitiveTypeInfo.getPrimitiveCategory().equals(DECIMAL)
                    && ((DecimalTypeInfo) primitiveTypeInfo).getPrecision() > DECIMAL64_MAX_PRECISION) {
                return true;
            }
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
            if ((windowFunctionType == OMNI_AGGREGATION_TYPE_AVG || windowFunctionType == OMNI_AGGREGATION_TYPE_SUM)
                    && (primitiveCategory == STRING || primitiveCategory == CHAR || primitiveCategory == VARCHAR)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSupportUDFInQueue(Queue<ExprNodeDesc> queue, List<String> expressions) {
        while (!queue.isEmpty()) {
            ExprNodeDesc current = queue.poll();
            if (current instanceof ExprNodeGenericFuncDesc) {
                BaseExpression expr = getBaseExpression(current);
                if (expr == null) return false;
                expressions.add(expr.toString());
                current.getChildren().forEach(queue::offer);
            } else if ((current instanceof ExprNodeColumnDesc || current instanceof ExprNodeConstantDesc)
                    && !SUPPORTED_TYPE.contains(((PrimitiveTypeInfo) current.getTypeInfo()).getPrimitiveCategory())) {
                return false;
            }
        }
        return true;
    }

    private BaseExpression getBaseExpression(ExprNodeDesc current) {
        if (((ExprNodeGenericFuncDesc) current).getGenericUDF() instanceof GenericUDFConcat ||
                ((ExprNodeGenericFuncDesc) current).getGenericUDF() instanceof GenericUDFCoalesce && current.getChildren().size() >= 3) {
            return null;
        }
        boolean supportUDF = ExpressionUtils.isSupportUDF(((ExprNodeGenericFuncDesc) current).getGenericUDF());
        if (!supportUDF || checkUnsupportedArithmetic((ExprNodeGenericFuncDesc) current)) {
            return null;
        }
        return getExpression(current);
    }

    private BaseExpression getExpression(ExprNodeDesc current) {
        BaseExpression expr = ExpressionUtils.build((ExprNodeGenericFuncDesc) current.clone(), null);
        if (expr instanceof CastFunctionExpression && checkUnsupportedCast((CastFunctionExpression) expr)) {
            return null;
        }
        return expr;
    }

    private boolean isUDFSupport(List<ExprNodeDesc> colList, boolean isFilterOperator) {
        Queue<ExprNodeDesc> queue = new LinkedBlockingQueue<>();
        colList.forEach(queue::offer);
        List<String> expressions = new ArrayList<>();
        try {
            if (!isSupportUDFInQueue(queue, expressions)) {
                return false;
            }
        } catch (Exception e) {
            LOG.error("generate omni expressions error", e);
            return false;
        }
        if (isFilterOperator) {
            for (String filterExpr : expressions) {
                if (!checkOmniJsonWhiteList(filterExpr, new String[0])) {
                    return false;
                }
            }
            return true;
        }
        return checkOmniJsonWhiteList("", expressions.toArray(new String[0]));
    }

    private boolean isLegalFilter(List<ExprNodeDesc> colList) {
        for (ExprNodeDesc desc : colList) {
            if (!isValidConversion(desc) || !isValidFilterExpression(desc)) {
                return false;
            }
        }
        if (colList.size() > 0 && colList.get(0).getChildren() != null) {
            List<ExprNodeDesc> childList = colList.get(0).getChildren();
            for (ExprNodeDesc desc : childList) {
                if (!isValidConversion(desc) || !isValidFilterExpression(desc)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isLegalTimestamp(List<ExprNodeDesc> colList) {
        for (ExprNodeDesc desc : colList) {
            if (checkUnsupportedTimestamp(desc)) {
                return false;
            }
        }
        if (colList.size() > 0 && colList.get(0).getChildren() != null) {
            List<ExprNodeDesc> childList = colList.get(0).getChildren();
            for (ExprNodeDesc desc : childList) {
                if (checkUnsupportedTimestamp(desc)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkMapJoinInput(Operator<? extends OperatorDesc> operator) {
        MapJoinDesc mapJoinDesc = (MapJoinDesc) operator.getConf();
        Map<Integer, String> parentToInput = mapJoinDesc.getParentToInput();
        for (String inputWorkName : parentToInput.values()) {
            if (reduceSinkReplaceableWorkName.contains(inputWorkName)) {
                return true;
            }
        }
        return false;
    }

    private void replaceInputFormat(BaseWork work) {
        if (work instanceof MapWork) {
            for (PartitionDesc partitionDesc : ((MapWork) work).getPathToPartitionInfo().values()) {
                if (partitionDesc.getInputFileFormatClassName().equals(OrcInputFormat.class.getName())) {
                    partitionDesc.setInputFileFormatClass(OmniOrcInputFormat.class);
                } else if (partitionDesc.getInputFileFormatClassName().equals(MapredParquetInputFormat.class.getName())) {
                    partitionDesc.setInputFileFormatClass(OmniParquetInputFormat.class);
                }
                partitionDesc.getProperties().setProperty(SERIALIZATION_LIB, VecBatchWrapperSerde.class.getName());
                partitionDesc.getTableDesc().getProperties().setProperty(SERIALIZATION_LIB, VecBatchWrapperSerde.class.getName());
            }
        }
    }

    private boolean getGroupNeedSort(BaseWork work) {
        for (BaseWork child : tezWork.getChildren(work)) {
            TezEdgeProperty.EdgeType edgeType = tezWork.getEdgeType(work, child);
            if (edgeType == TezEdgeProperty.EdgeType.ONE_TO_ONE_EDGE) {
                return true;
            }
        }
        return false;
    }

    private void addMergeJoinNeedSort(TezEdgeProperty.EdgeType edgeType, Operator reducer) {
        if (edgeType == TezEdgeProperty.EdgeType.SIMPLE_EDGE || edgeType == TezEdgeProperty.EdgeType.ONE_TO_ONE_EDGE) {
            mergeJoinNeedSort.add(reducer);
        }
    }

    private void replaceSimpleEdge(boolean reduceSinkCanReplace, BaseWork child, TezEdgeProperty.EdgeType edgeType) {
        if (edgeType == TezEdgeProperty.EdgeType.SIMPLE_EDGE) {
            if (reduceSinkCanReplace) {
                // replace all edge of merge join into CUSTOM_SIMPLE_EDGE
                setParentWorkToCurrentWorkEdge(child, TezEdgeProperty.EdgeType.CUSTOM_SIMPLE_EDGE);
            } else {
                // replace all edge of merge join into SIMPLE_EDGE
                setParentWorkToCurrentWorkEdge(child, TezEdgeProperty.EdgeType.SIMPLE_EDGE);
            }
        }
    }

    private boolean isSupportEdgeType(TezEdgeProperty.EdgeType edgeType) {
        return edgeType == TezEdgeProperty.EdgeType.ONE_TO_ONE_EDGE
                || edgeType == TezEdgeProperty.EdgeType.CUSTOM_SIMPLE_EDGE
                || edgeType == TezEdgeProperty.EdgeType.BROADCAST_EDGE
                || edgeType == TezEdgeProperty.EdgeType.XPROD_EDGE;
    }

    private void setParentWorkToCurrentWorkEdge(BaseWork work, TezEdgeProperty.EdgeType edgeType) {
        for (BaseWork parent : tezWork.getParents(work)) {
            if (tezWork.getEdgeType(parent, work) == TezEdgeProperty.EdgeType.BROADCAST_EDGE || parent instanceof UnionWork) {
                continue;
            }
            tezWork.getEdgeProperty(parent, work).setEdgeType(edgeType);
        }
    }

    private Operator<? extends OperatorDesc> replaceWithOmniOperator(OperatorInfo operatorInfo) {
        return replaceWithOmniOperator(operatorInfo, 0);
    }

    private Operator<? extends OperatorDesc> replaceWithOmniOperator(OperatorInfo operatorInfo, int originalParentsNum) {
        boolean reduceSinkCanReplace = operatorInfo.isReduceSinkCanReplace();
        Operator current = operatorInfo.getOperator();
        boolean vectorized = operatorInfo.isVectorized();
        VectorizedRowBatchCtx vectorizedRowBatchCtx = operatorInfo.getVectorizedRowBatchCtx();
        List<Operator<? extends OperatorDesc>> parents = current.getParentOperators();
        List<Operator<? extends OperatorDesc>> childs = current.getChildOperators();
        List<Operator<? extends OperatorDesc>> replacedChilds = new ArrayList<>();
        Operator<? extends OperatorDesc> omniOperator = createOmniOperator(current, vectorized, reduceSinkCanReplace);
        VectorizationContext outputVectorizationContext = null;
        if (vectorized) {
            outputVectorizationContext = getOutputVectorizationContext(current);
        }
        Operator<? extends OperatorDesc> vectorToRowOperator = vectorized ? new OmniVectorizedVectorOperator(current.getCompilationOpContext(), new OmniVectorDesc(false),
                outputVectorizationContext, vectorizedRowBatchCtx)
                : new OmniVectorOperator(current.getCompilationOpContext(), new OmniVectorDesc(false));
        if (childs.size() == 0 && !current.getType().equals(OperatorType.REDUCESINK)) {
            vectorToRowOperator.setParentOperators(Arrays.asList(omniOperator));
            replacedChilds.add(vectorToRowOperator);
        }
        for (Operator<? extends OperatorDesc> child : childs) {
            if (isReplaceable(child, reduceSinkCanReplace)) {
                replacedChilds.add(child);
                replaceOperatorList(child.getParentOperators(), current, omniOperator);
            } else {
                vectorToRowOperator.getChildOperators().add(child);
                replaceOperatorList(child.getParentOperators(), current, vectorToRowOperator);
            }
        }
        if (!vectorToRowOperator.getChildOperators().isEmpty()) {
            vectorToRowOperator.setParentOperators(Arrays.asList(omniOperator));
            replacedChilds.add(vectorToRowOperator);
        }
        omniOperator.setChildOperators(replacedChilds);
        if (groupByOperatorNeedSort.contains(current)) {
            addSortOperator(current, replacedChilds, omniOperator);
        }
        List<Operator<? extends OperatorDesc>> replacedParents = new ArrayList<>();
        VectorizationContext inputVectorizationContext = null;
        if (vectorized) {
            inputVectorizationContext = current instanceof VectorizationOperator ?
                    ((VectorizationOperator) current).getInputVectorizationContext() : null;
        }
        for (Operator<? extends OperatorDesc> parent : parents) {
            if (isReplaceable(parent, reduceSinkCanReplace)) {
                replacedParents.add(parent);
                replaceOperatorList(parent.getChildOperators(), current, omniOperator);
            } else {
                Operator<? extends OperatorDesc> rowToVectorOperator = vectorized ? new OmniVectorizedVectorOperator(current.getCompilationOpContext(), new OmniVectorDesc(true),
                        inputVectorizationContext, vectorizedRowBatchCtx)
                        : new OmniVectorOperator(parent.getCompilationOpContext(), new OmniVectorDesc(true));
                rowToVectorOperator.setChildOperators(Arrays.asList(omniOperator));
                rowToVectorOperator.setParentOperators(Arrays.asList(parent));
                replaceOperatorList(parent.getChildOperators(), current, rowToVectorOperator);
                replacedParents.add(rowToVectorOperator);
            }
        }
        if (parents.isEmpty() && originalParentsNum > 0) {
            Operator<? extends OperatorDesc> rowToVectorOperator = vectorized ? new OmniVectorizedVectorOperator(current.getCompilationOpContext(), new OmniVectorDesc(true),
                    inputVectorizationContext, vectorizedRowBatchCtx)
                    : new OmniVectorOperator(current.getCompilationOpContext(), new OmniVectorDesc(true));
            if (mergeJoinNeedSort.contains(current)) {
                rowToVectorOperator = new OmniVectorWithSortOperator(current.getCompilationOpContext(), new OmniVectorDesc(true));
            }
            rowToVectorOperator.setChildOperators(Arrays.asList(omniOperator));
            replacedParents.add(rowToVectorOperator);
            omniOperator.setParentOperators(replacedParents);
            return rowToVectorOperator;
        }
        omniOperator.setParentOperators(replacedParents);
        return omniOperator;
    }

    private void replaceOperatorList(List<Operator<? extends OperatorDesc>> operatorList,
                                     Operator<? extends OperatorDesc> current, Operator<? extends OperatorDesc> replaced) {
        for (int i = 0; i < operatorList.size(); i++) {
            if (operatorList.get(i) == current) {
                operatorList.set(i, replaced);
            }
        }
    }

    private void addSortOperator(Operator current, List<Operator<? extends OperatorDesc>> replacedChilds,
                                 Operator<? extends OperatorDesc> omniOperator) {
        OmniSortOperator omniSortOperator = new OmniSortOperator(current.getCompilationOpContext(),
                (GroupByDesc) current.getConf());
        for (Operator child : replacedChilds) {
            child.setParentOperators(Arrays.asList(omniSortOperator));
        }
        omniSortOperator.setChildOperators(replacedChilds);
        omniSortOperator.setParentOperators(Arrays.asList(omniOperator));
        omniOperator.setChildOperators(Arrays.asList(omniSortOperator));
    }


    private VectorizationContext getOutputVectorizationContext(Operator current) {
        if (current instanceof TableScanOperator) {
            return ((VectorizationOperator) current.getChildOperators().get(0)).getInputVectorizationContext();
        } else {
            return current instanceof VectorizationContextRegion ?
                    ((VectorizationContextRegion) current).getOutputVectorizationContext()
                    : ((VectorizationOperator) current).getInputVectorizationContext();
        }
    }

    private Operator<? extends OperatorDesc> createOmniOperator(Operator current, boolean vectorized, boolean reduceSinkCanReplace) {
        VectorizationContext vectorizationContext = null;
        if (vectorized && current instanceof VectorizationOperator) {
            vectorizationContext = ((VectorizationOperator) current).getInputVectorizationContext();
        }
        switch (current.getType()) {
            case SELECT:
                OmniSelectOperator omniSelectOperator = new OmniSelectOperator(current.getCompilationOpContext(), (SelectDesc) current.getConf());
                omniSelectOperator.setSchema(current.getSchema());
                return omniSelectOperator;
            case FILTER:
                return new OmniFilterOperator(current.getCompilationOpContext(), (FilterDesc) current.getConf());
            case MAPJOIN:
                if (current instanceof VectorMapJoinBaseOperator) {
                    return new OmniMapJoinOperator((MapJoinOperator) current, (MapJoinDesc) current.getConf(), true,
                            ((VectorMapJoinBaseOperator) current).getOutputVectorizationContext());
                }
                if (current instanceof VectorizationContextRegion) {
                    return new OmniMapJoinOperator((MapJoinOperator) current, (MapJoinDesc) current.getConf(), false,
                            ((VectorizationContextRegion) current).getOutputVectorizationContext());
                } else {
                    return new OmniMapJoinOperator((MapJoinOperator) current, (MapJoinDesc) current.getConf());
                }
            case MERGEJOIN:
                if (mergeJoinNeedSort.contains(current)) {
                    return new OmniMergeJoinWithSortOperator(current.getCompilationOpContext(), (CommonMergeJoinDesc) current.getConf());
                } else {
                    return new OmniMergeJoinOperator(current.getCompilationOpContext(), (CommonMergeJoinDesc) current.getConf());
                }
            case GROUPBY:
                return new OmniGroupByOperator(current.getCompilationOpContext(), (GroupByDesc) current.getConf(), vectorizationContext,
                        vectorized ? ((VectorGroupByOperator) current).getOutputVectorizationContext() : null);
            case PTF:
                return new OmniPTFOperator(current.getCompilationOpContext(), (PTFDesc) current.getConf());
            case REDUCESINK:
                OmniReduceSinkOperator omniReduceSinkOperator = new OmniReduceSinkOperator(current.getCompilationOpContext(), (ReduceSinkDesc) current.getConf(), reduceSinkCanReplace);
                omniReduceSinkOperator.getConf().getKeySerializeInfo().getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                omniReduceSinkOperator.getConf().getValueSerializeInfo().getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                return omniReduceSinkOperator;
            case TABLESCAN:
                if (vectorized) {
                    return new OmniVectorizedTableScanOperator((TableScanOperator) current);
                } else {
                    return new OmniTableScanOperator((TableScanOperator) current);
                }
        }
        return null;
    }

    private void replaceSerializationLib() {
        for (BaseWork work : tezWork.getAllWork()) {
            if (!(work instanceof ReduceWork || work instanceof MergeJoinWork)) {
                continue;
            }
            if (work instanceof MergeJoinWork) {
                work = ((MergeJoinWork) work).getMainWork();
            }
            if (work instanceof MapWork) {
                continue;
            }
            List<Integer> toReplace = new ArrayList<>();
            Map<Integer, String> tagToInput = ((ReduceWork) work).getTagToInput();
            for (Map.Entry<Integer, String> entry : tagToInput.entrySet()) {
                if (reduceSinkReplaceableWorkName.contains(entry.getValue())) {
                    toReplace.add(entry.getKey());
                }
            }
            List<TableDesc> tagToValueDesc = ((ReduceWork) work).getTagToValueDesc();
            for (Integer tag : toReplace) {
                TableDesc keyDesc = ((ReduceWork) work).getKeyDesc();
                keyDesc.getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                tagToValueDesc.get(tag).getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
            }
        }
    }

    private void setReduceSinkChildWorkUnVectorized() {
        for (BaseWork work : reduceSinkReplaceableWork) {
            for (BaseWork childWork : tezWork.getChildren(work)) {
                if (tezWork.getEdgeType(work, childWork) == TezEdgeProperty.EdgeType.BROADCAST_EDGE
                        || tezWork.getEdgeType(work, childWork) == TezEdgeProperty.EdgeType.CONTAINS) {
                    continue;
                }
                childWork.setVectorMode(false);
            }
        }
    }

    private void replaceSerializationLibForMapJoinOperator() {
        for (BaseWork work : tezWork.getAllWork()) {
            for (Operator<?> operator : work.getAllOperators()) {
                if (operator instanceof OmniMapJoinOperator) {
                    MapJoinDesc conf = (MapJoinDesc) operator.getConf();
                    Map<Integer, String> parentToInput = conf.getParentToInput();
                    for (Map.Entry<Integer, String> entry : parentToInput.entrySet()) {
                        if (reduceSinkReplaceableWorkName.contains(entry.getValue())) {
                            TableDesc keyDesc = conf.getKeyTblDesc();
                            keyDesc.getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                            if (conf.getNoOuterJoin()) {
                                List<TableDesc> valueDesc = conf.getValueTblDescs();
                                valueDesc.get(entry.getKey()).getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                            } else {
                                List<TableDesc> valueDesc = conf.getValueFilteredTblDescs();
                                valueDesc.get(entry.getKey()).getProperties().setProperty(SERIALIZATION_LIB, OmniVecBatchSerDe.class.getName());
                            }
                        }
                    }
                }
            }
        }
    }


    private class OperatorInfo {
        private Operator operator;

        private boolean vectorized;
        private VectorizedRowBatchCtx vectorizedRowBatchCtx;
        private boolean reduceSinkCanReplace;

        public OperatorInfo(Operator operator, boolean vectorized, VectorizedRowBatchCtx vectorizedRowBatchCtx, boolean reduceSinkCanReplace) {
            this.operator = operator;
            this.vectorized = vectorized;
            this.vectorizedRowBatchCtx = vectorizedRowBatchCtx;
            this.reduceSinkCanReplace = reduceSinkCanReplace;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof OperatorInfo) {
                return (this.operator == ((OperatorInfo) obj).getOperator());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return operator.hashCode();
        }

        public Operator getOperator() {
            return operator;
        }

        public boolean isVectorized() {
            return vectorized;
        }

        public VectorizedRowBatchCtx getVectorizedRowBatchCtx() {
            return vectorizedRowBatchCtx;
        }

        public boolean isReduceSinkCanReplace() {
            return reduceSinkCanReplace;
        }
    }

}
