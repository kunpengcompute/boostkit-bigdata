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

package org.apache.hadoop.hive.ql.omnidata.physical;

import com.huawei.boostkit.omnidata.model.AggregationInfo;
import com.huawei.boostkit.omnidata.model.Predicate;

import com.google.common.collect.ImmutableMap;

import io.prestosql.spi.relation.RowExpression;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.operator.aggregation.OmniDataAggregation;
import org.apache.hadoop.hive.ql.omnidata.operator.filter.NdpFilter;
import org.apache.hadoop.hive.ql.omnidata.operator.filter.OmniDataFilter;
import org.apache.hadoop.hive.ql.omnidata.operator.limit.OmniDataLimit;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.omnidata.serialize.NdpSerializationUtils;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusInfo;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusManager;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Ndp Physical plan optimization
 */
public class NdpPlanResolver implements PhysicalPlanResolver {
    private static final Logger LOG = LoggerFactory.getLogger(NdpPlanResolver.class);

    private HiveConf hiveConf;

    private Context context;

    private ParseContext parseContext;

    private boolean isPushDownFilter = false;

    private boolean isPushDownPartFilter = false;

    private boolean isPushDownSelect = false;

    private boolean isPushDownAgg = false;

    private boolean isPushDownLimit = false;

    /**
     * Hive filter expression
     */
    private ExprNodeGenericFuncDesc filterDesc;

    /**
     * Hive does not support push-down filter expressions.
     */
    private ExprNodeDesc unsupportedFilterDesc;

    /**
     * Hive select expression
     */
    private VectorSelectDesc selectDesc;

    /**
     * Hive agg && group by expression
     */
    private GroupByDesc aggDesc;

    /**
     * Hive limit expression
     */
    private LimitDesc limitDesc;

    /**
     * Table data format
     */
    private String dataFormat = "";

    /**
     * If a table in an SQL statement is pushed down, this parameter is set to true.
     */
    private boolean existsTablePushDown = false;

    private NdpVectorizedRowBatchCtx ndpCtx;

    @Override
    public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
        this.hiveConf = pctx.getConf();
        this.context = pctx.getContext();
        this.parseContext = pctx.getParseContext();
        Dispatcher dispatcher = new NdpDispatcher();
        TaskGraphWalker walker = new TaskGraphWalker(dispatcher);
        List<Node> topNodes = new ArrayList<>(pctx.getRootTasks());
        walker.startWalking(topNodes, null);
        return pctx;
    }

    private class NdpDispatcher implements Dispatcher {
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            if (nodeOutputs == null || nodeOutputs.length == 0) {
                throw new SemanticException("No Dispatch Context");
            }
            // OmniData Hive unsupported operator 'ROLLUP'
            if (!NdpPlanChecker.checkRollUp(context.getCmd())) {
                return null;
            }

            // host resources status map
            Map<String, NdpStatusInfo> ndpStatusInfoMap = new HashMap<>(NdpStatusManager.getNdpZookeeperData(hiveConf));
            if (!NdpPlanChecker.checkHostResources(ndpStatusInfoMap)) {
                return null;
            }

            Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;

            if (!currTask.isMapRedLocalTask() && !currTask.isMapRedTask()) {
                return null;
            }

            // key: work name
            Map<String, MapWork> mapWorkMap = new HashMap<>();
            Map<String, TableScanOperator> tableScanOperatorMap = new HashMap<>();
            // use TreeMap, sort by reduce name, for example: Reduce1, Reduce2...
            Map<String, ReduceWork> reduceWorkMap = new TreeMap<>();
            if (currTask.getWork() instanceof TezWork) {
                NdpStatisticsUtils.generateMapReduceWork((TezWork) currTask.getWork(), mapWorkMap, tableScanOperatorMap,
                        reduceWorkMap);
            } else {
                // unsupported
                return null;
            }
            // deal with each TableScanOperator
            tableScanOperatorMap.forEach((workName, tableScanOp) -> {
                NdpPredicateInfo ndpPredicateInfo = new NdpPredicateInfo(false);
                // set table's selectivity
                tableScanOp.getConf().setOmniDataSelectivity(NdpStatisticsUtils.getSelectivity(tableScanOp));
                // check TableScanOp
                if (checkTableScanOp(tableScanOp, mapWorkMap.get(workName))) {
                    Optional<RowExpression> filter = Optional.empty();
                    Optional<AggregationInfo> aggregation = Optional.empty();
                    OptionalLong limit = OptionalLong.empty();
                    OmniDataPredicate omniDataPredicate = new OmniDataPredicate(tableScanOp);
                    omniDataPredicate.setSelectExpressions(selectDesc);
                    // get OmniData filter expression
                    if (isPushDownFilter) {
                        filter = getOmniDataFilter(omniDataPredicate, tableScanOp);
                        if (!filter.isPresent()) {
                            isPushDownFilter = false;
                            isPushDownAgg = false;
                        }
                    }
                    // get OmniData agg expression
                    if (isPushDownAgg) {
                        // The output column of the aggregation needs to be processed separately.
                        aggregation = getOmniDataAggregation(omniDataPredicate);
                        if (!aggregation.isPresent()) {
                            isPushDownAgg = false;
                        }
                    }
                    // get OmniData select expression
                    if (isPushDownSelect && !isPushDownAgg) {
                        omniDataPredicate = new OmniDataPredicate(tableScanOp);
                        omniDataPredicate.addProjectionsByTableScan(tableScanOp);
                    }
                    // get OmniData limit expression
                    if (isPushDownLimit) {
                        limit = getOmniDataLimit();
                    }
                    // the decode type must exist
                    if ((isPushDownFilter || isPushDownAgg) && omniDataPredicate.getDecodeTypes().size() > 0) {
                        replaceTableScanOp(tableScanOp, mapWorkMap.get(workName));
                        Predicate predicate = new Predicate(omniDataPredicate.getTypes(),
                                omniDataPredicate.getColumns(), filter, omniDataPredicate.getProjections(),
                                ImmutableMap.of(), ImmutableMap.of(), aggregation, limit);
                        ndpPredicateInfo = new NdpPredicateInfo(true, isPushDownAgg, isPushDownFilter,
                                omniDataPredicate.getHasPartitionColumns(), predicate,
                                tableScanOp.getConf().getNeededColumnIDs(), omniDataPredicate.getDecodeTypes(),
                                omniDataPredicate.getDecodeTypesWithAgg(), ndpCtx, dataFormat);
                        // print OmniData Hive log information
                        printPushDownInfo(tableScanOp.getConf().getAlias(), ndpStatusInfoMap);
                        existsTablePushDown = true;
                    }
                }
                // set ndpPredicateInfo after serialization
                tableScanOp.getConf()
                        .setNdpPredicateInfoStr(NdpSerializationUtils.serializeNdpPredicateInfo(ndpPredicateInfo));
                initPushDown();
            });
            if (existsTablePushDown) {
                // set OmniData hosts info to Hive conf
                NdpStatusManager.setOmniDataHostToConf(hiveConf, ndpStatusInfoMap);
                // OmniData reduce work optimize
                if (OmniDataConf.getOmniDataReduceOptimizedEnabled(hiveConf)) {
                    NdpStatisticsUtils.optimizeReduceWork(tableScanOperatorMap, reduceWorkMap, hiveConf, parseContext);
                }
            }
            OmniDataConf.setOmniDataExistsTablePushDown(hiveConf, existsTablePushDown);
            return null;
        }

        private boolean checkTableScanOp(TableScanOperator tableScanOp, BaseWork work) {
            Optional<String> format = NdpPlanChecker.getDataFormat(tableScanOp, work);
            if (format.isPresent()) {
                dataFormat = format.get();
            } else {
                LOG.info("Table [{}] failed to push down, only orc and parquet are supported",
                        tableScanOp.getConf().getAlias());
                return false;
            }
            if (NdpPlanChecker.checkTableScanNumChild(tableScanOp) && NdpPlanChecker.checkHiveType(tableScanOp)
                    && NdpPlanChecker.checkTableSize(tableScanOp, hiveConf) && NdpPlanChecker.checkSelectivity(tableScanOp,
                    hiveConf)) {
                // scan operator: select agg filter limit
                scanTableScanChildOperators(tableScanOp.getChildOperators());
            }
            return isPushDownAgg || isPushDownFilter;
        }

        /**
         * support : FilterOperator -> VectorSelectOperator -> VectorGroupByOperator
         * support : FilterOperator -> VectorSelectOperator -> VectorLimitOperator
         *
         * @param operators operator
         */
        private void scanTableScanChildOperators(List<Operator<? extends OperatorDesc>> operators) {
            if (operators == null || operators.size() != 1) {
                return;
            }
            Operator<? extends OperatorDesc> operator = operators.get(0);
            if (operator instanceof VectorFilterOperator) {
                // filter push down
                filterDesc = NdpPlanChecker.checkFilterOperator((VectorFilterOperator) operator);
                isPushDownFilter = (filterDesc != null);
                scanTableScanChildOperators(operator.getChildOperators());
            } else if (operator instanceof VectorSelectOperator) {
                // check select
                selectDesc = NdpPlanChecker.checkSelectOperator((VectorSelectOperator) operator);
                isPushDownSelect = (selectDesc != null);
                scanTableScanChildOperators(operator.getChildOperators());
            } else if (operator instanceof VectorGroupByOperator) {
                // check agg
                aggDesc = NdpPlanChecker.checkGroupByOperator((VectorGroupByOperator) operator);
                isPushDownAgg = (aggDesc != null);
                scanTableScanChildOperators(operator.getChildOperators());
            } else if (operator instanceof VectorLimitOperator) {
                // check limit
                limitDesc = NdpPlanChecker.checkLimitOperator((VectorLimitOperator) operator);
                isPushDownLimit = (limitDesc != null);
                scanTableScanChildOperators(operator.getChildOperators());
            }
        }

        /**
         * Converts the filter expression of Hive to that of the OmniData Server.
         *
         * @param omniDataPredicate OmniData Predicate
         * @return Filter expression of OmniData Filter
         */
        private Optional<RowExpression> getOmniDataFilter(OmniDataPredicate omniDataPredicate,
                                                          TableScanOperator tableScanOperator) {
            NdpFilter ndpFilter = new NdpFilter(filterDesc);
            NdpFilter.NdpFilterMode mode = ndpFilter.getMode();
            if (mode.equals(NdpFilter.NdpFilterMode.PART)) {
                if (!NdpStatisticsUtils.evaluatePartFilterSelectivity(tableScanOperator,
                        ndpFilter.getUnPushDownFuncDesc(), parseContext, hiveConf)) {
                    return Optional.empty();
                }
                isPushDownPartFilter = true;
                unsupportedFilterDesc = ndpFilter.getUnPushDownFuncDesc();
                filterDesc = (ExprNodeGenericFuncDesc) ndpFilter.getPushDownFuncDesc();
                // The AGG does not support part push down
                isPushDownAgg = false;
            } else if (mode.equals(NdpFilter.NdpFilterMode.NONE)) {
                return Optional.empty();
            }
            OmniDataFilter omniDataFilter = new OmniDataFilter(omniDataPredicate);
            // ExprNodeGenericFuncDesc need to clone
            return Optional.ofNullable(
                    omniDataFilter.getFilterExpression((ExprNodeGenericFuncDesc) filterDesc.clone(), ndpFilter));
        }

        /**
         * Converts the aggregation expression of Hive to that of the OmniData Server.
         *
         * @param omniDataPredicate OmniData Predicate
         * @return Aggregation expression of OmniData Server
         */
        private Optional<AggregationInfo> getOmniDataAggregation(OmniDataPredicate omniDataPredicate) {
            OmniDataAggregation omniDataAggregation = new OmniDataAggregation(omniDataPredicate);
            return Optional.ofNullable(omniDataAggregation.getAggregation(aggDesc));
        }

        private OptionalLong getOmniDataLimit() {
            return OmniDataLimit.getOmniDataLimit(limitDesc.getLimit());
        }

        private void replaceTableScanOp(TableScanOperator tableScanOp, BaseWork work){
            if (isPushDownFilter) {
                if (isPushDownPartFilter) {
                    replaceRawVectorizedRowBatchCtx(tableScanOp, work);
                } else {
                    removeTableScanRawFilter(tableScanOp.getChildOperators());
                }
                ndpCtx = new NdpVectorizedRowBatchCtx(work.getVectorizedRowBatchCtx());
                // set rowColumnTypeInfos to TableScanDesc
                tableScanOp.getConf().setRowColumnTypeInfos(work.getVectorizedRowBatchCtx().getRowColumnTypeInfos());
            }
            if (isPushDownAgg) {
                removeTableScanRawAggregation(tableScanOp.getChildOperators(), work);
                removeTableScanRawSelect(tableScanOp.getChildOperators());
            }
            // set whether to push down
            tableScanOp.getConf().setPushDownFilter(isPushDownFilter);
            tableScanOp.getConf().setPushDownAgg(isPushDownAgg);
        }

        /**
         * The outputColumnId is added to VectorizedRowBatchCtx because a new filter expression is replaced.
         *
         * @param tableScanOp TableScanOperator
         * @param work BaseWork
         */
        private void replaceRawVectorizedRowBatchCtx(TableScanOperator tableScanOp, BaseWork work) {
            VectorizedRowBatchCtx oldCtx = work.getVectorizedRowBatchCtx();
            VectorizedRowBatchCtx ndpCtx = new VectorizedRowBatchCtx(oldCtx.getRowColumnNames(),
                    oldCtx.getRowColumnTypeInfos(), oldCtx.getRowdataTypePhysicalVariations(), oldCtx.getDataColumnNums(),
                    oldCtx.getPartitionColumnCount(), oldCtx.getVirtualColumnCount(), oldCtx.getNeededVirtualColumns(),
                    tableScanOp.getOutputVectorizationContext().getScratchColumnTypeNames(),
                    tableScanOp.getOutputVectorizationContext().getScratchDataTypePhysicalVariations());
            work.setVectorizedRowBatchCtx(ndpCtx);
        }

        private void removeTableScanRawFilter(List<Operator<? extends OperatorDesc>> operators) {
            try {
                for (Operator<? extends OperatorDesc> child : operators) {
                    if (child instanceof VectorFilterOperator) {
                        // remove raw VectorFilterOperator
                        child.getParentOperators().get(0).removeChildAndAdoptItsChildren(child);
                        return;
                    }
                    removeTableScanRawFilter(child.getChildOperators());
                }
            } catch (SemanticException e) {
                LOG.error("Exception when trying to remove partition predicates: fail to find child from parent", e);
            }
        }

        private void removeTableScanRawAggregation(List<Operator<? extends OperatorDesc>> operators, BaseWork work) {
            try {
                for (Operator<? extends OperatorDesc> child : operators) {
                    if (child instanceof VectorGroupByOperator) {
                        // remove raw VectorGroupByOperator
                        child.getParentOperators().get(0).removeChildAndAdoptItsChildren(child);
                        return;
                    }
                    removeTableScanRawAggregation(child.getChildOperators(), work);
                }
            } catch (SemanticException e) {
                LOG.error("Exception when trying to remove partition predicates: fail to find child from parent", e);
            }
        }

        private void removeTableScanRawSelect(List<Operator<? extends OperatorDesc>> operators) {
            for (Operator<? extends OperatorDesc> child : operators) {
                if (child instanceof VectorSelectOperator) {
                    // remove raw VectorExpression
                    ((VectorSelectOperator) child).setvExpressions(new VectorExpression[] {});
                    return;
                }
                removeTableScanRawSelect(child.getChildOperators());
            }
        }

        private void printPushDownInfo(String tableName, Map<String, NdpStatusInfo> ndpStatusInfoMap) {
            String pushDownInfo = String.format(
                    "Table [%s] Push Down Info [ Select:[%s], Filter:[%s], Aggregation:[%s], Group By:[%s], Limit:[%s], Raw Filter:[%s], Host Map:[%s]",
                    tableName, ((selectDesc != null) && isPushDownAgg) ? Arrays.stream(selectDesc.getSelectExpressions())
                            .map(VectorExpression::toString)
                            .collect(Collectors.joining(", ")) : "", isPushDownFilter ? filterDesc.toString() : "",
                    isPushDownAgg ? (aggDesc.getAggregators()
                            .stream()
                            .map(AggregationDesc::getExprString)
                            .collect(Collectors.joining(", "))) : "",
                    (isPushDownAgg && aggDesc.getKeyString() != null) ? aggDesc.getKeyString() : "",
                    isPushDownLimit ? limitDesc.getLimit() : "",
                    (unsupportedFilterDesc != null) ? unsupportedFilterDesc.toString() : "",
                    ((ndpStatusInfoMap.size() > 0) ? ndpStatusInfoMap.entrySet()
                            .stream()
                            .map(s -> s.getValue().getDatanodeHost() + " -> " + s.getKey())
                            .limit(100)
                            .collect(Collectors.joining(", ")) : ""));
            LOG.info(pushDownInfo);
            System.out.println(pushDownInfo);
        }

        private void initPushDown() {
            isPushDownFilter = false;
            isPushDownPartFilter = false;
            isPushDownSelect = false;
            isPushDownAgg = false;
            isPushDownLimit = false;
            filterDesc = null;
            unsupportedFilterDesc = null;
            selectDesc = null;
            aggDesc = null;
            limitDesc = null;
            ndpCtx = null;
            dataFormat = "";
        }
    }
}