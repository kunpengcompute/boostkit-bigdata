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

package org.apache.hadoop.hive.ql.omnidata.operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.physical.NdpStatisticsUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * OmniData Hive operator tool
 *
 * @since 2022/12/12
 */
public class NdpOperatorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NdpStatisticsUtils.class);

    /**
     * The outputColumnId is added to VectorizedRowBatchCtx because a new filter expression is replaced.
     *
     * @param tableScanOp TableScanOperator
     * @param work        BaseWork
     */
    public static void replaceRawVectorizedRowBatchCtx(TableScanOperator tableScanOp, BaseWork work) {
        VectorizedRowBatchCtx oldCtx = work.getVectorizedRowBatchCtx();
        VectorizedRowBatchCtx newCtx = new VectorizedRowBatchCtx(oldCtx.getRowColumnNames(),
                oldCtx.getRowColumnTypeInfos(), oldCtx.getRowdataTypePhysicalVariations(), oldCtx.getDataColumnNums(),
                oldCtx.getPartitionColumnCount(), oldCtx.getVirtualColumnCount(), oldCtx.getNeededVirtualColumns(),
                tableScanOp.getOutputVectorizationContext().getScratchColumnTypeNames(),
                tableScanOp.getOutputVectorizationContext().getScratchDataTypePhysicalVariations());
        work.setVectorizedRowBatchCtx(newCtx);
    }

    /**
     * Traverse TableScanOperator and replace the native filter and aggregation of Hive.
     *
     * @param tableScanOp      TableScanOperator
     * @param ndpPredicateInfo OmniData predicate info
     * @param mapWork          Hive MapWork
     * @throws HiveException SemanticException
     */
    public static void replaceFilterAndAgg(TableScanOperator tableScanOp, NdpPredicateInfo ndpPredicateInfo,
                                           MapWork mapWork) throws HiveException {
        if (ndpPredicateInfo.getIsPushDownFilter()) {
            if (ndpPredicateInfo.getIsPushDownPartFilter()) {
                replaceRawVectorizedRowBatchCtx(tableScanOp, mapWork);
            } else {
                removeTableScanRawFilter(tableScanOp.getChildOperators());
            }
        }
        if (ndpPredicateInfo.getIsPushDownAgg()) {
            removeTableScanRawAggregation(tableScanOp.getChildOperators());
            removeTableScanRawSelect(tableScanOp.getChildOperators());
        }
    }

    /**
     * Remove the native filter operator of Hive.
     *
     * @param operators Hive's operators
     * @throws HiveException SemanticException
     */
    public static void removeTableScanRawFilter(List<Operator<? extends OperatorDesc>> operators) throws HiveException {
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
            throw new HiveException("Exception when trying to remove partition predicates: fail to find child from "
                    + "parent", e);
        }
    }

    /**
     * Remove the native aggregation operator of Hive.
     *
     * @param operators operators Hive's operators
     * @throws HiveException SemanticException
     */
    public static void removeTableScanRawAggregation(List<Operator<? extends OperatorDesc>> operators)
            throws HiveException {
        try {
            for (Operator<? extends OperatorDesc> child : operators) {
                if (child instanceof VectorGroupByOperator) {
                    // remove raw VectorGroupByOperator
                    child.getParentOperators().get(0).removeChildAndAdoptItsChildren(child);
                    return;
                }
                removeTableScanRawAggregation(child.getChildOperators());
            }
        } catch (SemanticException e) {
            throw new HiveException("Exception when trying to remove partition predicates: fail to find child from "
                    + "parent", e);
        }
    }

    /**
     * Remove the native select operator of Hive.
     *
     * @param operators Hive's operators
     */
    public static void removeTableScanRawSelect(List<Operator<? extends OperatorDesc>> operators) {
        for (Operator<? extends OperatorDesc> child : operators) {
            if (child instanceof VectorSelectOperator) {
                // remove raw VectorExpression
                ((VectorSelectOperator) child).setvExpressions(new VectorExpression[]{});
                return;
            }
            removeTableScanRawSelect(child.getChildOperators());
        }
    }

    /**
     * Through MapWork, get MapWork and TableScanOperator
     *
     * @param mapWork              Hive MapWork
     * @param mapWorkMap           stores the map between work names and MapWork.
     * @param tableScanOperatorMap stores the map between work names and TableScanOperator.
     */
    public static void generateMapWork(MapWork mapWork, Map<String, MapWork> mapWorkMap,
                                       Map<String, TableScanOperator> tableScanOperatorMap) {
        mapWork.getAliasToWork().values().forEach(operator -> {
            if (operator instanceof TableScanOperator) {
                tableScanOperatorMap.put(mapWork.getName(), (TableScanOperator) operator);
                mapWorkMap.put(mapWork.getName(), mapWork);
            }
        });
    }

    /**
     * Through TezWork, get MapWork, ReduceWork and TableScanOperator
     *
     * @param tezWork              TezWork
     * @param mapWorkMap           MapWork's map
     * @param tableScanOperatorMap TableScanOperator's map
     * @param reduceWorkMap        ReduceWork's map
     */
    public static void generateMapReduceWork(TezWork tezWork, Map<String, MapWork> mapWorkMap,
                                             Map<String, TableScanOperator> tableScanOperatorMap, Map<String,
            ReduceWork> reduceWorkMap) {
        for (BaseWork work : tezWork.getAllWork()) {
            if (work instanceof MapWork) {
                MapWork mapWork = (MapWork) work;
                if (mapWork.getAliasToWork().size() != 1) {
                    continue;
                }
                // support only one operator
                generateMapWork(mapWork, mapWorkMap, tableScanOperatorMap);
            }
            if (work instanceof ReduceWork) {
                reduceWorkMap.put(work.getName(), (ReduceWork) work);
            }
        }
    }

    /**
     * Set OmniData info to configuration
     *
     * @param conf Hive configuration
     */
    public static void setOmniDataInfoByConf(Configuration conf) {
        MapWork work = Utilities.getMapWork(conf);
        if (work != null && work.getAliasToWork().size() == 1) {
            work.getAliasToWork().values().forEach(t -> {
                if (t instanceof TableScanOperator) {
                    OmniDataConf.setOmniDataAggOptimizedEnabled(conf,
                            ((TableScanOperator) t).getConf().isPushDownAgg());
                    OmniDataConf.setOmniDataFilterOptimizedEnabled(conf,
                            ((TableScanOperator) t).getConf().isPushDownFilter());
                    OmniDataConf.setOmniDataTableOptimizedSelectivity(conf,
                            ((TableScanOperator) t).getConf().getOmniDataSelectivity());
                }
            });
        }
    }
}