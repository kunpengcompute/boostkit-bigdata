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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.LevelOrderWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateStatsProcCtx;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.tez.mapreduce.grouper.TezSplitGrouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * OmniData Hive statistics tool
 * Used to collect statistics for optimization
 *
 * @since 2022-07-12
 */
public class NdpStatisticsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NdpStatisticsUtils.class);

    /**
     * basic reduce quantity and protection measures
     */
    public static final int BASED_REDUCES = 50;

    /**
     * basic selectivity quantity and protection measures
     */
    public static final double BASED_SELECTIVITY = 0.025d;

    /**
     * Ratio of the length of the needed column to the total length, the value cannot be less than the basic value
     */
    public static final double BASED_COL_LENGTH_PROPORTION = 0.025d;

    /**
     * Optimization can be performed only when the selectivity is lower than this threshold
     */
    public static final double SELECTIVITY_THRESHOLD = 0.5d;

    /**
     * Number of decimal places reserved
     */
    public static final int OMNIDATA_SCALE = 4;

    /**
     * Calculate the estimated selectivity based on the total number of Table and the total number of Filter
     *
     * @param tableScanOp TableScanOperator
     * @return selectivity
     */
    public static double getSelectivity(TableScanOperator tableScanOp) {
        double selectivity = 1.0d;
        if (tableScanOp.getConf().getStatistics() == null) {
            return selectivity;
        }
        long tableCount = tableScanOp.getConf().getStatistics().getNumRows();
        long filterCount = tableScanOp.getChildOperators().get(0).getConf().getStatistics().getNumRows();
        if (tableCount > 0) {
            BigDecimal tableCountBig = new BigDecimal(tableCount);
            BigDecimal filterCountBig = new BigDecimal(filterCount);
            selectivity = filterCountBig.divide(tableCountBig, OMNIDATA_SCALE, BigDecimal.ROUND_HALF_UP).doubleValue();
        }
        return selectivity;
    }

    /**
     * Through TezWork, get MapWork, ReduceWork and TableScanOperator
     *
     * @param tezWork TezWork
     * @param mapWorkMap MapWork's map
     * @param tableScanOperatorMap TableScanOperator's map
     * @param reduceWorkMap ReduceWork's map
     */
    public static void generateMapReduceWork(TezWork tezWork, Map<String, MapWork> mapWorkMap,
                                             Map<String, TableScanOperator> tableScanOperatorMap, Map<String, ReduceWork> reduceWorkMap) {
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
     * Analyze the table in advance to collect statistics.
     * Estimate the optimized size based on the selectivity and needed column proportion.
     *
     * @param tableScanOp tableScanOp
     * @param parseContext The current parse context.
     * @return optimized size
     */
    public static long estimateOptimizedSize(TableScanOperator tableScanOp, ParseContext parseContext) {
        long dataSize = tableScanOp.getConf().getStatistics().getDataSize();
        long avgRowSize = tableScanOp.getConf().getStatistics().getAvgRowSize();
        double omniDataSelectivity = Math.max(tableScanOp.getConf().getOmniDataSelectivity(), BASED_SELECTIVITY);
        // get the average length of the needed column
        double totalAvgColLen = getTotalAvgColLen(tableScanOp, parseContext);
        if (totalAvgColLen <= 0 || avgRowSize <= 0) {
            return (long) ((double) dataSize * omniDataSelectivity);
        } else {
            double colLenProportion = Math.max(totalAvgColLen / (double) avgRowSize, BASED_COL_LENGTH_PROPORTION);
            return (long) ((double) dataSize * omniDataSelectivity * colLenProportion);
        }
    }

    /**
     * get the average length of the needed column
     *
     * @param tableScanOp tableScanOp
     * @param parseContext parseContext
     * @return table's average length
     */
    public static double getTotalAvgColLen(TableScanOperator tableScanOp, ParseContext parseContext) {
        Table table = tableScanOp.getConf().getTableMetadata();
        List<ColStatistics> colStats;
        if (table.isPartitioned()) {
            Optional<Statistics> statistics = collectStatistics(parseContext, tableScanOp);
            if (statistics.isPresent()) {
                colStats = statistics.get().getColumnStats();
            } else {
                colStats = new ArrayList<>();
            }
        } else {
            // Get table level column statistics from metastore for needed columns
            colStats = StatsUtils.getTableColumnStats(tableScanOp.getConf().getTableMetadata(),
                    tableScanOp.getSchema().getSignature(), tableScanOp.getConf().getNeededColumns(), null);
        }
        double totalAvgColLen = 0d;
        // add the avgColLen for each column, colStats's size may be null
        for (ColStatistics colStat : colStats) {
            totalAvgColLen += colStat.getAvgColLen();
        }
        return totalAvgColLen;
    }

    /**
     * Statistics: Describes the output of an operator in terms of size, rows, etc  based on estimates.
     * use ParseContext to collect Table's statistics
     *
     * @param pctx The current parse context.
     * @param tableScanOp TableScanOperator
     * @return Statistics
     */
    public static Optional<Statistics> collectStatistics(ParseContext pctx, TableScanOperator tableScanOp) {
        try {
            AnnotateStatsProcCtx aspCtx = new AnnotateStatsProcCtx(pctx);
            PrunedPartitionList partList = aspCtx.getParseContext().getPrunedPartitions(tableScanOp);
            ColumnStatsList colStatsCached = aspCtx.getParseContext().getColStatsCached(partList);
            Table table = tableScanOp.getConf().getTableMetadata();
            // column level statistics are required only for the columns that are needed
            List<ColumnInfo> schema = tableScanOp.getSchema().getSignature();
            List<String> neededColumns = tableScanOp.getNeededColumns();
            List<String> referencedColumns = tableScanOp.getReferencedColumns();
            // gather statistics for the first time and the attach it to table scan operator
            return Optional.of(
                    StatsUtils.collectStatistics(aspCtx.getConf(), partList, table, schema, neededColumns, colStatsCached,
                            referencedColumns, true));
        } catch (HiveException e) {
            LOG.error("OmniData Hive failed to retrieve stats ", e);
            return Optional.empty();
        }
    }

    /**
     * estimate reducer number
     *
     * @param totalInputFileSize total input size
     * @param bytesPerReducer Size of bytes available for each reduce
     * @param oldReducers old reduce number
     * @return new reduce number
     */
    public static int estimateReducersByFileSize(long totalInputFileSize, long bytesPerReducer, int oldReducers) {
        if (totalInputFileSize <= 0) {
            return 0;
        }
        double bytes = Math.max(totalInputFileSize, bytesPerReducer);
        int newReducers = (int) Math.ceil(bytes / (double) bytesPerReducer);
        // The new reduce number cannot be less than the basic reduce number
        newReducers = Math.max(BASED_REDUCES, newReducers);
        // The new reduce number cannot be greater than the old reduce number
        newReducers = Math.min(oldReducers, newReducers);
        return newReducers;
    }

    /**
     * Update TezEdgeProperty and set reduce tasks in ReduceWork
     *
     * @param reduceWork ReduceWork
     * @param optimizedNumReduces optimized reduce numbers
     * @param bytesPerReducer size per reducer
     */
    public static void setOptimizedNumReduceTasks(ReduceWork reduceWork, int optimizedNumReduces,
                                                  long bytesPerReducer) {
        // limit for reducers
        final int maxReducers = reduceWork.getNumReduceTasks();
        if (!reduceWork.isAutoReduceParallelism()) {
            // set optimized reduce number
            reduceWork.setNumReduceTasks(Math.min(optimizedNumReduces, maxReducers));
            return;
        }
        // tez auto reduce parallelism should be set to the 'minPartition' and 'maxPartition'
        float minPartitionFactor = 0.5f;
        float maxPartitionFactor = 1.0f;

        // min we allow tez to pick
        int minPartition = Math.max(1, (int) ((float) optimizedNumReduces * minPartitionFactor));
        minPartition = Math.min(minPartition, maxReducers);

        // max we allow tez to pick
        int maxPartition = Math.max(1, (int) ((float) optimizedNumReduces * maxPartitionFactor));
        maxPartition = Math.min(maxPartition, maxReducers);

        // set optimized reduce number
        reduceWork.setNumReduceTasks(optimizedNumReduces);
        reduceWork.setMinReduceTasks(minPartition);
        reduceWork.setMaxReduceTasks(maxPartition);

        // update TezEdgeProperty
        reduceWork.getEdgePropRef()
                .setAutoReduce(reduceWork.getEdgePropRef().getHiveConf(), true, minPartition, maxPartition,
                        bytesPerReducer);
    }

    /**
     * The ReduceWork entry is optimized
     *
     * @param tableScanOperatorMap TableScanOperator's map
     * @param reduceWorkMap ReduceWork's map
     * @param hiveConf hive conf
     * @param pctx The current parse context.
     */
    public static void optimizeReduceWork(Map<String, TableScanOperator> tableScanOperatorMap,
                                          Map<String, ReduceWork> reduceWorkMap, HiveConf hiveConf, ParseContext pctx) {
        // use 'tez.grouping.max-size' to optimize bytesPerReducer
        long bytesPerReducer = Math.max(hiveConf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER),
                TezSplitGrouper.TEZ_GROUPING_SPLIT_MAX_SIZE_DEFAULT);
        for (ReduceWork reduceWork : reduceWorkMap.values()) {
            if (!reduceWork.isAutoReduceParallelism() || reduceWork.getTagToInput().values().size() <= 0) {
                continue;
            }
            // supported 'isAutoReduceParallelism' is true
            int optimizedNumReduces = estimateNumReducesByInput(reduceWork, tableScanOperatorMap, reduceWorkMap, pctx,
                    bytesPerReducer);
            if (optimizedNumReduces > 0) {
                NdpStatisticsUtils.setOptimizedNumReduceTasks(reduceWork, optimizedNumReduces, bytesPerReducer);
            }
        }
    }

    /**
     * The LengthPerGroup is optimized based on the selectivity of Agg or Filter
     * to increase the number of splits processed by a Tez Task.
     *
     * @param conf hive conf
     * @return optimized LengthPerGroup
     */
    public static double optimizeLengthPerGroup(Configuration conf) {
        double selectivity = OmniDataConf.getOmniDataTableOptimizedSelectivity(conf);
        double coefficient = 1d;
        double configCoefficient = OmniDataConf.getOmniDataGroupOptimizedCoefficient(conf);
        if (OmniDataConf.getOmniDataAggOptimizedEnabled(conf)) {
            if (configCoefficient > 0) {
                LOG.info("Desired OmniData optimized coefficient overridden by config to: {}", configCoefficient);
                return configCoefficient;
            }
            // OmniData agg optimized
            if (selectivity <= SELECTIVITY_THRESHOLD) {
                int maxAggCoefficient = 10;
                // 1 / selectivity + 1
                coefficient = Math.min((new BigDecimal("1").divide(new BigDecimal(selectivity), OMNIDATA_SCALE,
                        BigDecimal.ROUND_HALF_UP)).add(new BigDecimal("1")).doubleValue(), maxAggCoefficient);
            }
        } else if (OmniDataConf.getOmniDataFilterOptimizedEnabled(conf)) {
            if (configCoefficient > 0) {
                LOG.info("Desired OmniData optimized coefficient overridden by config to: {}", configCoefficient);
                return configCoefficient;
            }
            // OmniData filter optimized
            if (selectivity <= SELECTIVITY_THRESHOLD) {
                int maxFilterCoefficient = 10;
                // 0.5 / selectivity + 0.5
                coefficient = Math.min((new BigDecimal("0.5").divide(new BigDecimal(selectivity), OMNIDATA_SCALE,
                        BigDecimal.ROUND_HALF_UP)).add(new BigDecimal("0.5")).doubleValue(), maxFilterCoefficient);
            }
        } else {
            if (OmniDataConf.getOmniDataGroupOptimizedEnabled(conf)) {
                double maxCoefficient = 2.5d;
                // 0.2 / selectivity + 1
                coefficient = Math.min((new BigDecimal("0.2").divide(new BigDecimal(selectivity), OMNIDATA_SCALE,
                        BigDecimal.ROUND_HALF_UP)).add(new BigDecimal("1")).doubleValue(), maxCoefficient);
            }
        }
        return coefficient;
    }

    /**
     * The Filter in TableScanOperator is changed. The Stats of the Filter needs to be updated.
     *
     * @param pctx The current parse context.
     * @param tableScanOp TableScanOperator
     * @throws SemanticException Exception from SemanticAnalyzer.
     */
    public static void updateFilterStats(ParseContext pctx, TableScanOperator tableScanOp) throws SemanticException {
        AnnotateStatsProcCtx aspCtx = new AnnotateStatsProcCtx(pctx);

        // create a walker which walks the tree in a BFS manner while maintaining the
        // operator stack. The dispatcher generates the plan from the operator tree
        Map<Rule, NodeProcessor> opRules = new LinkedHashMap<>();
        opRules.put(new RuleRegExp("FIL", FilterOperator.getOperatorName() + "%"),
                StatsRulesProcFactory.getFilterRule());

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher dispatcher = new DefaultRuleDispatcher(StatsRulesProcFactory.getDefaultRule(), opRules, aspCtx);
        GraphWalker ogw = new LevelOrderWalker(dispatcher, 0);

        // Create a list of topOp nodes
        ArrayList<Node> topNodes = new ArrayList<>();
        topNodes.add(tableScanOp);
        ogw.startWalking(topNodes, null);
    }

    /**
     * Evaluate whether the part pushdown selectivity meets the requirements. If yes, update the filter and selectivity.
     *
     * @param tableScanOp TableScanOperator
     * @param newExprDesc new Filter ExprNodeDesc
     * @param parseContext The current parse context.
     * @param conf hive conf
     * @return true or false
     */
    public static boolean evaluatePartFilterSelectivity(TableScanOperator tableScanOp, ExprNodeDesc newExprDesc,
                                                        ParseContext parseContext, Configuration conf) {
        Operator<? extends OperatorDesc> operator = tableScanOp.getChildOperators().get(0);
        if (!(operator instanceof VectorFilterOperator)) {
            return false;
        }
        VectorFilterOperator vectorFilterOp = (VectorFilterOperator) operator;
        VectorExpression oldExpr = vectorFilterOp.getPredicateExpression();
        ExprNodeDesc oldExprNodeDesc = vectorFilterOp.getConf().getPredicate();
        try {
            VectorExpression ndpExpr = tableScanOp.getOutputVectorizationContext()
                    .getVectorExpression(newExprDesc, VectorExpressionDescriptor.Mode.FILTER);
            // set new filter expression
            vectorFilterOp.setFilterCondition(ndpExpr);
            // set new filter desc
            vectorFilterOp.getConf().setPredicate(newExprDesc);
            // update filter's statistics
            NdpStatisticsUtils.updateFilterStats(parseContext, tableScanOp);
        } catch (HiveException e) {
            LOG.error("OmniData Hive update filter stats failed", e);
            return false;
        }
        // Calculate the new selection rate
        double newSelectivity = tableScanOp.getConf().getOmniDataSelectivity() / NdpStatisticsUtils.getSelectivity(
                tableScanOp);
        if (newSelectivity <= OmniDataConf.getOmniDataFilterSelectivity(conf)) {
            // set table's selectivity
            tableScanOp.getConf().setOmniDataSelectivity(newSelectivity);
            LOG.info("Table [{}] part selectivity is {}", tableScanOp.getConf().getAlias(), newSelectivity);
            return true;
        }
        vectorFilterOp.setFilterCondition(oldExpr);
        vectorFilterOp.getConf().setPredicate(oldExprNodeDesc);
        try {
            NdpStatisticsUtils.updateFilterStats(parseContext, tableScanOp);
        } catch (SemanticException e) {
            LOG.error("OmniData Hive update filter stats failed", e);
            return false;
        }
        LOG.info("Table [{}] failed to part push down, since selectivity[{}] > threshold[{}]",
                tableScanOp.getConf().getAlias(), newSelectivity, OmniDataConf.getOmniDataFilterSelectivity(conf));
        return false;
    }

    private static void generateMapWork(MapWork mapWork, Map<String, MapWork> mapWorkMap,
                                        Map<String, TableScanOperator> tableScanOperatorMap) {
        mapWork.getAliasToWork().values().forEach(operator -> {
            if (operator instanceof TableScanOperator) {
                tableScanOperatorMap.put(mapWork.getName(), (TableScanOperator) operator);
                mapWorkMap.put(mapWork.getName(), mapWork);
            }
        });
    }

    private static int estimateNumReducesByInput(ReduceWork reduceWork,
                                                 Map<String, TableScanOperator> tableScanOperatorMap, Map<String, ReduceWork> reduceWorkMap, ParseContext pctx,
                                                 long bytesPerReducer) {
        boolean isSupported = true;
        long totalOptimizedSize = 0L;
        int reduces = 0;
        int totalReduces = 0;
        // we need to add up all the estimates from reduceWork's input
        for (String inputWorkName : reduceWork.getTagToInput().values()) {
            if (tableScanOperatorMap.containsKey(inputWorkName)) {
                TableScanOperator tableScanOp = tableScanOperatorMap.get(inputWorkName);
                if (tableScanOp.getConf().getStatistics() != null) {
                    // add the optimized input sizes
                    totalOptimizedSize += NdpStatisticsUtils.estimateOptimizedSize(tableScanOp, pctx);
                } else {
                    isSupported = false;
                    break;
                }
            } else if (reduceWorkMap.containsKey(inputWorkName)) {
                reduces++;
                // add the child's reduce number
                totalReduces += reduceWorkMap.get(inputWorkName).getNumReduceTasks();
            } else {
                // unsupported MergeJoinWork
                isSupported = false;
                break;
            }
        }
        int optimizedNumReduces = -1;
        if (isSupported) {
            optimizedNumReduces = NdpStatisticsUtils.estimateReducersByFileSize(totalOptimizedSize, bytesPerReducer,
                    reduceWork.getNumReduceTasks());
            // reduce work exists in the input
            if (reduces > 0) {
                int avgReduces = totalReduces / reduces;
                // When the number of reduce works of a map work is less than the BASED_REDUCES,
                // the number of reduce work can be ignored.
                optimizedNumReduces = (optimizedNumReduces > NdpStatisticsUtils.BASED_REDUCES) ? optimizedNumReduces
                        + avgReduces : avgReduces;
            }
        }
        return optimizedNumReduces;
    }
}