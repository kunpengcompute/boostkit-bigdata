package org.apache.hadoop.hive.ql.omnidata.physical;

import static org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpEngineEnum.MR;
import static org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpEngineEnum.Tez;

import com.huawei.boostkit.omnidata.model.AggregationInfo;
import com.huawei.boostkit.omnidata.model.Predicate;

import com.google.common.collect.ImmutableMap;

import io.prestosql.spi.relation.RowExpression;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.config.NdpConf;
import org.apache.hadoop.hive.ql.omnidata.operator.aggregation.OmniDataAggregation;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpEngineEnum;
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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
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

    private NdpConf ndpConf;

    private NdpEngineEnum engine;

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
     * Hive agg pushDown optimize
     */
    private boolean isAggOptimized = false;

    @Override
    public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
        this.hiveConf = pctx.getConf();
        this.ndpConf = new NdpConf(hiveConf);
        this.context = pctx.getContext();
        Dispatcher dispatcher = new NdpDispatcher();
        TaskGraphWalker walker = new TaskGraphWalker(dispatcher);
        List<Node> topNodes = new ArrayList<>(pctx.getRootTasks());
        walker.startWalking(topNodes, null);
        hiveConf.set(NdpConf.NDP_AGG_OPTIMIZED_ENABLE, String.valueOf(isAggOptimized));
        return pctx;
    }

    private class NdpDispatcher implements Dispatcher {
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
            if (nodeOutputs == null || nodeOutputs.length == 0) {
                throw new SemanticException("No Dispatch Context");
            }

            if (!ndpConf.getNdpEnabled() || !NdpPlanChecker.checkRollUp(context.getCmd())) {
                return null;
            }

            // host resources status map
            Map<String, NdpStatusInfo> ndpStatusInfoMap = new HashMap<>(NdpStatusManager.getNdpZookeeperData(ndpConf));
            if (!NdpPlanChecker.checkHostResources(ndpStatusInfoMap)) {
                return null;
            }

            Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;

            if (!currTask.isMapRedLocalTask() && !currTask.isMapRedTask()) {
                return null;
            }

            List<Integer> workIndexList = new ArrayList<>();
            List<BaseWork> works = new ArrayList<>();
            List<Operator<? extends OperatorDesc>> topOp = new ArrayList<>();
            if (currTask.getWork() instanceof TezWork) {
                // tez
                engine = Tez;
                TezWork tezWork = (TezWork) currTask.getWork();
                works.addAll(tezWork.getAllWork());
                for (int i = 0; i < works.size(); i++) {
                    BaseWork work = works.get(i);
                    if (work instanceof MapWork) {
                        if (((MapWork) work).getAliasToWork().size() == 1) {
                            topOp.addAll(((MapWork) work).getAliasToWork().values());
                            workIndexList.add(i);
                        }
                    }
                }
            } else {
                // unsupported
                return null;
            }
            int index = 0;
            int ndpTableNums = 0;
            for (Operator<? extends OperatorDesc> operator : topOp) {
                if (operator instanceof TableScanOperator) {
                    TableScanOperator tableScanOp = (TableScanOperator) operator;
                    NdpPredicateInfo ndpPredicateInfo = new NdpPredicateInfo(false);
                    // check TableScanOp
                    if (checkTableScanOp(tableScanOp, works.get(workIndexList.get(index)))) {
                        Optional<RowExpression> filter = Optional.empty();
                        Optional<AggregationInfo> aggregation = Optional.empty();
                        OptionalLong limit = OptionalLong.empty();
                        OmniDataPredicate omniDataPredicate = new OmniDataPredicate(tableScanOp);
                        omniDataPredicate.setSelectExpressions(selectDesc);
                        // get OmniData filter expression
                        if (isPushDownFilter) {
                            filter = getOmniDataFilter(omniDataPredicate);
                        }
                        // get OmniData agg expression
                        if (isPushDownAgg) {
                            // The output column of the aggregation needs to be processed separately.
                            aggregation = getOmniDataAggregation(omniDataPredicate);
                        }
                        // get OmniData select expression
                        if (isPushDownSelect && !isPushDownAgg) {
                            omniDataPredicate = new OmniDataPredicate(tableScanOp);
                            omniDataPredicate.setSelectExpressions(selectDesc);
                            omniDataPredicate.addProjectionsByTableScan(tableScanOp);
                        }
                        // get OmniData limit expression
                        if (isPushDownLimit) {
                            limit = getOmniDataLimit();
                        }
                        // the decode type must exist
                        if ((isPushDownFilter || isPushDownAgg) && omniDataPredicate.getDecodeTypes().size() > 0) {
                            replaceTableScanOp(tableScanOp, works.get(workIndexList.get(index)));
                            Predicate predicate = new Predicate(omniDataPredicate.getTypes(),
                                    omniDataPredicate.getColumns(), filter, omniDataPredicate.getProjections(),
                                    ImmutableMap.of(), ImmutableMap.of(), aggregation, limit);
                            ndpPredicateInfo = new NdpPredicateInfo(true, isPushDownAgg, isPushDownFilter,
                                    omniDataPredicate.getHasPartitionColumns(), predicate,
                                    tableScanOp.getConf().getNeededColumnIDs(), omniDataPredicate.getDecodeTypes(),
                                    omniDataPredicate.getDecodeTypesWithAgg());
                            NdpStatusManager.setOmniDataHostToConf(hiveConf, ndpStatusInfoMap);
                            printPushDownInfo(tableScanOp.getConf().getAlias(), ndpStatusInfoMap);
                            ndpTableNums++;
                        }
                    }
                    initPushDown();
                    // set ndpPredicateInfo after serialization
                    tableScanOp.getConf()
                            .setNdpPredicateInfoStr(NdpSerializationUtils.serializeNdpPredicateInfo(ndpPredicateInfo));
                }
                index++;
            }
            if (ndpTableNums != 1) {
                isAggOptimized = false;
            }
            return null;
        }

        private boolean checkTableScanOp(TableScanOperator tableScanOp, BaseWork work) {
            if (NdpPlanChecker.checkTableScanNumChild(tableScanOp) && NdpPlanChecker.checkHiveType(tableScanOp)
                    && NdpPlanChecker.checkTableSize(tableScanOp, ndpConf) && NdpPlanChecker.checkSelectivity(tableScanOp,
                    ndpConf) && NdpPlanChecker.checkDataFormat(tableScanOp, work)) {
                // scan operator: select agg filter limit
                scanTableScanChildOperators(tableScanOp.getChildOperators());
                return isPushDownAgg || isPushDownFilter;
            }
            return false;
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

        private Optional<RowExpression> getOmniDataFilter(OmniDataPredicate omniDataPredicate) {
            NdpFilter ndpFilter = new NdpFilter(filterDesc);
            NdpFilter.NdpFilterMode mode = ndpFilter.getMode();
            if (mode.equals(NdpFilter.NdpFilterMode.PART)) {
                isPushDownPartFilter = true;
                unsupportedFilterDesc = ndpFilter.getUnPushDownFuncDesc();
                filterDesc = (ExprNodeGenericFuncDesc) ndpFilter.getPushDownFuncDesc();
                // The AGG does not support part push down
                isPushDownAgg = false;
            } else if (mode.equals(NdpFilter.NdpFilterMode.NONE)) {
                isPushDownFilter = false;
                return Optional.empty();
            }
            OmniDataFilter omniDataFilter = new OmniDataFilter(omniDataPredicate);
            // ExprNodeGenericFuncDesc need to clone
            RowExpression filterRowExpression = omniDataFilter.getFilterExpression(
                    (ExprNodeGenericFuncDesc) filterDesc.clone(), ndpFilter);
            if (filterRowExpression == null) {
                isPushDownFilter = false;
                return Optional.empty();
            }
            return Optional.of(filterRowExpression);
        }

        private Optional<AggregationInfo> getOmniDataAggregation(OmniDataPredicate omniDataPredicate) {
            OmniDataAggregation omniDataAggregation = new OmniDataAggregation(omniDataPredicate);
            AggregationInfo aggregationInfo = omniDataAggregation.getAggregation(aggDesc);
            if (aggregationInfo == null) {
                isPushDownAgg = false;
                return Optional.empty();
            }
            return Optional.of(aggregationInfo);
        }

        private OptionalLong getOmniDataLimit() {
            return OmniDataLimit.getOmniDataLimit(limitDesc.getLimit());
        }

        private DataTypePhysicalVariation[] getDataTypePhysicalVariation(VectorizationContext vOutContext) {
            List<DataTypePhysicalVariation> variations = new ArrayList<>();
            vOutContext.getProjectedColumns().forEach(c -> {
                try {
                    variations.add(vOutContext.getDataTypePhysicalVariation(c));
                } catch (HiveException e) {
                    e.printStackTrace();
                }
            });
            return variations.toArray(new DataTypePhysicalVariation[0]);
        }

        private void replaceTableScanOp(TableScanOperator tableScanOp, BaseWork work) {
            if (isPushDownFilter) {
                if (isPushDownPartFilter) {
                    replaceTableScanRawFilter(tableScanOp, tableScanOp.getChildOperators(), unsupportedFilterDesc);
                    replaceRawVectorizedRowBatchCtx(tableScanOp, work);
                } else {
                    removeTableScanRawFilter(tableScanOp.getChildOperators());
                }
            } else {
                filterDesc = null;
            }
            if (isPushDownAgg) {
                removeTableScanRawAggregation(tableScanOp.getChildOperators(), work);
                removeTableScanRawSelect(tableScanOp.getChildOperators());
                isAggOptimized = isPushDownAgg;
            } else {
                aggDesc = null;
            }
        }

        private void replaceAggRawVectorizedRowBatchCtx(BaseWork work, VectorizationContext vOutContext) {
            VectorizedRowBatchCtx oldCtx = work.getVectorizedRowBatchCtx();
            VectorizedRowBatchCtx ndpCtx = new VectorizedRowBatchCtx(
                    vOutContext.getInitialColumnNames().toArray(new String[0]), vOutContext.getInitialTypeInfos(),
                    getDataTypePhysicalVariation(vOutContext),
                    vOutContext.getProjectedColumns().stream().mapToInt(Integer::valueOf).toArray(),
                    oldCtx.getPartitionColumnCount(), 0, oldCtx.getNeededVirtualColumns(),
                    vOutContext.getScratchColumnTypeNames(), vOutContext.getScratchDataTypePhysicalVariations());
            work.setVectorizedRowBatchCtx(ndpCtx);
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

        private void replaceTableScanRawFilter(TableScanOperator tableScanOp,
                                               List<Operator<? extends OperatorDesc>> operators, ExprNodeDesc newExprDesc) {
            try {
                for (Operator<? extends OperatorDesc> child : operators) {
                    if (child instanceof VectorFilterOperator) {
                        // Convert 'ExprNodeDesc' to 'VectorExpression' via VectorizationContext, mode is 'FILTER'
                        VectorExpression ndpExpr = tableScanOp.getOutputVectorizationContext()
                                .getVectorExpression(newExprDesc, VectorExpressionDescriptor.Mode.FILTER);
                        // Replace with new filter expression
                        ((VectorFilterOperator) child).setFilterCondition(ndpExpr);
                        return;
                    }
                    replaceTableScanRawFilter(tableScanOp, child.getChildOperators(), newExprDesc);
                }
            } catch (SemanticException e) {
                LOG.error("Exception when trying to remove partition predicates: fail to find child from parent", e);
            } catch (HiveException e) {
                e.printStackTrace();
            }
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
                        if (engine.equals(MR)) {
                            replaceAggRawVectorizedRowBatchCtx(work,
                                    ((VectorGroupByOperator) child).getOutputVectorizationContext());
                        }
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
            try {
                for (Operator<? extends OperatorDesc> child : operators) {
                    if (child instanceof VectorSelectOperator) {
                        if (engine.equals(MR)) {
                            // remove raw VectorSelectOperator
                            child.getParentOperators().get(0).removeChildAndAdoptItsChildren(child);
                        } else {
                            // remove raw VectorExpression
                            ((VectorSelectOperator) child).setvExpressions(new VectorExpression[] {});
                        }
                        return;
                    }
                    removeTableScanRawSelect(child.getChildOperators());
                }
            } catch (SemanticException e) {
                LOG.error("Exception when trying to remove partition predicates: fail to find child from parent", e);
            }
        }

        private void printPushDownInfo(String tableName, Map<String, NdpStatusInfo> ndpStatusInfoMap) {
            String pushDownInfo = String.format(
                    "Table [%s] Push Down Info [ Select:[%s], Filter:[%s], Aggregation:[%s], Group By:[%s], Limit:[%s], Raw Filter:[%s], Host Map:[%s]",
                    tableName, ((selectDesc != null) && isPushDownAgg) ? Arrays.stream(selectDesc.getSelectExpressions())
                            .map(VectorExpression::toString)
                            .collect(Collectors.joining(", ")) : "", (filterDesc != null) ? filterDesc.toString() : "",
                    (aggDesc != null) ? (aggDesc.getAggregators()
                            .stream()
                            .map(AggregationDesc::getExprString)
                            .collect(Collectors.joining(", "))) : "",
                    (aggDesc != null && aggDesc.getKeyString() != null) ? aggDesc.getKeyString() : "",
                    (limitDesc != null) ? limitDesc.getLimit() : "",
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
        }
    }
}