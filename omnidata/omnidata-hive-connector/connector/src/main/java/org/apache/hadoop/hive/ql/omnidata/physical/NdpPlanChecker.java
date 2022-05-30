package org.apache.hadoop.hive.ql.omnidata.physical;

import static org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum.*;
import static org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpHiveOperatorEnum.*;

import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.omnidata.config.NdpConf;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpHiveOperatorEnum;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.omnidata.operator.filter.NdpFilter.*;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusInfo;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusManager;
import org.apache.hadoop.hive.ql.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

/**
 * Used to check the validity of the operation during the Ndp planning phase.
 *
 * @since 2022-01-14
 */
public class NdpPlanChecker {
    private NdpPlanChecker() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(NdpPlanChecker.class);

    private static final ImmutableSet<String> SUPPORTED_HIVE_TYPES = ImmutableSet.of("bigint", "boolean", "char",
            "date", "double", "float", "int", "smallint", "string", "tinyint", "varchar");

    private static final ImmutableSet<String> SUPPORTED_AGGREGATE_FUNCTIONS = ImmutableSet.of("count", "avg", "sum",
            "max", "min");

    private static final ImmutableSet<String> AVG_SUM_FUNCTION_HIVE_TYPES = ImmutableSet.of("bigint", "double", "float",
            "int", "smallint", "tinyint");

    private static final ImmutableSet<NdpUdfEnum> SUPPORTED_HIVE_UDF = ImmutableSet.of(CAST, INSTR, LENGTH, LOWER,
            REPLACE, SPLIT, SUBSCRIPT, SUBSTRING, UPPER, SUBSTR);

    // unsupported: LIKE
    private static final ImmutableSet<NdpHiveOperatorEnum> SUPPORTED_HIVE_OPERATOR = ImmutableSet.of(AND, BETWEEN,
            EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, IN, LESS_THAN, LESS_THAN_OR_EQUAL, NOT, NOT_EQUAL, NOT_NULL, NULL,
            OR);

    // NdpLeafOperator.LIKE
    private static final ImmutableSet<NdpLeafOperator> SUPPORTED_HIVE_LEAF_OPERATOR = ImmutableSet.of(
            NdpLeafOperator.BETWEEN, NdpLeafOperator.IN, NdpLeafOperator.LESS_THAN, NdpLeafOperator.GREATER_THAN,
            NdpLeafOperator.LESS_THAN_OR_EQUAL, NdpLeafOperator.GREATER_THAN_OR_EQUAL, NdpLeafOperator.EQUAL,
            NdpLeafOperator.IS_NULL);

    /**
     * Currently, 'roll up' is not supported.
     *
     * @param cmd hive query sql
     * @return true or false
     */
    public static boolean checkRollUp(String cmd) {
        if (cmd.replaceAll("\\s*", "").toLowerCase().contains("rollup(")) {
            LOG.info("SQL [{}] failed to push down, since contains unsupported operator ROLLUP", cmd);
            return false;
        }
        return true;
    }

    /**
     * Currently, only one child is supported.
     *
     * @param tableScanOp TableScanOperator
     * @return true or false
     */
    public static boolean checkTableScanNumChild(TableScanOperator tableScanOp) {
        if (tableScanOp.getNumChild() == 1) {
            return true;
        } else {
            LOG.info("Table [{}] failed to push down, since unsupported the number of TableScanOperator's child : [{}]",
                    tableScanOp.getConf().getAlias(), tableScanOp.getNumChild());
            return false;
        }
    }

    /**
     * Currently, two data formats are supported: Orc and Parquet.
     *
     * @param tableScanOp TableScanOperator
     * @return true or false
     */
    public static boolean checkDataFormat(TableScanOperator tableScanOp, BaseWork work) {
        String tableName = tableScanOp.getConf().getAlias();
        // TableMetadata may be 'null'
        if (tableScanOp.getConf().getTableMetadata() == null) {
            if (work instanceof MapWork) {
                PartitionDesc desc = ((MapWork) work).getAliasToPartnInfo().get(tableScanOp.getConf().getAlias());
                if (desc != null) {
                    String inputFormat = desc.getInputFileFormatClass().getSimpleName();
                    String outputFormat = desc.getOutputFileFormatClass().getSimpleName();
                    return checkDataFormat(inputFormat, outputFormat, tableName);
                } else {
                    LOG.info("Table [{}] failed to push down, since PartitionDesc is null",
                            tableName);
                    return false;
                }
            } else {
                LOG.info("Table [{}] failed to push down, since unsupported this work: [{}]",
                        tableName, work.getClass().getSimpleName());
                return false;
            }
        } else {
            String inputFormat = tableScanOp.getConf().getTableMetadata().getInputFormatClass().getSimpleName();
            String outputFormat = tableScanOp.getConf().getTableMetadata().getOutputFormatClass().getSimpleName();
            return checkDataFormat(inputFormat, outputFormat, tableName);
        }
    }

    /**
     * Currently, two data formats are supported: Orc and Parquet.
     *
     * @param inputFormat hive input data format
     * @param outputFormat hive output data format
     * @param tableName hive table name
     * @return true or false
     */
    public static boolean checkDataFormat(String inputFormat, String outputFormat, String tableName) {
        if (!(inputFormat.toLowerCase(Locale.ENGLISH).contains("orc") || inputFormat.toLowerCase(Locale.ENGLISH)
                .contains("parquet"))) {
            LOG.info("Table [{}] failed to push down, since unsupported this input data format: [{}]", tableName,
                    inputFormat);
            return false;
        }
        if (!(outputFormat.toLowerCase(Locale.ENGLISH).contains("orc") || outputFormat.toLowerCase(Locale.ENGLISH)
                .contains("parquet"))) {
            LOG.info("Table [{}] failed to push down, since unsupported this output data format: [{}]", tableName,
                    inputFormat);
            return false;
        }
        return true;
    }

    /**
     * Check whether host resources are available.
     *
     * @param ndpStatusInfoMap ndp host resource
     * @return true or false
     */
    public static boolean checkHostResources(Map<String, NdpStatusInfo> ndpStatusInfoMap) {
        if (ndpStatusInfoMap.size() == 0) {
            LOG.info("OmniData Hive failed to push down, the number of OmniData server is 0.");
            return false;
        }
        ndpStatusInfoMap.entrySet().removeIf(info -> !checkHostResources(info.getValue()));
        if (ndpStatusInfoMap.size() == 0) {
            LOG.info("OmniData Hive failed to push down, the number of OmniData server is 0.");
            return false;
        }
        return true;
    }

    public static boolean checkHostResources(NdpStatusInfo statusInfo) {
        if (statusInfo == null) {
            return false;
        }
        if (statusInfo.getRunningTasks() > statusInfo.getMaxTasks() * statusInfo.getThreshold()) {
            return false;
        }
        return true;
    }

    public static boolean checkPushDown(Configuration conf, boolean isPushDown) {
        return isPushDown && conf.get(NdpStatusManager.NDP_DATANODE_HOSTNAMES) != null
                && conf.get(NdpStatusManager.NDP_DATANODE_HOSTNAMES).length() > 0;
    }

    /**
     * Check whether the Udf in white list
     *
     * @param udf hive udf
     * @return true or false
     */
    public static boolean checkUdfByWhiteList(NdpUdfEnum udf) {
        return SUPPORTED_HIVE_UDF.contains(udf);
    }

    /**
     * Check whether the operator in white list
     *
     * @param operator hive operator
     * @return true or false
     */
    public static boolean checkOperatorByWhiteList(NdpHiveOperatorEnum operator) {
        return SUPPORTED_HIVE_OPERATOR.contains(operator);
    }

    /**
     * Check whether the leaf operator in white list
     *
     * @param operator hive leaf operator
     * @return true or false
     */
    public static boolean checkLeafOperatorByWhiteList(NdpLeafOperator operator) {
        return SUPPORTED_HIVE_LEAF_OPERATOR.contains(operator);
    }

    public static boolean checkHiveType(String type) {
        String lType = type.toLowerCase(Locale.ENGLISH);
        // Keep the English letters and remove the others. like: char(11) -> char
        if (lType.contains("char")) {
            lType = type.replaceAll("[^a-z<>]", "");
        }
        return SUPPORTED_HIVE_TYPES.contains(lType);
    }

    /**
     * Check whether the data type is supported
     *
     * @param tableScanOp TableScanOperator
     * @return true or false
     */
    public static boolean checkHiveType(TableScanOperator tableScanOp) {
        String[] columnTypes = tableScanOp.getSchemaEvolutionColumnsTypes().split(",");
        for (Integer columnId : tableScanOp.getConf().getNeededColumnIDs()) {
            if (!checkHiveType(columnTypes[columnId])) {
                LOG.info("Table [{}] failed to push down, since unsupported this column type: [{}]",
                        tableScanOp.getConf().getAlias(), columnTypes[columnId]);
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether the filterOperator.
     *
     * @param vectorFilterOperator VectorFilterOperator
     * @return true or false
     */
    public static ExprNodeGenericFuncDesc checkFilterOperator(VectorFilterOperator vectorFilterOperator) {
        if (vectorFilterOperator == null) {
            return null;
        }
        ExprNodeDesc nodeDesc = vectorFilterOperator.getConf().getPredicate();
        if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            return (ExprNodeGenericFuncDesc) nodeDesc;
        }
        LOG.info("FilterOperator failed to push down, since unsupported this ExprNodeDesc: [{}]",
                nodeDesc.getClass().getSimpleName());
        return null;
    }

    /**
     * Check whether the vectorSelectOperator.
     *
     * @param vectorSelectOperator VectorSelectOperator
     * @return true or false
     */
    public static VectorSelectDesc checkSelectOperator(VectorSelectOperator vectorSelectOperator) {
        if (vectorSelectOperator == null) {
            return null;
        }
        SelectDesc selectDesc = vectorSelectOperator.getConf();
        if (selectDesc.getVectorDesc() instanceof VectorSelectDesc) {
            return (VectorSelectDesc) selectDesc.getVectorDesc();
        }
        LOG.info("VectorSelectOperator failed to push down, since unsupported this SelectDesc: [{}]",
                selectDesc.getClass().getSimpleName());
        return null;
    }

    /**
     * Check whether the vectorGroupByOperator
     *
     * @param vectorGroupByOperator VectorGroupByOperator
     * @return true or false
     */
    public static GroupByDesc checkGroupByOperator(VectorGroupByOperator vectorGroupByOperator) {
        if (vectorGroupByOperator != null) {
            VectorGroupByDesc aggVectorsDesc = (VectorGroupByDesc) vectorGroupByOperator.getVectorDesc();
            // Agg or groupby can be pushed down only when agg or groupby exists.
            if (aggVectorsDesc.getKeyExpressions().length > 0 || aggVectorsDesc.getVecAggrDescs().length > 0) {
                for (VectorAggregationDesc agg : aggVectorsDesc.getVecAggrDescs()) {
                    if (!checkAggregationDesc(agg.getAggrDesc())) {
                        return null;
                    }
                }
                return vectorGroupByOperator.getConf();
            }
        }
        LOG.info("VectorGroupByOperator failed to push down");
        return null;
    }


    /**
     * Check whether Limit offset > 0
     *
     * @param vectorLimitOperator VectorLimitOperator
     * @return true or false
     */
    public static LimitDesc checkLimitOperator(VectorLimitOperator vectorLimitOperator) {
        if (vectorLimitOperator == null) {
            return null;
        }
        LimitDesc limitDesc = vectorLimitOperator.getConf();
        if (limitDesc.getOffset() == null && limitDesc.getLimit() > 0) {
            return limitDesc;
        }
        LOG.info("VectorLimitOperator failed to push down, since unsupported Limit offset > 0");
        return null;
    }

    public static boolean checkAggregationDesc(AggregationDesc agg) {
        if (!SUPPORTED_AGGREGATE_FUNCTIONS.contains(agg.getGenericUDAFName())) {
            LOG.info("Aggregation failed to push down, since unsupported this [{}]", agg.getGenericUDAFName());
            return false;
        }
        switch (agg.getGenericUDAFName()) {
            case "count":
                return checkCountFunction(agg);
            case "avg":
                return checkAvgFunction(agg);
            case "sum":
                return checkSumFunction(agg);
            case "min":
                return checkMinFunction(agg);
            case "max":
                return checkMaxFunction(agg);
            default:
                return false;
        }
    }

    public static boolean checkCountFunction(AggregationDesc agg) {
        if (agg.getDistinct()) {
            LOG.info("Aggregation [{}] failed to push down, since unsupported [distinct]", agg.getGenericUDAFName());
            return false;
        }
        for (ExprNodeDesc parameter : agg.getParameters()) {
            if (!checkHiveType(parameter.getTypeString())) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkAvgFunction(AggregationDesc agg) {
        return checkSumFunction(agg);
    }

    public static boolean checkSumFunction(AggregationDesc agg) {
        if (agg.getDistinct()) {
            LOG.info("Aggregation [{}] failed to push down, since unsupported [distinct]", agg.getGenericUDAFName());
            return false;
        }
        boolean isConstant = false;
        for (ExprNodeDesc parameter : agg.getParameters()) {
            // check whether a parameter is a constant, If all are constants, do not push down.
            if (!(parameter instanceof ExprNodeConstantDesc)) {
                isConstant = true;
                if ((!checkHiveType(parameter.getTypeString())) || (!AVG_SUM_FUNCTION_HIVE_TYPES.contains(
                        parameter.getTypeString()))) {
                    LOG.info("Aggregation [{}] failed to push down, since unsupported this column type: [{}]",
                            agg.getGenericUDAFName(), parameter.getTypeString());
                    return false;
                }
            }
        }
        return isConstant;
    }

    public static boolean checkMinFunction(AggregationDesc agg) {
        if (agg.getDistinct()) {
            LOG.info("Aggregation [{}] failed to push down, since unsupported [distinct]", agg.getGenericUDAFName());
            return false;
        }
        boolean isConstant = false;
        for (ExprNodeDesc parameter : agg.getParameters()) {
            // check whether a parameter is a constant, If all are constants, do not push down.
            if (!(parameter instanceof ExprNodeConstantDesc)) {
                isConstant = true;
                if (!checkHiveType(parameter.getTypeString())) {
                    return false;
                }
            }
        }
        return isConstant;
    }

    public static boolean checkMaxFunction(AggregationDesc agg) {
        return checkMinFunction(agg);
    }

    public static double getSelectivity(TableScanOperator tableScanOp) {
        double selectivity = 0.5;
        if (tableScanOp.getConf().getStatistics() == null) {
            return selectivity;
        }
        try {
            long tableCount = tableScanOp.getConf().getStatistics().getNumRows();
            long filterCount = tableScanOp.getChildOperators().get(0).getConf().getStatistics().getNumRows();
            if (tableCount > 0) {
                selectivity = 1.0 * filterCount / tableCount;
            }
        } catch (Exception e) {
            LOG.error("Can't calculate the selectivity", e);
        }
        return selectivity;
    }

    /**
     * Check whether the filter selectivity is supported
     *
     * @param tableScanOp TableScanOperator
     * @param ndpConf NdpConf
     * @return true or false
     */
    public static boolean checkSelectivity(TableScanOperator tableScanOp, NdpConf ndpConf) {
        if (ndpConf.getNdpFilterSelectivityEnable()) {
            double currentSelectivity = getSelectivity(tableScanOp);
            if (currentSelectivity > ndpConf.getNdpFilterSelectivity()) {
                LOG.info("Table [{}] failed to push down, since selectivity[{}] > threshold[{}]",
                        tableScanOp.getConf().getAlias(), currentSelectivity, ndpConf.getNdpFilterSelectivity());
                return false;
            } else {
                LOG.info("Table [{}] selectivity is {}", tableScanOp.getConf().getAlias(), currentSelectivity);
                return true;
            }
        } else {
            LOG.info("Table [{}] filter selectivity is unenabled", tableScanOp.getConf().getAlias());
            return true;
        }
    }

    /**
     * Check whether the table size is supported
     *
     * @param tableScanOp TableScanOperator
     * @param ndpConf NdpConf
     * @return true or false
     */
    public static boolean checkTableSize(TableScanOperator tableScanOp, NdpConf ndpConf) {
        if (tableScanOp.getConf().getStatistics() == null) {
            return false;
        }
        long currentTableSize = tableScanOp.getConf().getStatistics().getDataSize();
        if (currentTableSize < ndpConf.getNdpTablesSizeThreshold()) {
            LOG.info("Table [{}] failed to push down, since table size[{}] < threshold[{}]",
                    tableScanOp.getConf().getAlias(), currentTableSize, ndpConf.getNdpTablesSizeThreshold());
            return false;
        }
        return true;
    }

}