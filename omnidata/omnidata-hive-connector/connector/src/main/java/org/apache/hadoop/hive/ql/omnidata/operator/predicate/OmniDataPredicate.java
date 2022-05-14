package org.apache.hadoop.hive.ql.omnidata.operator.predicate;

import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.stream.Collectors.toList;

import com.huawei.boostkit.omnidata.model.Column;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.operator.aggregation.NdpArithmeticExpressionInfo;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpArithmeticEnum;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * OmniData Predicate Info
 * need : columns types projections decodeTypes
 * columns: used column information
 * projections && types indicates the returned field.
 *
 * @since 2021-12-07
 */
public class OmniDataPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(OmniDataPredicate.class);

    /**
     * returned OmniData Columns
     */
    private List<Column> columns = new ArrayList<>();

    /**
     * returned OmniData Types
     */
    private List<Type> types = new ArrayList<>();

    private List<RowExpression> projections = new ArrayList<>();

    private List<String> decodeTypes = new ArrayList<>();

    private List<Boolean> decodeTypesWithAgg = new ArrayList<>();

    private Map<String, Integer> colName2ProjectId = new HashMap<>();

    private Map<String, Integer> colName2ColIndex = new HashMap<>();

    private Map<Integer, Integer> colId2ColIndex = new HashMap<>();

    private Map<Integer, String> colId2ColName = new HashMap<>();

    private VectorExpression[] expressions;

    private boolean hasPartitionColumns = false;

    private boolean isPushDown = true;

    public boolean isPushDown() {
        return isPushDown;
    }

    /**
     * initialize needed columns
     *
     * @param tableScanOp TableScanOperator
     */
    public OmniDataPredicate(TableScanOperator tableScanOp) {
        int index = 0;
        String[] columnTypes = tableScanOp.getSchemaEvolutionColumnsTypes().split(",");
        String[] columnNames = tableScanOp.getSchemaEvolutionColumns().split(",");
        for (int columnId : tableScanOp.getConf().getNeededColumnIDs()) {
            Type omniDataType = OmniDataUtils.transOmniDataType(columnTypes[columnId]);
            columns.add(new Column(columnId, columnNames[columnId], omniDataType));
            colId2ColIndex.put(columnId, index);
            colId2ColName.put(columnId, columnNames[columnId]);
            colName2ColIndex.put(columnNames[columnId], index);
            index++;
        }

        // add partition column
        List<String> referencedColumns = tableScanOp.getConf().getReferencedColumns();
        if (referencedColumns != null && referencedColumns.size() > tableScanOp.getConf().getNeededColumnIDs().size()) {
            List<ColumnInfo> signature = tableScanOp.getSchema().getSignature();
            List<String> names = signature.stream().map(ColumnInfo::getInternalName).collect(toList());
            for (String columnName : referencedColumns) {
                List<Integer> neededColumnIds = tableScanOp.getConf().getNeededColumnIDs();
                if (!names.contains(columnName) || neededColumnIds.contains(names.indexOf(columnName))) {
                    continue;
                }
                int columnId = names.indexOf(columnName);
                ColumnInfo columnInfo = signature.get(columnId);
                if (columnInfo.getIsVirtualCol()) {
                    Type omniDataType = OmniDataUtils.transOmniDataType(columnInfo.getTypeName());
                    columns.add(new Column(columnId, columnName, omniDataType, true, null));
                    colId2ColIndex.put(columnId, index);
                    colId2ColName.put(columnId, columnName);
                    colName2ColIndex.put(columnName, index);
                    hasPartitionColumns = true;
                    index++;
                }
            }
        }
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Type> getTypes() {
        return types;
    }

    public List<RowExpression> getProjections() {
        return projections;
    }

    public List<String> getDecodeTypes() {
        return decodeTypes;
    }

    public void setDecodeTypes(List<String> decodeTypes) {
        this.decodeTypes = decodeTypes;
    }

    public List<Boolean> getDecodeTypesWithAgg() {
        return decodeTypesWithAgg;
    }

    public void setDecodeTypesWithAgg(List<Boolean> decodeTypesWithAgg) {
        this.decodeTypesWithAgg = decodeTypesWithAgg;
    }

    public Map<String, Integer> getColName2ProjectId() {
        return colName2ProjectId;
    }

    public Map<String, Integer> getColName2ColIndex() {
        return colName2ColIndex;
    }

    public Map<Integer, Integer> getColId2ColIndex() {
        return colId2ColIndex;
    }

    public Map<Integer, String> getColId2ColName() {
        return colId2ColName;
    }

    public boolean getHasPartitionColumns() {
        return hasPartitionColumns;
    }

    public void setSelectExpressions(VectorSelectDesc vectorSelectDesc) {
        if (vectorSelectDesc != null) {
            expressions = vectorSelectDesc.getSelectExpressions();
        } else {
            expressions = new VectorExpression[] {};
        }
    }

    public VectorExpression[] getExpressions() {
        return expressions;
    }

    private void addProjections(int outputColumnId, boolean isAgg) {
        Type omniDataType = columns.get(colId2ColIndex.get(outputColumnId)).getType();
        types.add(omniDataType);
        colName2ProjectId.put(columns.get(colId2ColIndex.get(outputColumnId)).getName(), projections.size());
        projections.add(new InputReferenceExpression(colId2ColIndex.get(outputColumnId), omniDataType));
        decodeTypes.add(omniDataType.toString());
        decodeTypesWithAgg.add(isAgg);
    }

    /**
     * Use the Hive TableScanOperator to add OmniData projections
     *
     * @param tableScanOp Hive TableScanOperator
     */
    public void addProjectionsByTableScan(TableScanOperator tableScanOp) {
        tableScanOp.getConf().getNeededColumnIDs().forEach(c -> addProjections(c, false));
    }

    public void addProjectionsByAgg(int outputColumnId) {
        checkAndClearColumnInfo(outputColumnId);
        if (checkColumnAlreadyExists(outputColumnId)) {
            addProjections(outputColumnId, true);
        } else {
            addProjectionsByVectorExpression(outputColumnId, true);
        }
    }

    public boolean addProjectionsByGroupByKey(int outputColumnId) {
        if (checkColumnAlreadyExists(outputColumnId)) {
            if (colId2ColIndex.containsKey(outputColumnId)) {
                addProjections(outputColumnId, false);
                return true;
            }
        }
        return false;
    }

    /**
     * count() need to add bigint decode type
     */
    public void addProjectionsByAggCount(int outputColumnId) {
        checkAndClearColumnInfo(outputColumnId);
        if (checkColumnAlreadyExists(outputColumnId)) {
            Type omniDataType = columns.get(colId2ColIndex.get(outputColumnId)).getType();
            types.add(omniDataType);
            colName2ProjectId.put(columns.get(colId2ColIndex.get(outputColumnId)).getName(), projections.size());
            projections.add(new InputReferenceExpression(colId2ColIndex.get(outputColumnId), omniDataType));
            // count() return bigint
            decodeTypes.add(BIGINT.toString());
            decodeTypesWithAgg.add(false);
        } else {
            addProjectionsByVectorExpression(outputColumnId, false);
            // replace with bigint decode type
            decodeTypes.remove(decodeTypes.size() - 1);
            decodeTypes.add(BIGINT.toString());
        }
    }

    public boolean checkColumnAlreadyExists(int outputColumnId) {
        return colId2ColName.containsKey(outputColumnId);
    }

    /**
     * If the column contains one or more mathematical('+ - * / %') operators
     * and projection has been generated, clear the column information.
     *
     * @param columnId Hive output id
     */
    private void checkAndClearColumnInfo(int columnId) {
        if (checkColumnAlreadyExists(columnId) && !colId2ColIndex.containsKey(columnId)) {
            colName2ProjectId.remove("" + columnId);
            colId2ColName.remove(columnId);
        }
    }

    private void addConstantProjections(ConstantVectorExpression constantVectorExpression) {
        Type omniDataType = OmniDataUtils.transOmniDataType(constantVectorExpression.getOutputTypeInfo().getTypeName());
        types.add(omniDataType);
        // constantVectorExpressionParameters(): return "val " + value. need to remove "val "
        String constantValue = constantVectorExpression.vectorExpressionParameters().substring("val ".length());
        projections.add(OmniDataUtils.transOmniDataConstantExpr(constantValue, omniDataType));
        decodeTypes.add(omniDataType.toString());
        decodeTypesWithAgg.add(false);
    }

    /**
     * Agg Expressions with VectorExpression
     */
    private void addProjectionsByVectorExpression(int outputColumnId, boolean isAgg) {
        for (VectorExpression expression : expressions) {
            if (expression.getOutputColumnNum() == outputColumnId) {
                // check constant expression
                if (expression instanceof ConstantVectorExpression) {
                    addConstantProjections((ConstantVectorExpression) expression);
                } else {
                    Type omniDataType = OmniDataUtils.transOmniDataType(expression.getOutputTypeInfo().getTypeName());
                    types.add(omniDataType);
                    // String columnName = outputColumnId + "#SelExpression";
                    String columnName = "" + outputColumnId;
                    colName2ProjectId.put(columnName, projections.size());
                    colId2ColName.put(outputColumnId, columnName);
                    projections.add(createProjection(expression));
                    decodeTypes.add(omniDataType.toString());
                    decodeTypesWithAgg.add(isAgg);
                }
                break;
            }
        }
    }

    private RowExpression createProjection(VectorExpression expression) {
        Type returnType = OmniDataUtils.transOmniDataType(expression.getOutputTypeInfo().getTypeName());
        NdpArithmeticEnum operatorType = NdpArithmeticEnum.getArithmeticByClass(expression.getClass());
        FunctionHandle functionHandle = createFunctionHandle(expression, operatorType, returnType);
        if (functionHandle == null) {
            return null;
        }
        List<RowExpression> arguments = createArgument(expression);
        return new CallExpression(operatorType.name(), functionHandle, returnType, arguments, Optional.empty());
    }

    private List<RowExpression> createArgument(VectorExpression expression) {
        List<RowExpression> arguments = new ArrayList<>();
        VectorExpression[] childExpressions = expression.getChildExpressions();
        if (childExpressions == null || childExpressions.length == 1) {
            int childOutputId = (childExpressions == null ? -1 : childExpressions[0].getOutputColumnNum());
            createArgumentCore(expression, arguments, childOutputId);
        } else {
            for (VectorExpression childExpression : childExpressions) {
                arguments.add(createProjection(childExpression));
            }
        }
        return arguments;
    }

    private FunctionHandle createFunctionHandle(VectorExpression expression, NdpArithmeticEnum operatorType,
        Type returnType) {
        FunctionHandle functionHandle;
        String tmpType;
        switch (operatorType) {
            case UNSUPPORTED:
                isPushDown = false;
                LOG.error(operatorType.name() + " is not supported");
                return null;
            case CAST:
                tmpType = expression.getInputTypeInfos()[0].getTypeName();
                functionHandle = new BuiltInFunctionHandle(
                    new Signature(QualifiedObjectName.valueOfDefaultFunction("$operator$" + NdpArithmeticEnum.CAST),
                        SCALAR, returnType.getTypeSignature(),
                        OmniDataUtils.transOmniDataType(tmpType).getTypeSignature()));
                break;
            case CAST_IDENTITY:
                // CAST_IDENTITY is a special type of CAST and requires special processing.
                operatorType = NdpArithmeticEnum.CAST;
                tmpType = expression.getInputTypeInfos()[0].getTypeName();
                functionHandle = new BuiltInFunctionHandle(
                    new Signature(QualifiedObjectName.valueOfDefaultFunction("$operator$" + NdpArithmeticEnum.CAST),
                        SCALAR, OmniDataUtils.transAggType(returnType).getTypeSignature(),
                        OmniDataUtils.transOmniDataType(tmpType).getTypeSignature()));
                break;
            default:
                Type leftOmniDataType = OmniDataUtils.transOmniDataType(
                    expression.getInputTypeInfos()[0].getTypeName());
                Type rightOmniDataType = OmniDataUtils.transOmniDataType(
                    expression.getInputTypeInfos()[1].getTypeName());
                functionHandle = new BuiltInFunctionHandle(
                    new Signature(QualifiedObjectName.valueOfDefaultFunction("$operator$" + operatorType.name()),
                        SCALAR, returnType.getTypeSignature(), leftOmniDataType.getTypeSignature(),
                        rightOmniDataType.getTypeSignature()));
        }
        return functionHandle;
    }

    private void createArgumentCore(VectorExpression expression, List<RowExpression> arguments, int childOutputId) {
        NdpArithmeticExpressionInfo expressionInfo = new NdpArithmeticExpressionInfo(
            expression.vectorExpressionParameters());
        // i -> for
        for (int i = 0; i < expression.getInputTypeInfos().length; i++) {
            Type omniDataType = OmniDataUtils.transOmniDataType(expression.getInputTypeInfos()[i].getTypeName());
            if (expressionInfo.getIsVal()[i]) {
                arguments.add(OmniDataUtils.transOmniDataConstantExpr(expressionInfo.getColValue()[i], omniDataType));
            } else if (expressionInfo.getColId()[i] == childOutputId) {
                arguments.add(createProjection(expression.getChildExpressions()[0]));
            } else {
                if (expression.getClass().getName().contains("expressions.Cast")) {
                    isPushDown = false;
                    LOG.error("This kind of Cast is not supported.");
                    return;
                }
                arguments.add(
                    new InputReferenceExpression(colId2ColIndex.get(expressionInfo.getColId()[i]), omniDataType));
            }
        }
    }

}
