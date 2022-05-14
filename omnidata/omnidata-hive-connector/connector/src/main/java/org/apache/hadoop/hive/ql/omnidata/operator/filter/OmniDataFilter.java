package org.apache.hadoop.hive.ql.omnidata.operator.filter;

import org.apache.hadoop.hive.ql.omnidata.operator.filter.NdpFilter.*;

import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

import com.google.common.collect.ImmutableList;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.Type;

import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.omnidata.physical.NdpPlanChecker;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Filter Expression
 *
 * @since 2021-11-15
 */
public class OmniDataFilter {

    private static final Logger LOG = LoggerFactory.getLogger(OmniDataFilter.class);

    private List<NdpFilterLeaf> ndpFilterLeafList;

    private OmniDataPredicate predicateInfo;

    private boolean isPushDownFilter = true;

    public OmniDataFilter(OmniDataPredicate predicateInfo) {
        this.predicateInfo = predicateInfo;
    }

    public RowExpression getFilterExpression(ExprNodeGenericFuncDesc funcDesc, NdpFilter ndpFilter) {
        ndpFilter.createNdpFilterBinaryTree(funcDesc);
        this.ndpFilterLeafList = ndpFilter.getNdpFilterLeafList();
        RowExpression filterRowExpression = extractExpressionTree(ndpFilter.getBinaryTreeHead());
        return isPushDownFilter ? filterRowExpression : null;
    }

    private List<RowExpression> getArguments(NdpFilterBinaryTree expressionTree) {
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(extractExpressionTree(expressionTree.getLeftChild()));
        if (expressionTree.getRightChild() != null) {
            arguments.add(extractExpressionTree(expressionTree.getRightChild()));
        }
        return arguments;
    }

    private RowExpression extractExpressionTree(NdpFilterBinaryTree expressionTree) {
        if (expressionTree == null) {
            return null;
        }
        switch (expressionTree.getNdpOperator()) {
            case AND:
                return new SpecialForm(SpecialForm.Form.AND, BOOLEAN, getArguments(expressionTree));
            case OR:
                return new SpecialForm(SpecialForm.Form.OR, BOOLEAN, getArguments(expressionTree));
            case NOT:
                Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("not"),
                        FunctionKind.SCALAR, BOOLEAN.getTypeSignature(), BOOLEAN.getTypeSignature());
                return new CallExpression("not", new BuiltInFunctionHandle(signature), BOOLEAN,
                        getArguments(expressionTree), Optional.empty());
            case LEAF:
                return getLeafRowExpression(expressionTree);
            default:
                return null;
        }
    }

    private RowExpression getLeafRowExpression(NdpFilterBinaryTree expressionTree) {
        int leaf = expressionTree.getLeaf();
        if (!(leaf >= 0 && leaf < ndpFilterLeafList.size())) {
            LOG.error("OmniData Hive Filter failed to push down, leaf operator out of Index");
            isPushDownFilter = false;
            return null;
        }
        NdpFilterLeaf ndpFilterLeaf = ndpFilterLeafList.get(leaf);
        NdpLeafOperator leafOperator = ndpFilterLeaf.getNdpLeafOperator();
        // leaf operator needs white list verification
        if (!NdpPlanChecker.checkLeafOperatorByWhiteList(leafOperator)) {
            isPushDownFilter = false;
            return null;
        }
        switch (leafOperator) {
            case EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return getCompareExpression(ndpFilterLeaf);
            case BETWEEN:
                return getBetweenExpression(ndpFilterLeaf);
            case IN:
                return getInExpression(ndpFilterLeaf);
            case IS_NULL:
                return getIsNullExpression(ndpFilterLeaf);
            case LIKE:
                return getLikeExpression(ndpFilterLeaf);
            default:
                isPushDownFilter = false;
                return null;
        }
    }

    private RowExpression getBetweenExpression(NdpFilterLeaf ndpFilterLeaf) {
        List<RowExpression> betweenArguments = new ArrayList<>();
        // Only the left expression exists BETWEEN.
        NdpFilterLeaf.NdpFilterFunc leftFilterFunc = ndpFilterLeaf.getLeftFilterFunc();
        if (leftFilterFunc.isExistsUdf()) {
            addUdfExpression(leftFilterFunc, betweenArguments);
        } else {
            addColumnExpression(leftFilterFunc, betweenArguments);
        }
        addConstantExpression(leftFilterFunc, ndpFilterLeaf.getConstantList(), betweenArguments, -1);
        return new SpecialForm(SpecialForm.Form.BETWEEN, BOOLEAN, betweenArguments);
    }

    private RowExpression getInExpression(NdpFilterLeaf ndpFilterLeaf) {
        int charLength = -1;
        List<RowExpression> inArguments = new ArrayList<>();
        // Only the left expression exists IN.
        NdpFilterLeaf.NdpFilterFunc leftFilterFunc = ndpFilterLeaf.getLeftFilterFunc();
        if (leftFilterFunc.isExistsUdf()) {
            addUdfExpression(leftFilterFunc, inArguments);
            charLength = getCharLength(leftFilterFunc.getUdfExpression());
        } else {
            addColumnExpression(leftFilterFunc, inArguments);
        }
        addConstantExpression(leftFilterFunc, ndpFilterLeaf.getConstantList(), inArguments, charLength);
        return new SpecialForm(SpecialForm.Form.IN, BOOLEAN, inArguments);
    }

    /**
     * The Hive char type needs to be processed in the 'in' operator before being converted to the OmniData Server.
     *
     * @param exprNodeDesc Udf Expression
     * @return char length
     */
    private int getCharLength(ExprNodeDesc exprNodeDesc) {
        int charLength = -1;
        if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) exprNodeDesc;
            GenericUDF genericUDF = funcDesc.getGenericUDF();
            Class udfClass = (genericUDF instanceof GenericUDFBridge)
                    ? ((GenericUDFBridge) genericUDF).getUdfClass()
                    : genericUDF.getClass();
            if (udfClass == UDFToString.class && funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc) {
                ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) funcDesc.getChildren().get(0);
                if (columnDesc.getTypeInfo() instanceof CharTypeInfo) {
                    charLength = ((CharTypeInfo) columnDesc.getTypeInfo()).getLength();
                }
            }
        }
        return charLength;
    }

    private RowExpression getIsNullExpression(NdpFilterLeaf ndpFilterLeaf) {
        Type omniDataType = OmniDataUtils.transOmniDataType(ndpFilterLeaf.getLeftFilterFunc().getReturnType());
        List<RowExpression> isNullArguments = new ArrayList<>();
        isNullArguments.add(new InputReferenceExpression(
                predicateInfo.getColName2ColIndex().get(ndpFilterLeaf.getLeftFilterFunc().getColumnName()), omniDataType));
        return new SpecialForm(SpecialForm.Form.IS_NULL, BOOLEAN, isNullArguments);
    }

    private RowExpression getLikeExpression(NdpFilterLeaf ndpFilterLeaf) {
        Type omniDataType = OmniDataUtils.transOmniDataType(ndpFilterLeaf.getLeftFilterFunc().getReturnType());
        Signature signatureCast = internalOperator(OperatorType.CAST, LIKE_PATTERN, ImmutableList.of(VARCHAR));
        List<RowExpression> castArguments = new ArrayList<>();
        castArguments.add(
                OmniDataUtils.transOmniDataConstantExpr(ndpFilterLeaf.getConstantList().get(0).toString(), omniDataType));
        CallExpression castCallExpression = new CallExpression("CAST", new BuiltInFunctionHandle(signatureCast),
                LIKE_PATTERN, castArguments);
        Signature signatureLike = new Signature(QualifiedObjectName.valueOfDefaultFunction("LIKE"), FunctionKind.SCALAR,
                BOOLEAN.getTypeSignature(), omniDataType.getTypeSignature(), LIKE_PATTERN.getTypeSignature());
        List<RowExpression> likeArguments = new ArrayList<>();
        likeArguments.add(new InputReferenceExpression(
                predicateInfo.getColName2ColIndex().get(ndpFilterLeaf.getLeftFilterFunc().getColumnName()), omniDataType));
        likeArguments.add(castCallExpression);
        return new CallExpression("LIKE", new BuiltInFunctionHandle(signatureLike), BOOLEAN, likeArguments);
    }

    private RowExpression getCompareExpression(NdpFilterLeaf ndpFilterLeaf) {
        int charLength = -1;
        List<RowExpression> compareArguments = new ArrayList<>();
        NdpFilterLeaf.NdpFilterFunc leftFilterFunc = ndpFilterLeaf.getLeftFilterFunc();
        // On the left is column or UDF expression.
        if (leftFilterFunc.isExistsUdf()) {
            addUdfExpression(leftFilterFunc, compareArguments);
            charLength = getCharLength(leftFilterFunc.getUdfExpression());
        } else {
            addColumnExpression(leftFilterFunc, compareArguments);
        }
        // Whether there is a column or UDF expression on the right
        if (ndpFilterLeaf.getRightFilterFunc() != null) {
            NdpFilterLeaf.NdpFilterFunc rightFilterFunc = ndpFilterLeaf.getRightFilterFunc();
            if (rightFilterFunc.isExistsUdf()) {
                addUdfExpression(rightFilterFunc, compareArguments);
            } else {
                addColumnExpression(rightFilterFunc, compareArguments);
            }
        } else {
            // On the right is a constant expression
            addConstantExpression(leftFilterFunc, ndpFilterLeaf.getConstantList(), compareArguments, charLength);
        }
        Type omniDataType = OmniDataUtils.transOmniDataType(leftFilterFunc.getReturnType());
        String signatureName = OmniDataUtils.transOmniDataOperator(ndpFilterLeaf.getNdpLeafOperator().name());
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction("$operator$" + signatureName.toLowerCase(Locale.ENGLISH)),
                FunctionKind.SCALAR, BOOLEAN.getTypeSignature(), omniDataType.getTypeSignature(),
                omniDataType.getTypeSignature());
        return new CallExpression(signatureName, new BuiltInFunctionHandle(signature), BOOLEAN, compareArguments);
    }

    private void addColumnExpression(NdpFilterLeaf.NdpFilterFunc ndpFilterFunc, List<RowExpression> arguments) {
        Type omniDataType = OmniDataUtils.transOmniDataType(ndpFilterFunc.getReturnType());
        arguments.add(
                new InputReferenceExpression(predicateInfo.getColName2ColIndex().get(ndpFilterFunc.getColumnName()),
                        omniDataType));
    }

    private void addUdfExpression(NdpFilterLeaf.NdpFilterFunc ndpFilterFunc, List<RowExpression> arguments) {
        NdpUdfExpression ndpUdfExpression = new NdpUdfExpression(predicateInfo);
        ndpUdfExpression.createNdpUdf(ndpFilterFunc.getUdfExpression(), arguments);
        if (!ndpUdfExpression.isPushDownUdf()) {
            isPushDownFilter = false;
        }
    }

    private void addConstantExpression(NdpFilterLeaf.NdpFilterFunc ndpFilterFunc, List<Object> constantList,
                                       List<RowExpression> arguments, int charLength) {
        Type omniDataType = OmniDataUtils.transOmniDataType(ndpFilterFunc.getReturnType());
        constantList.forEach(constant -> {
            if (constant == null) {
                // Handle the case : where a = null
                arguments.add(new ConstantExpression(null, omniDataType));
            } else {
                if (charLength < 0) {
                    arguments.add(OmniDataUtils.transOmniDataConstantExpr(constant.toString(), omniDataType));
                } else {
                    // for example: char in ('aa','bb')
                    arguments.add(OmniDataUtils.transOmniDataConstantExpr(
                            String.format("%-" + charLength + "s", (constant.toString())), omniDataType));
                }
            }
        });
    }

}