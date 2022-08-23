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

package org.apache.hadoop.hive.ql.omnidata.operator.filter;

import static org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpHiveOperatorEnum.NOT;

import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpHiveOperatorEnum;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Ndp Filter
 * <p>
 * First, need to determine whether to perform partial push down. Currently,
 * Second, to create a binary tree(NdpFilterBinaryTree.class) and convert Hive to OmniData.
 *
 * @since 2022-02-22
 */
public class NdpFilter {
    private static final Logger LOG = LoggerFactory.getLogger(NdpFilter.class);

    private NdpFilterBinaryTree binaryTreeHead;

    private final List<NdpFilterLeaf> ndpFilterLeafList = new ArrayList<>();

    private ExprNodeDesc pushDownFilterDesc;

    private ExprNodeDesc unPushDownFilterDesc;

    private final List<ExprNodeDesc> unsupportedFilterDescList = new ArrayList<>();

    private final List<ExprNodeDesc> supportedFilterDescList = new ArrayList<>();

    private boolean isExistsOr = false;

    private NdpFilterMode mode;

    public NdpFilter(ExprNodeGenericFuncDesc rawFuncDesc) {
        // filter push down entrance
        parseFilterOperator(rawFuncDesc);
        // !isExistsOr: the 'OR' operator does not support partial push down
        if (unsupportedFilterDescList.size() > 0 && supportedFilterDescList.size() > 0 && !isExistsOr) {
            mode = NdpFilterMode.PART;
        } else if (unsupportedFilterDescList.size() == 0 && supportedFilterDescList.size() > 0) {
            // If no unsupported filter exists, push down all filter
            mode = NdpFilterMode.ALL;
            return;
        } else {
            mode = NdpFilterMode.NONE;
            return;
        }
        // start to part push down
        // create un push down
        if (checkDynamicValue(unsupportedFilterDescList)) {
            this.unPushDownFilterDesc = createNewFuncDesc(rawFuncDesc, unsupportedFilterDescList);
        } else {
            mode = NdpFilterMode.NONE;
            return;
        }
        // create part push down
        this.pushDownFilterDesc = createNewFuncDesc(rawFuncDesc, supportedFilterDescList);
    }

    public NdpFilterBinaryTree getBinaryTreeHead() {
        return binaryTreeHead;
    }

    public List<NdpFilterLeaf> getNdpFilterLeafList() {
        return ndpFilterLeafList;
    }

    public ExprNodeDesc getPushDownFuncDesc() {
        return pushDownFilterDesc;
    }

    public ExprNodeDesc getUnPushDownFuncDesc() {
        return unPushDownFilterDesc;
    }

    /**
     * get filter push down mode
     *
     * @return NdpFilterMode
     */
    public NdpFilterMode getMode() {
        if (mode == null) {
            return NdpFilterMode.NONE;
        } else {
            return mode;
        }
    }

    /**
     * manually setting the push-down mode
     *
     * @param mode
     */
    public void setMode(NdpFilterMode mode) {
        this.mode = mode;
    }

    /**
     * Converting Hive Operators to FilterBinaryTree
     *
     * @param funcDesc Hive ExprNodeGenericFuncDesc
     */
    public void createNdpFilterBinaryTree(ExprNodeGenericFuncDesc funcDesc) {
        binaryTreeHead = new NdpFilterBinaryTree(funcDesc, ndpFilterLeafList);
    }

    private boolean checkDynamicValue(List<ExprNodeDesc> exprNodeDescList) {
        for (ExprNodeDesc desc : exprNodeDescList) {
            for (ExprNodeDesc child : desc.getChildren()) {
                if (child instanceof ExprNodeDynamicValueDesc || child instanceof ExprNodeDynamicListDesc) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Todo: need to optimization
     *
     * @param oldFuncDesc oldFuncDesc
     * @param newChildren newChildren
     * @return new ExprNodeGenericFuncDesc
     */
    public static ExprNodeDesc createNewFuncDesc(ExprNodeGenericFuncDesc oldFuncDesc, List<ExprNodeDesc> newChildren) {
        if (newChildren.size() == 1) {
            return newChildren.get(0);
        } else {
            return new ExprNodeGenericFuncDesc(oldFuncDesc.getTypeInfo(), oldFuncDesc.getGenericUDF(),
                    oldFuncDesc.getFuncText(), newChildren);
        }
    }

    private void parseFilterOperator(ExprNodeGenericFuncDesc funcDesc) {
        NdpHiveOperatorEnum operator = NdpHiveOperatorEnum.getNdpHiveOperator(funcDesc);
        switch (operator) {
            case AND:
                parseAndOrOperator(funcDesc);
                break;
            case OR:
                isExistsOr = true;
                parseAndOrOperator(funcDesc);
                break;
            case NOT:
                // Not is only one child
                parseNotOperator(funcDesc);
                break;
            case UNSUPPORTED:
                // operator unsupported
                LOG.info("OmniData Hive Filter do not to all push down, since unsupported this Operator class: [{}]",
                        funcDesc.getGenericUDF().getClass().getSimpleName());
                unsupportedFilterDescList.add(funcDesc);
                break;
            default:
                if (checkUdf(funcDesc, operator)) {
                    supportedFilterDescList.add(funcDesc);
                } else {
                    // udf unsupported
                    unsupportedFilterDescList.add(funcDesc);
                }
        }
    }

    private void parseAndOrOperator(ExprNodeGenericFuncDesc funcDesc) {
        List<ExprNodeDesc> children = funcDesc.getChildren();
        for (int i = 0; i < children.size(); i++) {
            ExprNodeDesc child = children.get(i);
            if (child instanceof ExprNodeGenericFuncDesc) {
                parseFilterOperator((ExprNodeGenericFuncDesc) child);
            } else {
                if (checkBooleanColumn(child)) {
                    ExprNodeDesc newBooleanChild = transBooleanColumnExpression((ExprNodeColumnDesc) child.clone());
                    children.set(i, newBooleanChild);
                    supportedFilterDescList.add(newBooleanChild);
                } else {
                    unsupportedFilterDescList.add(child);
                }
            }
        }
    }

    private void parseNotOperator(ExprNodeGenericFuncDesc notFuncDesc) {
        // Not is only one child
        if (notFuncDesc.getChildren().get(0) instanceof ExprNodeGenericFuncDesc) {
            ExprNodeGenericFuncDesc notChild = (ExprNodeGenericFuncDesc) notFuncDesc.getChildren().get(0);
            NdpHiveOperatorEnum operator = NdpHiveOperatorEnum.getNdpHiveOperator(notChild);
            if (NdpHiveOperatorEnum.checkNotSupportedOperator(operator)) {
                if (checkUdf(notChild, NOT)) {
                    supportedFilterDescList.add(notFuncDesc);
                    return;
                }
            }
        }
        // udf unsupported
        unsupportedFilterDescList.add(notFuncDesc);
    }

    /**
     * Convert 'Column[o_boolean]' to 'GenericUDFOPEqual(Column[o_boolean], Const boolean true)'
     *
     * @param booleanDesc Column[o_boolean]
     * @return GenericUDFOPEqual(Column[o_boolean], Const boolean true)
     */
    private ExprNodeDesc transBooleanColumnExpression(ExprNodeColumnDesc booleanDesc) {
        TypeInfo typeInfo = booleanDesc.getTypeInfo();
        String funcText = "=";
        ExprNodeConstantDesc constantDesc = new ExprNodeConstantDesc(typeInfo, true);
        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(booleanDesc);
        children.add(constantDesc);
        return new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqual(), funcText, children);
    }

    /**
     * In Hive, there are two expressions for 'o_boolean=true':
     * 1.GenericUDFOPEqual(Column[o_boolean], Const boolean true)
     * 2.Column[o_boolean]
     *
     * @param exprNodeDesc expressions
     * @return true or false
     */
    private boolean checkBooleanColumn(ExprNodeDesc exprNodeDesc) {
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) exprNodeDesc;
            return "boolean".equals(columnDesc.getTypeInfo().getTypeName());
        }
        return false;
    }

    private boolean checkUdf(ExprNodeGenericFuncDesc funcDesc, NdpHiveOperatorEnum operator) {
        int argumentIndex = 0;
        if (operator.equals(NdpHiveOperatorEnum.BETWEEN)) {
            // Between not support ExprNodeDynamicValueDesc
            if (!checkBetween(funcDesc)) {
                return false;
            }
            // first argument for BETWEEN should be boolean type
            argumentIndex = 1;
        }
        ExprNodeDesc udfFuncDesc = funcDesc;
        while (udfFuncDesc.getChildren() != null) {
            udfFuncDesc = udfFuncDesc.getChildren().get(argumentIndex);
            argumentIndex = 0;
            if (udfFuncDesc instanceof ExprNodeGenericFuncDesc) {
                // check whether the UDF supports push down
                if (!NdpUdfEnum.checkUdfSupported(((ExprNodeGenericFuncDesc) udfFuncDesc).getGenericUDF())) {
                    LOG.info("OmniData Hive Filter failed to all push down, since unsupported this UDF class: [{}]",
                            ((ExprNodeGenericFuncDesc) udfFuncDesc).getGenericUDF());
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkBetween(ExprNodeGenericFuncDesc funcDesc) {
        return funcDesc.getChildren().get(2) instanceof ExprNodeConstantDesc && funcDesc.getChildren()
                .get(3) instanceof ExprNodeConstantDesc;
    }

    /**
     * Converting Hive Operators to Omnidata Operators
     * <p>
     * For example sql: 'select a,b,c from table1 where a=1 and b=2 and c=3;' .as follows:
     * Hive Operators:
     * And
     * /    |   \
     * a=1   b=2  c=3
     * ||
     * V
     * OmniData Operators:
     * And
     * /    |
     * a=1  And
     * |    \
     * b=2  c=3
     *
     * @since 2021-11-23
     */
    public static class NdpFilterBinaryTree {

        private final Logger LOG = LoggerFactory.getLogger(NdpFilterBinaryTree.class);

        private NdpOperator ndpOperator;

        private NdpLeafOperator ndpLeafOperator;

        private NdpFilterBinaryTree leftChild;

        private NdpFilterBinaryTree rightChild;

        private int leaf = -1;

        public NdpFilterBinaryTree(NdpOperator ndpOperator, NdpLeafOperator ndpLeafOperator, int leaf) {
            this.ndpOperator = ndpOperator;
            this.ndpLeafOperator = ndpLeafOperator;
            this.leaf = leaf;
        }

        private NdpFilterBinaryTree(NdpOperator ndpOperator, List<ExprNodeGenericFuncDesc> exprNodes,
                                    List<NdpFilterLeaf> ndpFilterLeafList) {
            if (exprNodes.size() >= 2) {
                this.ndpOperator = ndpOperator;
                leftChild = new NdpFilterBinaryTree(exprNodes.get(0), ndpFilterLeafList);
                if (exprNodes.size() == 2) {
                    rightChild = new NdpFilterBinaryTree(exprNodes.get(1), ndpFilterLeafList);
                } else {
                    exprNodes.remove(0);
                    rightChild = new NdpFilterBinaryTree(ndpOperator, exprNodes, ndpFilterLeafList);
                }
            } else {
                LOG.info("OmniData Hive Filter failed to push down, since unsupported exprNodes.size() < 2");
                setNdpLeafOperatorUnsupported();
            }
        }

        public NdpFilterBinaryTree(ExprNodeGenericFuncDesc exprNode, List<NdpFilterLeaf> ndpFilterLeafList) {
            parseFilterOperator(exprNode);
            if (ndpOperator.equals(NdpOperator.AND) || ndpOperator.equals(NdpOperator.OR)) {
                // and,or
                parseAndOrOperator(exprNode, ndpFilterLeafList);
            } else if (ndpOperator.equals(NdpOperator.NOT)) {
                // not
                parseNotOperator(exprNode, ndpFilterLeafList);
            } else if (ndpOperator.equals(NdpOperator.LEAF)) {
                // leaf
                parseLeafOperator(exprNode, ndpFilterLeafList);
            }
        }

        public NdpOperator getNdpOperator() {
            return ndpOperator;
        }

        public int getLeaf() {
            return leaf;
        }

        public NdpFilterBinaryTree getLeftChild() {
            return leftChild;
        }

        public NdpFilterBinaryTree getRightChild() {
            return rightChild;
        }

        private void parseAndOrOperator(ExprNodeGenericFuncDesc exprNode, List<NdpFilterLeaf> ndpFilterLeafList) {
            ExprNodeDesc leftExpr = exprNode.getChildren().remove(0);
            if (leftExpr instanceof ExprNodeGenericFuncDesc) {
                // process left filter
                leftChild = new NdpFilterBinaryTree((ExprNodeGenericFuncDesc) leftExpr, ndpFilterLeafList);
                List<ExprNodeGenericFuncDesc> restChildren = new ArrayList<>();
                for (ExprNodeDesc expr : exprNode.getChildren()) {
                    if (expr instanceof ExprNodeGenericFuncDesc) {
                        restChildren.add((ExprNodeGenericFuncDesc) expr);
                    } else {
                        LOG.info(
                                "OmniData Hive Filter failed to push down, since Method parseAndOrOperator() unsupported this [{}]",
                                expr.getClass());
                        setNdpLeafOperatorUnsupported();
                    }
                }
                // process right filter
                if (restChildren.size() == 1) {
                    rightChild = new NdpFilterBinaryTree(restChildren.get(0), ndpFilterLeafList);
                } else {
                    rightChild = new NdpFilterBinaryTree(ndpOperator, restChildren, ndpFilterLeafList);
                }
            } else {
                LOG.info(
                        "OmniData Hive Filter failed to push down, since Method parseAndOrOperator() unsupported this [{}]",
                        leftExpr.getClass());
                setNdpLeafOperatorUnsupported();
            }
        }

        private void parseNotOperator(ExprNodeGenericFuncDesc exprNode, List<NdpFilterLeaf> ndpFilterLeafList) {
            if (ndpLeafOperator == null) {
                // GenericUDFOPNot
                ExprNodeDesc expr = exprNode.getChildren().get(0);
                if (expr instanceof ExprNodeGenericFuncDesc) {
                    leftChild = new NdpFilterBinaryTree((ExprNodeGenericFuncDesc) expr, ndpFilterLeafList);
                } else {
                    LOG.info(
                            "OmniData Hive Filter failed to push down, since Method parseNotOperator() unsupported this [{}]",
                            expr.getClass());
                    setNdpLeafOperatorUnsupported();
                }
            } else {
                // EQUAL IS_NULL LEAF
                leftChild = new NdpFilterBinaryTree(NdpOperator.LEAF, ndpLeafOperator, ndpFilterLeafList.size());
                NdpFilterLeaf ndpFilterLeaf = new NdpFilterLeaf(ndpLeafOperator);
                ndpFilterLeaf.parseExprLeaf(exprNode);
                ndpFilterLeafList.add(ndpFilterLeaf);
            }
        }

        private void parseLeafOperator(ExprNodeGenericFuncDesc exprNode, List<NdpFilterLeaf> ndpFilterLeafList) {
            leaf = ndpFilterLeafList.size();
            NdpFilterLeaf ndpFilterLeaf = new NdpFilterLeaf(ndpLeafOperator);
            ndpFilterLeaf.parseExprLeaf(exprNode);
            ndpFilterLeafList.add(ndpFilterLeaf);
        }

        private void setNdpLeafOperatorUnsupported() {
            ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
        }

        public void parseFilterOperator(ExprNodeGenericFuncDesc exprNode) {
            Class operator = (exprNode.getGenericUDF() instanceof GenericUDFBridge)
                    ? ((GenericUDFBridge) exprNode.getGenericUDF()).getUdfClass()
                    : exprNode.getGenericUDF().getClass();
            if (operator == GenericUDFOPAnd.class) {
                ndpOperator = NdpOperator.AND;
            } else if (operator == GenericUDFOPOr.class) {
                ndpOperator = NdpOperator.OR;
            } else if (operator == GenericUDFOPNot.class) {
                ndpOperator = NdpOperator.NOT;
            } else if (operator == GenericUDFOPNotEqual.class) {
                ndpOperator = NdpOperator.NOT;
                ndpLeafOperator = NdpLeafOperator.EQUAL;
            } else if (operator == GenericUDFOPEqual.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.EQUAL;
            } else if (operator == GenericUDFOPNotNull.class) {
                ndpOperator = NdpOperator.NOT;
                ndpLeafOperator = NdpLeafOperator.IS_NULL;
            } else if (operator == GenericUDFOPNull.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.IS_NULL;
            } else if (operator == GenericUDFOPGreaterThan.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.GREATER_THAN;
            } else if (operator == GenericUDFOPLessThan.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.LESS_THAN;
            } else if (operator == GenericUDFOPEqualOrGreaterThan.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.GREATER_THAN_OR_EQUAL;
            } else if (operator == GenericUDFOPEqualOrLessThan.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.LESS_THAN_OR_EQUAL;
            } else if (operator == GenericUDFBetween.class) {
                ndpOperator = (Boolean) ((ExprNodeConstantDesc) exprNode.getChildren().get(0)).getValue()
                        ? NdpOperator.NOT
                        : NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.BETWEEN;
            } else if (operator == GenericUDFIn.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.IN;
            } else if (operator == UDFLike.class) {
                ndpOperator = NdpOperator.LEAF;
                ndpLeafOperator = NdpLeafOperator.LIKE;
            } else {
                ndpOperator = NdpOperator.LEAF;
                LOG.info(
                        "OmniData Hive Filter failed to push down, since Method parseFilterOperator() unsupported this [{}]",
                        operator);
                setNdpLeafOperatorUnsupported();
            }
        }
    }

    public enum NdpFilterMode {
        /**
         * All filter push down
         */
        ALL,
        /**
         * part filter push down
         */
        PART,
        /**
         * no filter push down
         */
        NONE
    }

    public enum NdpOperator {
        OR,
        AND,
        NOT,
        LEAF
    }

    public enum NdpLeafOperator {
        BETWEEN,
        IN,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN_OR_EQUAL,
        EQUAL,
        LIKE,
        IS_NULL,
        UNSUPPORTED
    }

}