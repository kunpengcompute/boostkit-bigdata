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

package org.apache.hadoop.hive.ql.omnidata.operator.aggregation;

import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;

import com.huawei.boostkit.omnidata.model.AggregationInfo;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * OmniData Aggregation
 *
 * @since 2021-12-07
 */
public class OmniDataAggregation {

    private static final Logger LOG = LoggerFactory.getLogger(OmniDataAggregation.class);

    private OmniDataPredicate predicateInfo;

    private boolean isPushDownAgg = true;

    private Map<String, AggregationInfo.AggregateFunction> aggregationMap = new LinkedHashMap<>();

    public OmniDataAggregation(OmniDataPredicate predicateInfo) {
        this.predicateInfo = predicateInfo;
    }

    public AggregationInfo getAggregation(GroupByDesc groupByDesc) {
        VectorGroupByDesc aggVectorsDesc = (VectorGroupByDesc) groupByDesc.getVectorDesc();
        List<RowExpression> groupingKeys = new ArrayList<>();
        for (VectorExpression groupExpression : aggVectorsDesc.getKeyExpressions()) {
            createGroupingKey(groupExpression, groupingKeys);
        }
        for (VectorAggregationDesc aggregateFunction : aggVectorsDesc.getVecAggrDescs()) {
            createAggregateFunction(aggregateFunction);
        }
        return isPushDownAgg ? new AggregationInfo(aggregationMap, groupingKeys) : null;
    }

    private void createGroupingKey(VectorExpression groupExpression, List<RowExpression> groupingKeys) {
        if (IdentityExpression.isColumnOnly(groupExpression)) {
            IdentityExpression identityExpression = (IdentityExpression) groupExpression;
            int outputColumnId = identityExpression.getOutputColumnNum();
            if (!predicateInfo.addProjectionsByGroupByKey(outputColumnId)) {
                isPushDownAgg = false;
                LOG.info("Aggregation failed to push down, since outputColumnId is not exists");
                return;
            }
            int projectId = predicateInfo.getColName2ProjectId()
                    .get(predicateInfo.getColId2ColName().get(outputColumnId));
            Type omniDataType = OmniDataUtils.transOmniDataType(identityExpression.getOutputTypeInfo().getTypeName());
            groupingKeys.add(new InputReferenceExpression(projectId, omniDataType));
        } else {
            LOG.info("Aggregation failed to push down, since unsupported this [{}]", groupExpression.getClass());
            isPushDownAgg = false;
        }
    }

    private void createAggregateFunction(VectorAggregationDesc aggregateFunction) {
        String operatorName = aggregateFunction.getAggrDesc().getGenericUDAFName().toLowerCase(Locale.ENGLISH);
        switch (operatorName) {
            case "count":
                parseCountAggregation(aggregateFunction);
                break;
            case "max":
            case "min":
            case "sum":
                parseMaxMinSumAggregation(aggregateFunction);
                break;
            default:
                isPushDownAgg = false;
        }
    }

    private void parseCountAggregation(VectorAggregationDesc aggregateFunction) {
        List<RowExpression> arguments = new ArrayList<>();
        Signature signature;
        if (aggregateFunction.getInputExpression() != null) {
            // count(col_1)
            Type omniDataInputType = OmniDataUtils.transOmniDataType(
                    aggregateFunction.getInputTypeInfo().getTypeName());
            IdentityExpression identityExpression = (IdentityExpression) aggregateFunction.getInputExpression();
            int columnId = identityExpression.getOutputColumnNum();
            predicateInfo.addProjectionsByAggCount(columnId);
            int projectId = predicateInfo.getColName2ProjectId().get(predicateInfo.getColId2ColName().get(columnId));
            arguments.add(new InputReferenceExpression(projectId, predicateInfo.getTypes().get(projectId)));
            signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE,
                    BIGINT.getTypeSignature(), omniDataInputType.getTypeSignature());
        } else {
            // count(*)
            signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE,
                    BIGINT.getTypeSignature());
            predicateInfo.getDecodeTypes().add(BIGINT.toString());
            predicateInfo.getDecodeTypesWithAgg().add(false);
        }
        FunctionHandle functionHandle = new BuiltInFunctionHandle(signature);
        CallExpression callExpression = new CallExpression("count", functionHandle, BIGINT, arguments,
                Optional.empty());
        aggregationMap.put(String.format("%s_%s", "count", aggregationMap.size()),
                new AggregationInfo.AggregateFunction(callExpression, false));
    }

    private void parseMaxMinSumAggregation(VectorAggregationDesc aggregateFunction) {
        if (IdentityExpression.isColumnOnly(aggregateFunction.getInputExpression())) {
            String operatorName = aggregateFunction.getAggrDesc().getGenericUDAFName();
            IdentityExpression identityExpression = (IdentityExpression) aggregateFunction.getInputExpression();
            Type omniDataInputType = OmniDataUtils.transOmniDataType(
                    aggregateFunction.getInputTypeInfo().getTypeName());
            Type omniDataOutputType = OmniDataUtils.transOmniDataType(
                    aggregateFunction.getOutputTypeInfo().getTypeName());
            int columnId = identityExpression.getOutputColumnNum();
            predicateInfo.addProjectionsByAgg(columnId);
            if (!predicateInfo.isPushDown()) {
                isPushDownAgg = false;
                return;
            }
            int projectId = predicateInfo.getColName2ProjectId().get(predicateInfo.getColId2ColName().get(columnId));
            if (omniDataInputType != omniDataOutputType) {
                // The input and output types of the OmniData are the same.
                castAggOutputType(projectId, omniDataInputType, omniDataOutputType);
            }
            FunctionHandle functionHandle = new BuiltInFunctionHandle(
                    new Signature(QualifiedObjectName.valueOfDefaultFunction(operatorName), AGGREGATE,
                            omniDataOutputType.getTypeSignature(), omniDataOutputType.getTypeSignature()));
            List<RowExpression> arguments = new ArrayList<>();
            arguments.add(new InputReferenceExpression(projectId, predicateInfo.getTypes().get(projectId)));
            CallExpression callExpression = new CallExpression(operatorName, functionHandle, omniDataOutputType,
                    arguments, Optional.empty());
            aggregationMap.put(String.format("%s_%s", operatorName, projectId),
                    new AggregationInfo.AggregateFunction(callExpression, false));
        } else {
            LOG.info("Aggregation failed to push down, since unsupported this [{}]",
                    aggregateFunction.getInputExpression().getClass());
            isPushDownAgg = false;
        }
    }

    private void castAggOutputType(int projectId, Type omniDataInputType, Type omniDataOutputType) {
        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(predicateInfo.getProjections().remove(projectId));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.CAST.getOperatorName()), FunctionKind.SCALAR,
                omniDataOutputType.getTypeSignature(), omniDataInputType.getTypeSignature());
        predicateInfo.getProjections()
                .add(projectId, new CallExpression(NdpUdfEnum.CAST.getSignatureName(), new BuiltInFunctionHandle(signature),
                        omniDataOutputType, rowArguments, Optional.empty()));
        predicateInfo.getTypes().set(projectId, omniDataOutputType);
        predicateInfo.getDecodeTypes().set(projectId, omniDataOutputType.toString());
        predicateInfo.getDecodeTypesWithAgg().set(projectId, true);
    }
}