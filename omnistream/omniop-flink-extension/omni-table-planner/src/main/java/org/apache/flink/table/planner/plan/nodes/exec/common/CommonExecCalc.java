/*
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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for exec Calc.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    /**
     * FIELD_NAME_PROJECTION
     */
    public static final String FIELD_NAME_PROJECTION = "projection";

    /**
     * CALC_TRANSFORMATION
     */
    public static final String CALC_TRANSFORMATION = "calc";

    /**
     * FIELD_NAME_CONDITION
     */
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    @Nullable
    private final RexNode condition;

    @JsonIgnore
    private final Class<?> operatorBaseClass;
    @JsonIgnore
    private final boolean retainHeader;

    protected CommonExecCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable
            RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    private String getExtraDescription(String oldDescription, Transformation<RowData> inputTransform) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        HashMap<Integer, Integer> accessIndexMap = new HashMap<>();
        HashMap<Integer, Integer> fieldCountMap = new HashMap<>();

        //get inputType info
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();
        int currentIndex = 0;
        for (int oldIndex = 0; oldIndex < inputFields.size(); oldIndex++) {
            RowType.RowField field = inputFields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // save the offset and subfield cnt and expand row
                accessIndexMap.put(oldIndex, currentIndex);
                fieldCountMap.put(oldIndex, subFields.size());
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
                currentIndex += subFields.size();
            } else {
                inputTypeList.add(DescriptionUtil.getFieldType(fieldType));
                accessIndexMap.put(oldIndex, currentIndex);
                currentIndex++;
            }
        }

        //get outputTypes info
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (int oldIndex = 0; oldIndex < fields.size(); oldIndex++) {
            RowType.RowField field = fields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // expand row
                for (RowType.RowField rowField : subFields) {
                    outputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
            } else {
                outputTypeList.add(DescriptionUtil.getFieldType(fieldType));
            }
        }
        // Set the accessIndexMap
        RexNodeUtil.accessIndexMap = accessIndexMap;
        //get output indices
        List<Map<String, Object>> indicesList = new ArrayList<>();
        for (RexNode rexNode : projection) {
            // A field reference to a row
            if (rexNode.getKind() == SqlKind.INPUT_REF
                    && rexNode.getType().getSqlTypeName() == SqlTypeName.ROW) {
                int oldIndex = ((RexInputRef) rexNode).hashCode(); // colVal
                int count = fieldCountMap.get(oldIndex);
                int offset = accessIndexMap.get(oldIndex);
                // expand it to multiple fieldRef
                for (int i = 0; i < count; i++) {
                    RexInputRef fieldRef = RexInputRef.of(i, rexNode.getType());
                    Map<String, Object> fieldRefMap = RexNodeUtil.buildJsonMap(fieldRef);
                    // int colValNew = (int)(fieldRefMap.get("colval")) + offset;
                    fieldRefMap.put("colVal", offset + i);
                    indicesList.add(fieldRefMap);
                }
            } else {
                indicesList.add(RexNodeUtil.buildJsonMap(rexNode));
            }
        }
        //get condition info
        Map<String, Object> conditionMap = null;
        if (condition != null) {
            conditionMap = RexNodeUtil.buildJsonMap(condition);
        }
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("indices", indicesList);
        jsonMap.put("condition", conditionMap);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e); // Handle the exception or log it
        }
        RexNodeUtil.accessIndexMap.clear();
        return jsonString;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        inputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(projection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(this.condition)),
                        retainHeader,
                        getClass().getSimpleName());
        Transformation<RowData> transformation = ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(CALC_TRANSFORMATION, config),
                substituteStreamOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
        String oldDescription = transformation.getDescription();
        transformation.setDescription(getExtraDescription(oldDescription, inputTransform));
        return transformation;
    }
}
