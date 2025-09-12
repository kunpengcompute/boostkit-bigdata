package org.apache.flink.table.planner.plan.nodes.exec.common;

import static org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil.getFieldType;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExpandCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CommonExecExpand
 *
 * @since 2025-04-27
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommonExecExpand extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {
    /**
     * FIELD_NAME_PROJECTS
     */
    public static final String FIELD_NAME_PROJECTS = "projects";

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecExpand.class);

    @JsonProperty(FIELD_NAME_PROJECTS)
    private final List<List<RexNode>> projects;

    @JsonIgnore
    private final boolean retainHeader;

    public CommonExecExpand(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<List<RexNode>> projects,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projects = checkNotNull(projects);
        checkArgument(
                projects.size() > 0
                        && projects.get(0).size() > 0
                        && projects.stream().map(List::size).distinct().count() == 1);
        this.retainHeader = retainHeader;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        checkState(inputEdge.getOutputType() instanceof RowType);
        checkState(getOutputType() instanceof RowType);
        final CodeGenOperatorFactory<RowData> operatorFactory =
                ExpandCodeGenerator.generateExpandOperator(
                        new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader()),
                        (RowType) inputEdge.getOutputType(),
                        (RowType) getOutputType(),
                        projects,
                        retainHeader,
                        getClass().getSimpleName());
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                inputTransform,
                getDescription(),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism());
        String oldDescription = transformation.getDescription();
        transformation.setDescription(getExtraDescription(oldDescription, inputTransform));
        return transformation;
    }

    private String getExtraDescription(String oldDescription, Transformation<RowData> inputTransform) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        //get inputType info
        List<String> inputTypeList = new ArrayList<>();

        List<RowType.RowField> inputFields = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();

        HashMap<Integer, Integer> accessIndexMap = new HashMap<>();
        HashMap<Integer, Integer> fieldCountMap = new HashMap<>();

        int currentIndex = 0;
        for (int oldIndex = 0; oldIndex < inputFields.size(); oldIndex++) {
            RowType.RowField field = inputFields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if(fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // save the offset and subfield cnt and expand row
                accessIndexMap.put(oldIndex, currentIndex);
                fieldCountMap.put(oldIndex, subFields.size());
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(getFieldType(rowField.getType()));
                }
                currentIndex += subFields.size();
            } else {
                accessIndexMap.put(oldIndex, currentIndex);
                inputTypeList.add(getFieldType(fieldType));
                currentIndex++;
            }
        }

        //get outputTypes info
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (int oldIndex = 0; oldIndex < fields.size(); oldIndex++) {
            RowType.RowField field = fields.get(oldIndex);
            LogicalType fieldType = field.getType();
            if(fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                // get the subfield of the nested row
                List<RowType.RowField> subFields = ((RowType) field.getType()).getFields();
                // expand row
                for (RowType.RowField rowField : subFields) {
                    outputTypeList.add(getFieldType(rowField.getType()));
                }
            } else {
                outputTypeList.add(getFieldType(fieldType));
            }
        }

        // Set the accessIndexMap
        RexNodeUtil.accessIndexMap = accessIndexMap;

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        //get output indices
        jsonMap.put("originDescription", oldDescription);
        List<Map<String, Object>> projectDes = new ArrayList<>();
        for (List<RexNode> project : projects) {
            List<Map<String, Object>> indicesList = new ArrayList<>();
            for (RexNode rexNode : project) {
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
            Map<String, Object> projects = new LinkedHashMap<>();

            projects.put("inputTypes", inputTypeList);
            projects.put("outputTypes", outputTypeList);
            projects.put("indices", indicesList);
            projectDes.add(projects);
            jsonMap.put("projects", projectDes);
        }

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            // Handle the exception or log it
            LOG.warn("write expand jsonMap as string failed!", e);
        }
        RexNodeUtil.accessIndexMap.clear();
        return jsonString;
    }
}