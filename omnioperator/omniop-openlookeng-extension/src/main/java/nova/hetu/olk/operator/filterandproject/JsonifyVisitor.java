package nova.hetu.olk.operator.filterandproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.VarcharDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

class JsonifyVisitor
        implements RowExpressionVisitor<ObjectNode, Void>
{
    private static final String OPERATOR_PREFIX = "$operator$";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> ARITH_BIN_OPS = new ArrayList<>(
            Arrays.asList("ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULUS"));
    private static final List<String> COM_BIN_OPS = new ArrayList<>(Arrays.asList("GREATER_THAN",
            "GREATER_THAN_OR_EQUAL", "LESS_THAN", "LESS_THAN_OR_EQUAL", "EQUAL", "NOT_EQUAL"));
    private static final List<String> UNARY_OPS = new ArrayList<>(Arrays.asList("NEGATION", "not"));

    @Override
    public ObjectNode visitCall(CallExpression call, Void context)
    {
        ObjectNode callRoot = MAPPER.createObjectNode();
        // demangle name to get rid of OPERATOR_PREFIX
        String callName = call.getDisplayName();
        if (callName.startsWith(OPERATOR_PREFIX)) {
            callName = callName.substring(OPERATOR_PREFIX.length()).toUpperCase(Locale.ROOT);
        }
        TypeSignature callSignature = call.getType().getTypeSignature();
        DataType returnType = OperatorUtils.toDataType(call.getType());
        int typeId = returnType.getId().ordinal();
        // Binary operator in rowExpression
        if (ARITH_BIN_OPS.contains(callName) || COM_BIN_OPS.contains(callName)) {
            callRoot.put("exprType", "BINARY").put("returnType", typeId).put("operator", callName);

            if (returnType instanceof Decimal64DataType) {
                callRoot.put("precision", ((Decimal64DataType) returnType).getPrecision()).put("scale",
                        ((Decimal64DataType) returnType).getScale());
            }
            else if (returnType instanceof Decimal128DataType) {
                callRoot.put("precision", ((Decimal128DataType) returnType).getPrecision()).put("scale",
                        ((Decimal128DataType) returnType).getScale());
            }

            callRoot.set("left", call.getArguments().get(0).accept(this, context));
            callRoot.set("right", call.getArguments().get(1).accept(this, context));
        }
        else if (UNARY_OPS.contains(callName)) {
            // Unary operator in rowExpression
            callRoot.put("exprType", "UNARY").put("returnType", typeId).put("operator", callName).set("expr",
                    call.getArguments().get(0).accept(this, context));
        }
        else {
            // Function call in rowExpression
            ArrayNode arguments = MAPPER.createArrayNode();
            // Process all arguments of this function call
            for (RowExpression argument : call.getArguments()) {
                arguments.add(argument.accept(this, context));
            }
            callRoot.put("exprType", "FUNCTION").put("returnType", typeId).put("function_name", callName)
                    .set("arguments", arguments);
            if (returnType instanceof VarcharDataType) {
                callRoot.put("width", ((VarcharDataType) returnType).getWidth());
            }
            else if (returnType instanceof Decimal64DataType) {
                callRoot.put("precision", ((Decimal64DataType) returnType).getPrecision()).put("scale",
                        ((Decimal64DataType) returnType).getScale());
            }
            else if (returnType instanceof Decimal128DataType) {
                callRoot.put("precision", ((Decimal128DataType) returnType).getPrecision()).put("scale",
                        ((Decimal128DataType) returnType).getScale());
            }
        }
        return callRoot;
    }

    @Override
    public ObjectNode visitSpecialForm(SpecialForm specialForm, Void context)
    {
        ObjectNode specialFormRoot = MAPPER.createObjectNode();
        String formName = specialForm.getForm().name();
        int returnType = OperatorUtils.toDataType(specialForm.getType()).getId().ordinal();
        int size = specialForm.getArguments().size();
        switch (formName) {
            case "AND":
            case "OR":
                specialFormRoot.put("exprType", "BINARY").put("returnType", returnType).put("operator", formName)
                        .set("left", specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("right", specialForm.getArguments().get(1).accept(this, context));
                break;
            case "BETWEEN":
                specialFormRoot.put("exprType", "BETWEEN").put("returnType", returnType).set("value",
                        specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("lower_bound", specialForm.getArguments().get(1).accept(this, context));
                specialFormRoot.set("upper_bound", specialForm.getArguments().get(2).accept(this, context));
                break;
            case "IF":
                specialFormRoot.put("exprType", "IF").put("returnType", returnType).set("condition",
                        specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("if_true", specialForm.getArguments().get(1).accept(this, context));
                specialFormRoot.set("if_false", specialForm.getArguments().get(2).accept(this, context));
                break;
            case "SWITCH":
                specialFormRoot.put("exprType", "SWITCH").put("returnType", returnType).put("numOfCases", size - 2)
                        .set("input", specialForm.getArguments().get(0).accept(this, context));
                for (int i = 1; i < size - 1; i++) {
                    specialFormRoot.set("Case" + i, specialForm.getArguments().get(i).accept(this, context));
                }
                specialFormRoot.set("else", specialForm.getArguments().get(size - 1).accept(this, context));
                break;
            case "WHEN":
                specialFormRoot.put("exprType", "WHEN").put("returnType", returnType).set("when",
                        specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("result", specialForm.getArguments().get(1).accept(this, context));
                break;
            case "COALESCE":
                specialFormRoot.put("exprType", "COALESCE").put("returnType", returnType).set("value1",
                        specialForm.getArguments().get(0).accept(this, context));
                specialFormRoot.set("value2", specialForm.getArguments().get(1).accept(this, context));
                break;
            default:
                ArrayNode arguments = MAPPER.createArrayNode();
                // Process all arguments of this function call
                for (RowExpression argument : specialForm.getArguments()) {
                    arguments.add(argument.accept(this, context));
                }
                specialFormRoot.put("exprType", formName).put("returnType", returnType).set("arguments", arguments);
                break;
        }
        return specialFormRoot;
    }

    @Override
    public ObjectNode visitInputReference(InputReferenceExpression reference, Void context)
    {
        ObjectNode inputRefRoot = MAPPER.createObjectNode();
        DataType dataType = OperatorUtils.toDataType(reference.getType());
        inputRefRoot.put("exprType", "FIELD_REFERENCE").put("dataType", dataType.getId().ordinal()).put("colVal",
                reference.getField());
        if (dataType instanceof CharDataType) {
            inputRefRoot.put("width", ((CharDataType) dataType).getWidth());
        }
        else if (dataType instanceof VarcharDataType) {
            inputRefRoot.put("width", ((VarcharDataType) dataType).getWidth());
        }
        else if (dataType instanceof Decimal64DataType) {
            Decimal64DataType type = ((Decimal64DataType) dataType);
            inputRefRoot.put("precision", type.getPrecision()).put("scale", type.getScale());
        }
        else if (dataType instanceof Decimal128DataType) {
            Decimal128DataType type = ((Decimal128DataType) dataType);
            inputRefRoot.put("precision", type.getPrecision()).put("scale", type.getScale());
        }
        return inputRefRoot;
    }

    @Override
    public ObjectNode visitConstant(ConstantExpression literal, Void context)
    {
        ObjectNode constantRoot = MAPPER.createObjectNode();
        DataType literalType = OperatorUtils.toDataType(literal.getType());
        constantRoot.put("exprType", "LITERAL").put("dataType", literalType.getId().ordinal());
        // Null check on expression value
        if (literal.getValue() == null) {
            constantRoot.put("isNull", true);
            if (literalType.getId() == DataType.DataTypeId.OMNI_DECIMAL64) {
                constantRoot.put("precision", ((Decimal64DataType) literalType).getPrecision());
                constantRoot.put("scale", ((Decimal64DataType) literalType).getScale());
            }
            else if (literalType.getId() == DataType.DataTypeId.OMNI_DECIMAL128) {
                constantRoot.put("precision", ((Decimal128DataType) literalType).getPrecision());
                constantRoot.put("scale", ((Decimal128DataType) literalType).getScale());
            }
            return constantRoot;
        }
        constantRoot.put("isNull", false);
        switch (literalType.getId()) {
            case OMNI_BOOLEAN:
                constantRoot.put("value", Boolean.valueOf(literal.getValue().toString()));
                break;
            case OMNI_DOUBLE:
                constantRoot.put("value", Double.parseDouble(literal.getValue().toString()));
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                constantRoot.put("value", Integer.parseInt(literal.getValue().toString()));
                break;
            case OMNI_LONG:
                constantRoot.put("value", Long.parseLong(literal.getValue().toString()));
                break;
            case OMNI_DECIMAL64:
                constantRoot.put("value", (Long.parseLong(literal.getValue().toString())));
                constantRoot.put("precision", ((Decimal64DataType) literalType).getPrecision());
                constantRoot.put("scale", ((Decimal64DataType) literalType).getScale());
                break;
            case OMNI_DECIMAL128:
                // FIXME: Need to Support 128 bits properly
                String d128Val;
                if (literal.getValue() instanceof Slice) {
                    d128Val = Decimals.decodeUnscaledValue((Slice) literal.getValue()).toString();
                }
                else {
                    d128Val = literal.getValue().toString();
                }
                constantRoot.put("value", d128Val);
                constantRoot.put("precision", ((Decimal128DataType) literalType).getPrecision());
                constantRoot.put("scale", ((Decimal128DataType) literalType).getScale());
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                String varcharValue;
                if (literal.getValue() instanceof Slice) {
                    varcharValue = ((Slice) literal.getValue()).toStringAscii();
                }
                else {
                    varcharValue = String.valueOf(literal.getValue());
                }
                constantRoot.put("value", varcharValue);
                constantRoot.put("width", ((VarcharDataType) literalType).getWidth());
                break;
            case OMNI_NONE:
                // TODO: Support UNKNOWN presto type in DataType
                // omni-runtime treat NONE regardless of its value
                constantRoot.put("value", "UNKNOWN");
                break;
            default:
                constantRoot.put("invalidVal", "invalidVal");
                break;
        }
        return constantRoot;
    }

    @Override
    public ObjectNode visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        ObjectNode lambdaRoot = MAPPER.createObjectNode();
        // TODO: add lambda support in omni-runtime
        lambdaRoot.put("exprType", "LAMBDA");
        return lambdaRoot;
    }

    @Override
    public ObjectNode visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        ObjectNode varRefRoot = MAPPER.createObjectNode();
        DataType dataType = OperatorUtils.toDataType(reference.getType());
        varRefRoot.put("exprType", "VARIABLE_REFERENCE")
                .put("dataType", OperatorUtils.toDataType(reference.getType()).getId().ordinal())
                .put("varName", reference.getName());
        if (dataType instanceof CharDataType) {
            varRefRoot.put("width", ((CharDataType) dataType).getWidth());
        }
        else if (dataType instanceof VarcharDataType) {
            varRefRoot.put("width", ((VarcharDataType) dataType).getWidth());
        }
        return varRefRoot;
    }
}
