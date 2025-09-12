package org.apache.flink.streaming.runtime.tasks;

import com.huawei.omniruntime.flink.runtime.tasks.OmniDataTypesMap;
import com.huawei.omniruntime.flink.table.types.logical.LogicalTypeDescriptor;
import com.huawei.omniruntime.flink.utils.UdfUtil;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;

public final class OmniOperatorChain {

    private static final Logger LOG = LoggerFactory.getLogger(OmniOperatorChain.class);

    public OmniOperatorChain(StreamOperatorWrapper<?, ?> firstOperatorWrapper) {

    }

    private void setup(StreamOperatorWrapper<?, ?> firstOperatorWrapper) {

    }

    public static String type;

    public static String prepareJSONConfigurationFromAllOps(Iterable<StreamOperatorWrapper<?, ?>> allOperatorWrappers, JobID jobID) {
        JSONObject chainData = new JSONObject();

        chainData.put("operators", prepareJSONOperators(chainData, allOperatorWrappers, jobID));
        chainData.put("type", type);
        return chainData.toString();
    }

    public static String prepareJSONConfiguration(StreamOperatorWrapper<?, ?> firstOperatorWrapper) {
        JSONObject chainData = new JSONObject();

        chainData.put("operators", prepareJSONOperators(chainData, new StreamOperatorWrapper.ReadIterator(firstOperatorWrapper, false), null));

        return chainData.toString();
    }

    private static JSONArray prepareJSONOperators(JSONObject chatData, Iterable<StreamOperatorWrapper<?, ?>> it, JobID jobID) {
        JSONArray operators = new JSONArray();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (StreamOperatorWrapper<?, ?> wrapper : it) {
            JSONObject opData = new JSONObject();
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) wrapper.getStreamOperator();
            StreamConfig sc = op.getOperatorConfig();
            opData.put("name", sc.getOperatorName());
            String id = op.getClass().getCanonicalName().split("\\$")[0];
            opData.put("id", id);
            {
                JSONArray inputsData = new JSONArray();
                StreamConfig.InputConfig[] inputs = sc.getInputs(cl);

                for (StreamConfig.InputConfig i : inputs) {
                    inputsData.put(
                            OmniOperatorChain.parseTypeSerializer(((StreamConfig.NetworkInputConfig) i).getTypeSerializer())
                    );
                }

                opData.put("inputs", inputsData);
            }
            {
                if (id.contains("org.apache.flink.streaming.api.operators")
                        || id.contains("org.apache.flink.streaming.runtime.operators.sink")) {
                    type = "DataStream";
                    JSONObject jsonObject = new JSONObject(sc.getDescription());
                    if (!jsonObject.isNull("udf_so")) {
                        LOG.info("load udf_so:" + jsonObject.getString("udf_so"));
                        jsonObject.put("udf_so", UdfUtil.getJobJarPath(jobID) + jsonObject.getString("udf_so"));
                    }
                    opData.put("description", jsonObject);
                    opData.put("output", OmniOperatorChain.parseTypeSerializer(sc.getTypeSerializerOut(cl)));
                } else if (id != "org.apache.flink.table.runtime.operators.sink.SinkOperator") {
                    type = "table";
                    opData.put("description", rewriteToOmniDatatypes(sc.getDescription()));
                    opData.put("output", OmniOperatorChain.parseTypeSerializer(sc.getTypeSerializerOut(cl)));
                } else {
                    // Placeholder for empty descriptors and output types
                    opData.put("description", new JSONObject());
                    JSONObject out = new JSONObject();
                    out.put("kind", "Row");
                    JSONArray rowType = new JSONArray();
                    out.put("type", rowType);
                    opData.put("output", out);
                }
            }

            operators.put(opData);
        }

        return operators;
    }


    private static JSONObject rewriteToOmniDatatypes(String description) {
        JSONObject jsonObject = new JSONObject(description);
        JSONObject updatedJsonObject = traverseJSONObject(jsonObject);
        return updatedJsonObject;
    }

    public static JSONObject traverseJSONObject(JSONObject jsonObject) {
        if (jsonObject.has("dataType")) {
            String dataTypeStr = jsonObject.getString("dataType");
            jsonObject.remove("dataType");
            if (OmniDataTypesMap.dataTypeMap.containsKey(dataTypeStr)) {
                jsonObject.put("dataTypeId", OmniDataTypesMap.dataTypeMap.get(dataTypeStr));
            } else {
                LOG.debug("Cannot parse the datatype :{} ", dataTypeStr);
                jsonObject.put("dataTypeId", -1);
            }
        } else if (jsonObject.has("returnType")) {
            String dataTypeStr = jsonObject.getString("returnType");
            jsonObject.remove("returnType");
            if (OmniDataTypesMap.dataTypeMap.containsKey(dataTypeStr)) {
                jsonObject.put("returnTypeId", OmniDataTypesMap.dataTypeMap.get(dataTypeStr));
            } else {
                LOG.debug("Cannot parse the datatype :{} ", dataTypeStr);
                jsonObject.put("returnTypeId", -1);
            }
        }
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);

            // If the value is a JSONObject, traverse it
            if (value instanceof JSONObject) {
                JSONObject childObject = (JSONObject) value;
                // Recur for the child JSONObject
                traverseJSONObject(childObject);
            }

            // If the value is a JSONArray, traverse each element
            else if (value instanceof JSONArray) {
                JSONArray array = (JSONArray) value;
                for (int i = 0; i < array.length(); i++) {
                    Object arrayElement = array.get(i);
                    // If an element is a JSONObject, traverse it
                    if (arrayElement instanceof JSONObject) {
                        traverseJSONObject((JSONObject) arrayElement);
                    }
                }
            }
        }
        return jsonObject;
    }

    private static JSONObject parseTypeSerializer(TypeSerializer<?> ts) {
        JSONObject data = new JSONObject();
        Object typeObj = "Void";
        String kindStr = "basic";

        // NOTE(yar): Could be enums, but will require syncing enums between Java and C++
        //   The whole thing could be a protobuf instead too
        if (ts instanceof StringSerializer) {
            typeObj = "String";
        } else if (ts instanceof BooleanSerializer) {
            typeObj = "Boolean";
        } else if (ts instanceof ByteSerializer) {
            typeObj = "Byte";
        } else if (ts instanceof CharSerializer) {
            typeObj = "Character";
        } else if (ts instanceof DoubleSerializer) {
            typeObj = "Double";
        } else if (ts instanceof FloatSerializer) {
            typeObj = "Float";
        } else if (ts instanceof LongSerializer) {
            typeObj = "Long";
        } else if (ts instanceof IntSerializer) {
            typeObj = "Short";
        } else if (ts instanceof ShortSerializer) {
            typeObj = "Integer";
        } else if (ts instanceof TupleSerializer) {
            kindStr = "Tuple";
            typeObj = parseTupleSerializer((TupleSerializer<?>) ts);
        } else if (ts instanceof RowDataSerializer) {
            kindStr = "Row";
            try {
                typeObj = extractRowDataSerializer((RowDataSerializer) ts);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (ts != null) {
//            throw new RuntimeException("Cannot setup OmniOperatorChain because an unsupported serializer type was requested: " + ts.toString());
        }

        data.put("kind", kindStr);
        data.put("type", typeObj);

        return data;
    }

    private static JSONArray parseTupleSerializer(TupleSerializer<?> ts) {
        JSONArray tupleData = new JSONArray();

        for (int i = 0; i < ts.getArity(); i++) {
            tupleData.put(parseTypeSerializer(ts.getFieldSerializers()[i]));
        }

        return tupleData;
    }

    private static JSONArray extractRowDataSerializer(RowDataSerializer rowDataSerializers) throws NoSuchFieldException, IllegalAccessException {
        JSONArray rowFields = new JSONArray();
        Field f = rowDataSerializers.getClass().getDeclaredField("types");
        f.setAccessible(true);
        LogicalType[] logicalTypes = (LogicalType[]) f.get(rowDataSerializers);
        for (LogicalType type : logicalTypes) {
            if (type instanceof BigIntType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorBigIntType((BigIntType) type));
            } else if (type instanceof VarCharType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorVarCharType((VarCharType) type));
            } else if (type instanceof TimestampType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorTimestampType((TimestampType) type));
            } else {

            }

        }
        return rowFields;
    }


    /**
     * Links operator wrappers in forward topological order.
     *
     * @param allOperatorWrappers is an operator wrapper list of reverse topological order
     */
    private static StreamOperatorWrapper<?, ?> linkOperatorWrappers(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers) {
        StreamOperatorWrapper<?, ?> previous = null;
        for (StreamOperatorWrapper<?, ?> current : allOperatorWrappers) {
            if (previous != null) {
                previous.setPrevious(current);
            }
            current.setNext(previous);
            previous = current;
        }
        return previous;
    }

}