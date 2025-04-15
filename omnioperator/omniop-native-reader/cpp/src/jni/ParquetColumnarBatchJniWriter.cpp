/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "ParquetColumnarBatchJniWriter.h"
#include "jni_common.h"
#include "parquet/ParquetWriter.h"
#include "common/UriInfo.h"
#include "arrow/status.h"
#include <arrow/type.h>
#include <arrow/api.h>

using namespace omniruntime::writer;
using namespace arrow;

static constexpr int32_t DECIMAL_PRECISION_INDEX = 0;
static constexpr int32_t DECIMAL_SCALE_INDEX = 1;

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeWriter(
    JNIEnv *env, jobject jObj, jobject jsonObj, jlong writer)
{
    JNI_FUNC_START
    jstring uri = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("uri"));
    const char *uriStr = env->GetStringUTFChars(uri, JNI_FALSE);
    std::string uriString(uriStr);
    env->ReleaseStringUTFChars(uri, uriStr);

    jstring ugiTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("ugi"));
    const char *ugi = env->GetStringUTFChars(ugiTemp, JNI_FALSE);
    std::string ugiString(ugi);
    env->ReleaseStringUTFChars(ugiTemp, ugi);

    jstring schemaTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("scheme"));
    const char *schema = env->GetStringUTFChars(schemaTemp, JNI_FALSE);
    std::string schemaString(schema);
    env->ReleaseStringUTFChars(schemaTemp, schema);

    jstring hostTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("host"));
    const char *host = env->GetStringUTFChars(hostTemp, JNI_FALSE);
    std::string hostString(host);
    env->ReleaseStringUTFChars(hostTemp, host);

    jstring pathTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("path"));
    const char *path = env->GetStringUTFChars(pathTemp, JNI_FALSE);
    std::string pathString(path);
    env->ReleaseStringUTFChars(pathTemp, path);

    jint port = (jint)env->CallIntMethod(jsonObj, jsonMethodInt, env->NewStringUTF("port"));

    UriInfo uriInfo(uriString, schemaString, pathString, hostString, std::to_string(port));
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->InitRecordWriter(uriInfo, ugiString);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeSchema(
    JNIEnv *env, jobject JObj, jlong writer, jobjectArray fieldNames, jintArray fieldTypes,
    jbooleanArray nullables, jobjectArray decimalParam)
{
    JNI_FUNC_START
    auto pWriter = std::make_unique<ParquetWriter>();
    auto fieldTypesPtr = env->GetIntArrayElements(fieldTypes, JNI_FALSE);
    auto nullablesPtr = env->GetBooleanArrayElements(nullables, JNI_FALSE);
    if (nullablesPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "the nullablesPtr is null");
        return writer;
    }
    if (fieldTypesPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "Parquet type ids should not be null");
        return writer;
    }
    auto schemaLength = (int32_t)env->GetArrayLength(fieldTypes);
    FieldVector fieldVector;
    for (int i = 0; i < schemaLength; i++) {
        jint parquetType = fieldTypesPtr[i];
        jboolean nullable = nullablesPtr[i];
        jstring fieldName = (jstring)env->GetObjectArrayElement(fieldNames, i);
        const char *cFieldName = env->GetStringUTFChars(fieldName, nullptr);
        std::shared_ptr<DataType> writeParquetType;

            auto decimalParamArray = (jintArray)env->GetObjectArrayElement(decimalParam, i);
            auto decimalParamArrayPtr = env->GetIntArrayElements(decimalParamArray, JNI_FALSE);
            if (decimalParamArrayPtr == NULL) {
                env->ThrowNew(runtimeExceptionClass, "the decimalParamArrayPtr is null");
                return writer;
            }
            auto precision = decimalParamArrayPtr[DECIMAL_PRECISION_INDEX];
            auto scale = decimalParamArrayPtr[DECIMAL_SCALE_INDEX];
            switch (static_cast<Type::type>(parquetType)) {
            case Type::type::DECIMAL:
                 pWriter->precisions.push_back(precision);
                 pWriter->scales.push_back(scale);
                 writeParquetType = decimal128(precision, scale);
                 break;
            case Type::type::BOOL:
                writeParquetType = arrow::boolean();
                break;
            case Type::type::INT16:
                writeParquetType = arrow::int16();
                break;
            case Type::type::INT32:
                writeParquetType = arrow::int32();
                break;
            case Type::type::INT64:
                writeParquetType = arrow::int64();
                break;
            case Type::type::DATE32:
                writeParquetType = arrow::date32();
                break;
            case Type::type::DATE64:
                writeParquetType = arrow::date64();
                break;
            case Type::type::DOUBLE:
                writeParquetType = arrow::float64();
                break;
            case Type::type::STRING:
                writeParquetType = arrow::utf8();
                break;
            default:
                throw std::invalid_argument("Unsupported parquet type: "+std::to_string(parquetType));
            }
        fieldVector.emplace_back(field(cFieldName, writeParquetType, nullable));
        env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
        env->ReleaseStringUTFChars(fieldName, cFieldName);
    }
    if (pWriter == nullptr) {
            env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
            return writer;
    }
    pWriter->schema_ = std::make_shared<Schema>(fieldVector);
    ParquetWriter *pWriterNew= pWriter.release();
    env->ReleaseIntArrayElements(fieldTypes, fieldTypesPtr, JNI_ABORT);
    env->ReleaseBooleanArrayElements(nullables, nullablesPtr, JNI_ABORT);
    return (jlong)(pWriterNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId,
    jintArray omniTypes, jbooleanArray dataColumnsIds, jint numRows)
{
    JNI_FUNC_START
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->write(vecNativeIdPtr, colNums, omniTypesPtr, dataColumnsIdsPtr);
    env->ReleaseLongArrayElements(vecNativeId, vecNativeIdPtr, 0);
    env->ReleaseIntArrayElements(omniTypes, omniTypesPtr, 0);
    env->ReleaseBooleanArrayElements(dataColumnsIds, dataColumnsIdsPtr, 0);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    auto writeRows = endPos - startPos;
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->write(vecNativeIdPtr, colNums, omniTypesPtr, dataColumnsIdsPtr, true, startPos, endPos);

    env->ReleaseLongArrayElements(vecNativeId, vecNativeIdPtr, 0);
    env->ReleaseIntArrayElements(omniTypes, omniTypesPtr, 0);
    env->ReleaseBooleanArrayElements(dataColumnsIds, dataColumnsIdsPtr, 0);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
   jlong writer)
{
    JNI_FUNC_START

    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for writer");
        return;
    }
    pWriter->arrow_writer->Close();
    delete pWriter;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
