/**
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "ParquetColumnarBatchJniReader.h"
#include "jni_common.h"
#include "parquet/ParquetReader.h"
#include "parquet/ParquetExpression.h"
#include "common/UriInfo.h"

using namespace omniruntime::reader;

std::vector<int> GetIndices(JNIEnv *env, jobject jsonObj, const char* name)
{
    jintArray indicesArray = (jintArray)env->CallObjectMethod(jsonObj, jsonMethodObj, env->NewStringUTF(name));
    auto length = static_cast<int32_t>(env->GetArrayLength(indicesArray));
    auto ptr = env->GetIntArrayElements(indicesArray, JNI_FALSE);
    std::vector<int> indices;
    for (int32_t i = 0; i < length; i++) {
        indices.push_back(ptr[i]);
    }
    env->ReleaseIntArrayElements(indicesArray, ptr, 0);
    return indices;
}

std::vector<std::string> GetFieldNames(JNIEnv *env, jobject jsonObj)
{
    jobjectArray strArray = (jobjectArray)env->CallObjectMethod(jsonObj, jsonMethodObj, env->NewStringUTF("fieldNames"));
    auto length = static_cast<int32_t>(env->GetArrayLength(strArray));
    std::vector<std::string> indices;
    for (int32_t i = 0; i < length; i++) {
        jstring str = (jstring) env->GetObjectArrayElement(strArray, i);
        const char *cstr = env->GetStringUTFChars(str, JNI_FALSE);
        indices.push_back(cstr);
        env->ReleaseStringUTFChars(str, cstr);
    }
    return indices;
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_ParquetColumnarBatchJniReader_initializeReader(JNIEnv *env,
    jobject jObj, jobject jsonObj)
{
    JNI_FUNC_START
    // Get uriStr
    jstring uri = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("uri"));
    const char *uriStr = env->GetStringUTFChars(uri, JNI_FALSE);
    std::string uriString(uriStr);
    env->ReleaseStringUTFChars(uri, uriStr);

    jstring ugiTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("ugi"));
    const char *ugi = env->GetStringUTFChars(ugiTemp, JNI_FALSE);
    std::string ugiString(ugi);
    env->ReleaseStringUTFChars(ugiTemp, ugi);

    jstring schemeTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("scheme"));
    const char *scheme = env->GetStringUTFChars(schemeTmp, JNI_FALSE);
    std::string schemeString(scheme);
    env->ReleaseStringUTFChars(schemeTmp, scheme);

    jstring hostTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("host"));
    const char *host = env->GetStringUTFChars(hostTmp, JNI_FALSE);
    std::string hostString(host);
    env->ReleaseStringUTFChars(hostTmp, host);

    jstring pathTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("path"));
    const char *path = env->GetStringUTFChars(pathTmp, JNI_FALSE);
    std::string pathString(path);
    env->ReleaseStringUTFChars(pathTmp, path);

    jint port = (jint)env->CallIntMethod(jsonObj, jsonMethodInt, env->NewStringUTF("port"));

    jboolean notNeedFSCache = env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("notNeedFSCache"));

    UriInfo uriInfo(uriString, schemeString, pathString, hostString, std::to_string(port));

    // Get capacity for each record batch
    int64_t capacity = (int64_t)env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("capacity"));

    // Get RowGroups and Columns indices
    auto row_group_indices = GetIndices(env, jsonObj, "rowGroupIndices");
    auto column_indices = GetIndices(env, jsonObj, "columnIndices");

    ParquetReader *pReader = new ParquetReader();
    auto state = pReader->InitRecordReader(uriInfo, capacity, row_group_indices, column_indices, ugiString, notNeedFSCache);
    if (state != Status::OK()) {
        env->ThrowNew(runtimeExceptionClass, state.ToString().c_str());
        return 0;
    }
    return (jlong)(pReader);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_ParquetColumnarBatchJniReader_initializeReaderV2(JNIEnv *env,
    jobject jObj, jobject jsonObj)
{
    JNI_FUNC_START
    // Get uriStr
    jstring uri = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("uri"));
    const char *uriStr = env->GetStringUTFChars(uri, JNI_FALSE);
    std::string uriString(uriStr);
    env->ReleaseStringUTFChars(uri, uriStr);

    jstring ugiTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("ugi"));
    const char *ugi = env->GetStringUTFChars(ugiTemp, JNI_FALSE);
    std::string ugiString(ugi);
    env->ReleaseStringUTFChars(ugiTemp, ugi);

    jstring schemeTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("scheme"));
    const char *scheme = env->GetStringUTFChars(schemeTmp, JNI_FALSE);
    std::string schemeString(scheme);
    env->ReleaseStringUTFChars(schemeTmp, scheme);

    jstring hostTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("host"));
    const char *host = env->GetStringUTFChars(hostTmp, JNI_FALSE);
    std::string hostString(host);
    env->ReleaseStringUTFChars(hostTmp, host);

    jstring pathTmp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("path"));
    const char *path = env->GetStringUTFChars(pathTmp, JNI_FALSE);
    std::string pathString(path);
    env->ReleaseStringUTFChars(pathTmp, path);

    jint port = (jint)env->CallIntMethod(jsonObj, jsonMethodInt, env->NewStringUTF("port"));

    UriInfo uriInfo(uriString, schemeString, pathString, hostString, std::to_string(port));

    // Get capacity for each record batch
    int64_t capacity = (int64_t)env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("capacity"));

    int64_t start = (int64_t)env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("start"));
    int64_t end = (int64_t)env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("end"));

    // Get Filter Expression
    bool hasExpressionTree = static_cast<bool>(env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("expressionTree")));
    Expression pushedFilterArray;
    if (hasExpressionTree) {
        jobject expressionTree = env->CallObjectMethod(jsonObj, jsonMethodJsonObj, env->NewStringUTF("expressionTree"));
        auto result = omniruntime::reader::ParseToArrowExpression(env, expressionTree);
        if (!result.ok()) {
            env->ThrowNew(runtimeExceptionClass, result.status().ToString().c_str());
            return 0;
        }
        pushedFilterArray = result.MoveValueUnsafe();
    }

    auto fieldNames = GetFieldNames(env, jsonObj);

    ParquetReader *pReader = new ParquetReader();
    auto state = pReader->InitRecordReader(uriInfo, start, end, capacity, hasExpressionTree, pushedFilterArray,
        fieldNames, ugiString);
    if (state != Status::OK()) {
        env->ThrowNew(runtimeExceptionClass, state.ToString().c_str());
        return 0;
    }
    return (jlong)(pReader);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_ParquetColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong reader, jlongArray vecNativeId)
{
    JNI_FUNC_START
    ParquetReader *pReader = (ParquetReader *)reader;
    std::vector<omniruntime::vec::BaseVector*> recordBatch(pReader->columnReaders.size(), 0);
    long batchRowSize = 0;
    auto state = pReader->ReadNextBatch(recordBatch, &batchRowSize);
    if (state != Status::OK()) {
        for (auto vec : recordBatch) {
            delete vec;
        }
        recordBatch.clear();
        env->ThrowNew(runtimeExceptionClass, state.ToString().c_str());
        return 0;
    }

    for (uint64_t colIdx = 0; colIdx < recordBatch.size(); colIdx++) {
        auto vector = recordBatch[colIdx];
        // If vector is not initialized, meaning that all data had been read.
        if (vector == NULL) {
            return 0;
        }
        jlong omniVec = (jlong)(vector);
        env->SetLongArrayRegion(vecNativeId, colIdx, 1, &omniVec);
    }

    return (jlong)batchRowSize;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_scan_jni_ParquetColumnarBatchJniReader_recordReaderClose(JNIEnv *env,
    jobject jObj, jlong reader)
{
    JNI_FUNC_START
    ParquetReader *pReader = (ParquetReader *)reader;
    if (nullptr == pReader) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for parquet reader");
        return;
    }
    delete pReader;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
