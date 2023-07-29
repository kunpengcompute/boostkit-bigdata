/**
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
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
#include "tablescan/ParquetReader.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;
using namespace arrow;
using namespace parquet::arrow;
using namespace spark::reader;

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

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_initializeReader(JNIEnv *env,
    jobject jObj, jobject jsonObj)
{
    JNI_FUNC_START
    // Get filePath
    jstring path = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("filePath"));
    const char *filePath = env->GetStringUTFChars(path, JNI_FALSE);
    std::string file(filePath);
    env->ReleaseStringUTFChars(path, filePath);

    jstring ugiTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("ugi"));
    const char *ugi = env->GetStringUTFChars(ugiTemp, JNI_FALSE);
    std::string ugiString(ugi);
    env->ReleaseStringUTFChars(ugiTemp, ugi);

    // Get capacity for each record batch
    int64_t capacity = (int64_t)env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("capacity"));

    // Get RowGroups and Columns indices
    auto row_group_indices = GetIndices(env, jsonObj, "rowGroupIndices");
    auto column_indices = GetIndices(env, jsonObj, "columnIndices");

    ParquetReader *pReader = new ParquetReader();
    auto state = pReader->InitRecordReader(file, capacity, row_group_indices, column_indices, ugiString);
    if (state != Status::OK()) {
        env->ThrowNew(runtimeExceptionClass, state.ToString().c_str());
        return 0;
    }
    return (jlong)(pReader);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong reader, jintArray typeId, jlongArray vecNativeId)
{
    JNI_FUNC_START
    ParquetReader *pReader = (ParquetReader *)reader;
    std::shared_ptr<RecordBatch> recordBatchPtr;
    auto state = pReader->ReadNextBatch(&recordBatchPtr);
    if (state != Status::OK()) {
        env->ThrowNew(runtimeExceptionClass, state.ToString().c_str());
        return 0;
    }
    int vecCnt = 0;
    long batchRowSize = 0;
    if (recordBatchPtr != NULL) {
        batchRowSize = recordBatchPtr->num_rows();
        vecCnt = recordBatchPtr->num_columns();
        std::vector<std::shared_ptr<Field>> fields = recordBatchPtr->schema()->fields();

        for (int colIdx = 0; colIdx < vecCnt; colIdx++) {
            std::shared_ptr<Array> array = recordBatchPtr->column(colIdx);
            // One array in current batch
            std::shared_ptr<ArrayData> data = array->data();
            int omniTypeId = 0;
            uint64_t omniVecId = 0;
            spark::reader::CopyToOmniVec(data->type, omniTypeId, omniVecId, array);

            env->SetIntArrayRegion(typeId, colIdx, 1, &omniTypeId);
            jlong omniVec = static_cast<jlong>(omniVecId);
            env->SetLongArrayRegion(vecNativeId, colIdx, 1, &omniVec);
        }
    }
    return (jlong)batchRowSize;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_recordReaderClose(JNIEnv *env,
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
