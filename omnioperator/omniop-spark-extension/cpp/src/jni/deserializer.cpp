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

#include "jni_common.h"
#include "deserializer.hh"
#include "common/common.h"

using namespace omniruntime::vec;

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseInit(
    JNIEnv *env, jobject obj, jlong address, jint length)
{
    JNI_FUNC_START
    // tranform protobuf bytes to VecBatch
    auto *vecBatch = new spark::VecBatch();
    vecBatch->ParseFromArray(reinterpret_cast<char*>(address), length);
    return (jlong)(vecBatch);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseClose(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::VecBatch* vecBatch = reinterpret_cast<spark::VecBatch*>(address);
    delete vecBatch;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseVecCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::VecBatch* vecBatch = reinterpret_cast<spark::VecBatch*>(address);
    return vecBatch->veccnt();
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseRowCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::VecBatch* vecBatch = reinterpret_cast<spark::VecBatch*>(address);
    return vecBatch->rowcnt();
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseBatch(
    JNIEnv *env, jobject obj, jlong address, jintArray typeIdArray, jintArray precisionArray,
    jintArray scaleArray, jlongArray vecNativeIdArray)
{
    spark::VecBatch* vecBatch = reinterpret_cast<spark::VecBatch*>(address);
    int32_t vecCount = vecBatch->veccnt();
    int32_t rowCount = vecBatch->rowcnt();
    omniruntime::vec::BaseVector* vecs[vecCount];

    JNI_FUNC_START
    jint *typeIdArrayElements = env->GetIntArrayElements(typeIdArray, NULL);
    jint *precisionArrayElements = env->GetIntArrayElements(precisionArray, NULL);
    jint *scaleArrayElements = env->GetIntArrayElements(scaleArray, NULL);
    jlong *vecNativeIdArrayElements = env->GetLongArrayElements(vecNativeIdArray, NULL);

    for (auto i = 0; i < vecCount; ++i) {
        const spark::Vec& protoVec = vecBatch->vecs(i);
        const spark::VecType& protoTypeId = protoVec.vectype();
        scaleArrayElements[i] = protoTypeId.scale();
        precisionArrayElements[i] = protoTypeId.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoTypeId.typeid_());
 
        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
        vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);


        auto values = protoVec.values().data();
        auto offsets = protoVec.offset().data();
        auto nulls = protoVec.nulls().data();

        if (vectorDataTypeId == OMNI_CHAR || vectorDataTypeId == OMNI_VARCHAR) {
            auto charVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vecs[i]);
            char *valuesAddress =
                omniruntime::vec::unsafe::UnsafeStringVector::ExpandStringBuffer(charVec, protoVec.values().size());
            auto offsetsAddress = (uint8_t *)VectorHelper::UnsafeGetOffsetsAddr(vecs[i]);
            memcpy_s(valuesAddress, protoVec.values().size(), values, protoVec.values().size());
            memcpy_s(offsetsAddress, protoVec.offset().size(), offsets, protoVec.offset().size());
        } else {
            auto *valuesAddress = (uint8_t *)VectorHelper::UnsafeGetValues(vecs[i]);
            memcpy_s(valuesAddress, protoVec.values().size(), values, protoVec.values().size());
        }
        for (auto j = 0; j < protoVec.nulls().size(); ++j) {
            if (int(nulls[j])) {
                vecs[i]->SetNull(j);
            }
        }
    }
    
    env->ReleaseIntArrayElements(typeIdArray, typeIdArrayElements, 0);
    env->ReleaseIntArrayElements(precisionArray, precisionArrayElements, 0);
    env->ReleaseIntArrayElements(scaleArray, scaleArrayElements, 0);
    env->ReleaseLongArrayElements(vecNativeIdArray, vecNativeIdArrayElements, 0);
    JNI_FUNC_END_WITH_VECTORS(runtimeExceptionClass, vecs)
}


JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseInit(
    JNIEnv *env, jobject obj, jlong address, jint length)
{
    JNI_FUNC_START
    // tranform protobuf bytes to ProtoRowBatch
    auto *protoRowBatch = new spark::ProtoRowBatch();
    protoRowBatch->ParseFromArray(reinterpret_cast<char*>(address), length);
    return (jlong)(protoRowBatch);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseClose(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::ProtoRowBatch* protoRowBatch = reinterpret_cast<spark::ProtoRowBatch*>(address);
    delete protoRowBatch;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseVecCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::ProtoRowBatch* protoRowBatch = reinterpret_cast<spark::ProtoRowBatch*>(address);
    return protoRowBatch->veccnt();
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseRowCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    spark::ProtoRowBatch* protoRowBatch = reinterpret_cast<spark::ProtoRowBatch*>(address);
    return protoRowBatch->rowcnt();
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseBatch(
    JNIEnv *env, jobject obj, jlong address, jintArray typeIdArray, jintArray precisionArray,
    jintArray scaleArray, jlongArray vecNativeIdArray)
{
    spark::ProtoRowBatch* protoRowBatch = reinterpret_cast<spark::ProtoRowBatch*>(address);
    int32_t vecCount = protoRowBatch->veccnt();
    int32_t rowCount = protoRowBatch->rowcnt();
    omniruntime::vec::BaseVector* vecs[vecCount];
    std::vector<omniruntime::type::DataTypeId> omniDataTypeIds(vecCount);

    JNI_FUNC_START
    jint *typeIdArrayElements = env->GetIntArrayElements(typeIdArray, NULL);
    jint *precisionArrayElements = env->GetIntArrayElements(precisionArray, NULL);
    jint *scaleArrayElements = env->GetIntArrayElements(scaleArray, NULL);
    jlong *vecNativeIdArrayElements = env->GetLongArrayElements(vecNativeIdArray, NULL);

    for (auto i = 0; i < vecCount; ++i) {
        const spark::VecType& protoTypeId = protoRowBatch->vectypes(i);
        scaleArrayElements[i] = protoTypeId.scale();
        precisionArrayElements[i] = protoTypeId.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoTypeId.typeid_());
        omniDataTypeIds[i] = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
 
        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
        vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);
    }

    auto *parser = new RowParser(omniDataTypeIds);
    char *rows = const_cast<char*>(protoRowBatch->rows().data());
    const int32_t *offsets = reinterpret_cast<const int32_t*>(protoRowBatch->offsets().data());
    for (auto i = 0; i < rowCount; ++i) {
        char *rowPtr = rows + offsets[i];
        parser->ParseOneRow(reinterpret_cast<uint8_t*>(rowPtr), vecs, i);
    }
    
    env->ReleaseIntArrayElements(typeIdArray, typeIdArrayElements, 0);
    env->ReleaseIntArrayElements(precisionArray, precisionArrayElements, 0);
    env->ReleaseIntArrayElements(scaleArray, scaleArrayElements, 0);
    env->ReleaseLongArrayElements(vecNativeIdArray, vecNativeIdArrayElements, 0);
    delete parser;
    JNI_FUNC_END_WITH_VECTORS(runtimeExceptionClass, vecs)
}
