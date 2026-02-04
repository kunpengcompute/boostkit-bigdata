/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "OrcColumnarBatchJniWriter.h"
#include <memory>
#include <orc/Type.hh>
#include "jni_common.h"
#include "orcfile/OmniWriter.hh"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::writer;
using namespace orc;

static constexpr int32_t DECIMAL_PRECISION_INDEX = 0;
static constexpr int32_t DECIMAL_SCALE_INDEX = 1;
static constexpr int32_t MINOR_VERSION_11 = 11;
static constexpr int32_t MINOR_VERSION_12 = 12;
static constexpr int32_t MAJOR_VERSION_0 = 0;

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeOutputStream(
    JNIEnv *env, jobject jObj, jobject uriJson)
{
    JNI_FUNC_START
    jstring schemaJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("scheme"));
    const char *schemaPtr = env->GetStringUTFChars(schemaJstr, nullptr);
    std::string schemaStr(schemaPtr);
    env->ReleaseStringUTFChars(schemaJstr, schemaPtr);
    jstring fileJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("path"));
    const char *filePtr = env->GetStringUTFChars(fileJstr, nullptr);
    std::string fileStr(filePtr);
    env->ReleaseStringUTFChars(fileJstr, filePtr);
    jstring hostJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("host"));
    const char *hostPtr = env->GetStringUTFChars(hostJstr, nullptr);
    std::string hostStr(hostPtr);
    env->ReleaseStringUTFChars(hostJstr, hostPtr);
    jint port = (jint)env->CallIntMethod(uriJson, jsonMethodInt, env->NewStringUTF("port"));
    UriInfo uri{schemaStr, fileStr, hostStr, std::to_string(port)};
    std::unique_ptr<orc::OutputStream> outputStream = orc::writeFileOverride(uri);
    orc::OutputStream *outputStreamNew = outputStream.release();
    return (jlong)(outputStreamNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeSchemaType(
        JNIEnv *env, jobject jObj, jintArray orcTypeIds, jobjectArray schemaNames, jobjectArray decimalParam)
{
    JNI_FUNC_START
    auto orcTypeIdPtr = env->GetIntArrayElements(orcTypeIds, JNI_FALSE);
    if (orcTypeIdPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "Orc type ids should not be null.");
    }
    // 获取列名长度，orcTypeIds长度已经不确定，包含嵌套子列
    auto schemaNamesLength = (int32_t)env->GetArrayLength(schemaNames);
    auto writeType = createPrimitiveType(orc::TypeKind::STRUCT);

    int typeOffset = 0;  //用来给嵌套类型做偏移

    for (int i = 0; i < schemaNamesLength; ++i) {
        jint orcType = orcTypeIdPtr[i + typeOffset];
        jstring schemaName = (jstring)env->GetObjectArrayElement(schemaNames, i);
        const char *cSchemaName = env->GetStringUTFChars(schemaName, nullptr);
        std::unique_ptr<orc::Type> writeOrcType;

        // 遇到LIST类型，取出紧随其后的子类型，并让偏移量+1，map类型则偏移量+2
        if (static_cast<TypeKind>(orcType) == orc::TypeKind::LIST) {
            jint elementTypeId = orcTypeIdPtr[i + typeOffset + 1];
            writeOrcType = createListType(createPrimitiveType(static_cast<TypeKind>(elementTypeId)));
            typeOffset += 1;
        }else if (static_cast<TypeKind>(orcType) == orc::TypeKind::MAP) {
            jint keyTypeId = orcTypeIdPtr[i + typeOffset + 1];
            jint valueTypeId = orcTypeIdPtr[i + typeOffset + 2];
            writeOrcType = createMapType(
                    createPrimitiveType(static_cast<TypeKind>(keyTypeId)),
                    createPrimitiveType(static_cast<TypeKind>(valueTypeId)));
            typeOffset += 2;
        }
        else if (static_cast<TypeKind>(orcType) == orc::TypeKind::DECIMAL) {
            auto decimalParamArray = (jintArray)env->GetObjectArrayElement(decimalParam, i);
            auto decimalParamArrayPtr = env->GetIntArrayElements(decimalParamArray, JNI_FALSE);
            auto precision = decimalParamArrayPtr[DECIMAL_PRECISION_INDEX];
            auto scale = decimalParamArrayPtr[DECIMAL_SCALE_INDEX];
            writeOrcType = createDecimalType(precision, scale);
            env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
        } else {
            writeOrcType = createPrimitiveType(static_cast<TypeKind>(orcType));
        }
        writeType->addStructField(std::string(cSchemaName), std::move(writeOrcType));
        env->ReleaseStringUTFChars(schemaName, cSchemaName);
    }

    env->ReleaseIntArrayElements(orcTypeIds, orcTypeIdPtr, JNI_ABORT);
    orc::Type *writerTypeNew = writeType.release();
    return (jlong)(writerTypeNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeWriter(
    JNIEnv *env, jobject jObj, jlong outputStream, jlong schemaType, jobject writerOptionsJson)
{
    JNI_FUNC_START
    // Set write options
    // other param should set here, like padding tolerance, columns use bloom
    // filter, bloom filter fpp ...
    orc::MemoryPool *pool = orc::getDefaultPool();
    orc::WriterOptions writerOptions;
    writerOptions.setMemoryPool(pool);

    // Parsing and setting file version
    jobject versionJosnObj =
        (jobject)env->CallObjectMethod(writerOptionsJson, jsonMethodJsonObj, env->NewStringUTF("file version"));
    jint majorJint = (jint)env->CallIntMethod(versionJosnObj, jsonMethodInt, env->NewStringUTF("major"));
    jint minorJint = (jint)env->CallIntMethod(versionJosnObj, jsonMethodInt, env->NewStringUTF("minor"));
    uint32_t major = (uint32_t)majorJint;
    uint32_t minor = (uint32_t)minorJint;
    if (minor == MINOR_VERSION_11 && major == 0) {
        writerOptions.setFileVersion(FileVersion::v_0_11());
    }
    else if (minor == MINOR_VERSION_12 && major == 0) {
        writerOptions.setFileVersion(FileVersion::v_0_12());
    }
    else {
        env->ThrowNew(runtimeExceptionClass, "Unsupported file version.");
    }

    jint compressionJint = (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("compression"));
    writerOptions.setCompression(static_cast<CompressionKind>(compressionJint));

    jlong stripSizeJint =
        (jlong)env->CallLongMethod(writerOptionsJson, jsonMethodLong, env->NewStringUTF("strip size"));
    writerOptions.setStripeSize(stripSizeJint);

    jint rowIndexStrideJint =
        (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("row index stride"));
    writerOptions.setRowIndexStride((uint64_t)rowIndexStrideJint);

    jint compressionStrategyJint =
        (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("compression strategy"));
    writerOptions.setCompressionStrategy(static_cast<CompressionStrategy>(compressionStrategyJint));

    jstring timezoneKey = env->NewStringUTF("timezone");
    jstring timezoneJStr = (jstring)env->CallObjectMethod(writerOptionsJson, jsonMethodString, timezoneKey);
    env->DeleteLocalRef(timezoneKey);
    if (timezoneJStr != NULL) {
        const char *tzChars = env->GetStringUTFChars(timezoneJStr, NULL);
        std::string tzName(tzChars);
        writerOptions.setTimezoneName(tzName);
        env->ReleaseStringUTFChars(timezoneJStr, tzChars);
        env->DeleteLocalRef(timezoneJStr);
    } else {
        std::cout << "[warning] OrcColumnarBatchJniWriter : timezone not found in writerOptions" << std::endl;
    }

    orc::OutputStream *stream = (orc::OutputStream *)outputStream;
    orc::Type *writeType = (orc::Type *)schemaType;

    std::unique_ptr<OmniWriter> writer = createOmniWriter((*writeType), stream, writerOptions);
    OmniWriter *writerNew = writer.release();
    return (jlong)(writerNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jint numRows)
{
    JNI_FUNC_START

    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    OmniWriter *writerPtr = (OmniWriter *)writer;
    std::vector<BaseVector *> colVecs;
    for (int i = 0; i < colNums; ++i) {
        if (!dataColumnsIdsPtr[i]) {
            continue;
        }
        colVecs.push_back((BaseVector *)vecNativeIdPtr[i]);
    }
    std::unique_ptr<RowVector> rowVec = std::make_unique<RowVector>(numRows, colVecs);
    writerPtr->add(rowVec.get(), 0, numRows);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    auto writeRows = endPos - startPos;
    OmniWriter *writerPtr = (OmniWriter *)writer;
    std::vector<BaseVector *> colVecs;
    for (int i = 0; i < colNums; ++i) {
        if (!dataColumnsIdsPtr[i]) {
            continue;
        }
        colVecs.push_back((BaseVector *)vecNativeIdPtr[i]);
    }
    std::unique_ptr<RowVector> rowVec = std::make_unique<RowVector>(writeRows, colVecs);
    writerPtr->add(rowVec.get(), startPos, endPos);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
                                                                                          jlong outputStream,
                                                                                          jlong schemaType,
                                                                                          jlong writer)
{
    JNI_FUNC_START

    OmniWriter *writerPtr = (OmniWriter *)writer;
    if (writerPtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for writer");
    }

    try {
        writerPtr->close();
    }
    catch (const char *e) {
        std::string errorMsg = "close columnar writer fail:";
        errorMsg += e;
        env->ThrowNew(runtimeExceptionClass, errorMsg.c_str());
    }

    orc::Type *schemaTypePtr = (orc::Type *)schemaType;
    if (schemaTypePtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for write schema type");
    }
    delete schemaTypePtr;

    delete writerPtr;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
