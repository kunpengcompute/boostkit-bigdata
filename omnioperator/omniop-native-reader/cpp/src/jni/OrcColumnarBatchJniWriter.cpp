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
#include <orc/Vector.hh>
#include <orc/Writer.hh>
#include "jni_common.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
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
    auto orcTypeIdLength = (int32_t)env->GetArrayLength(orcTypeIds);
    auto writeType = createPrimitiveType(orc::TypeKind::STRUCT);

    for (int i = 0; i < orcTypeIdLength; ++i) {
        jint orcType = orcTypeIdPtr[i];
        jstring schemaName = (jstring)env->GetObjectArrayElement(schemaNames, i);
        const char *cSchemaName = env->GetStringUTFChars(schemaName, nullptr);
        std::unique_ptr<orc::Type> writeOrcType;
        if (static_cast<TypeKind>(orcType) == orc::TypeKind::DECIMAL) {
            auto decimalParamArray = (jintArray)env->GetObjectArrayElement(decimalParam, i);
            auto decimalParamArrayPtr = env->GetIntArrayElements(decimalParamArray, JNI_FALSE);
            auto precision = decimalParamArrayPtr[DECIMAL_PRECISION_INDEX];
            auto scale = decimalParamArrayPtr[DECIMAL_SCALE_INDEX];
            writeOrcType = createDecimalType(precision, scale);
        }
        else {
            writeOrcType = createPrimitiveType(static_cast<TypeKind>(orcType));
        }
        writeType->addStructField(std::string(cSchemaName), std::move(writeOrcType));
        env->ReleaseStringUTFChars(schemaName, cSchemaName);
    }

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

    orc::OutputStream *stream = (orc::OutputStream *)outputStream;
    orc::Type *writeType = (orc::Type *)schemaType;

    std::unique_ptr<orc::Writer> writer = createWriter((*writeType), stream, writerOptions);
    orc::Writer *writerNew = writer.release();
    return (jlong)(writerNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeBatch(JNIEnv *env,
                                                                                                     jobject jObj,
                                                                                                     jlong writer,
                                                                                                     jlong batchSize)
{
    orc::Writer *writerPtr = (orc::Writer *)writer;
    auto rowBatch = writerPtr->createRowBatch(batchSize);
    orc::ColumnVectorBatch *batch = rowBatch.release();
    return (jlong)(batch);
}

template <DataTypeId TYPE_ID, typename V>
void WriteVector(JNIEnv *env, BaseVector *vec, ColumnVectorBatch *fieldBatch, bool isSplitWrite = false, long startPos = 0,
                 long endPos = 0)
{
    using T = typename NativeType<TYPE_ID>::type;
    auto vector = (Vector<T> *)vec;
    V *lvb = dynamic_cast<V *>(fieldBatch);
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    long index = 0;
    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
    }
    if (vector->HasNull()) {
        lvb->hasNulls = true;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                notNulls[index] = 0;
            }
            index++;
        }
    }
    index = 0;
    if(sizeof(T) == sizeof(values[0])){
        errno_t err;
        err = memcpy_s(values, sizeof(T) * (endPos - startPos),
                  omniruntime::vec::unsafe::UnsafeVector::GetRawValues(vector) + startPos,
                  sizeof(T) * (endPos - startPos));
        if (err != EOK) {
            env->ThrowNew(runtimeExceptionClass,"Get values from vector failed");
        }
    } else {
        for (long j = startPos; j < endPos; j++) {
                values[index] = vector->GetValue(j);
                index++;
            }
    }
}

void WriteDecimal128VectorBatch(BaseVector *vec, ColumnVectorBatch *fieldBatch, bool isSplitWrite = false,
                                long startPos = 0, long endPos = 0)
{
    auto vector = (Vector<Decimal128> *)vec;
    auto *lvb = dynamic_cast<orc::Decimal128VectorBatch *>(fieldBatch);
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    long index = 0;
    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
    }
    if (vector->HasNull()) {
        lvb->hasNulls = true;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                notNulls[index] = 0;
            }
            index++;
        }
    }
    index = 0;
    for (long j = startPos; j < endPos; j++) {
        values[index] = orc::Int128(vector->GetValue(j).HighBits(), vector->GetValue(j).LowBits());
        index++;
    }
}

void WriteDecimal64VectorBatch(JNIEnv *env, BaseVector *vec, ColumnVectorBatch *fieldBatch, bool isSplitWrite = false,
                               long startPos = 0, long endPos = 0)
{
    auto vector = (Vector<int64_t> *)vec;
    auto *lvb = dynamic_cast<orc::Decimal64VectorBatch *>(fieldBatch);
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    long index = 0;
    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
    }
    if (vector->HasNull()) {
        lvb->hasNulls = true;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                notNulls[index] = 0;
            }
            index++;
        }
    }
    errno_t err;
    err = memcpy_s(values,sizeof(values[0]) * (endPos - startPos),
              omniruntime::vec::unsafe::UnsafeVector::GetRawValues(vector) + startPos,
              sizeof(vector->GetValue(0)) * (endPos - startPos));
    if (err != EOK) {
        env->ThrowNew(runtimeExceptionClass,"Get values from vector failed");
    }
}

void WriteVarCharVectorBatch(BaseVector *baseVector, ColumnVectorBatch *fieldBatch, bool isSplitWrite = false,
                             long startPos = 0, long endPos = 0)
{
    auto vector = (Vector<LargeStringContainer<std::string_view>> *)baseVector;
    auto *lvb = dynamic_cast<orc::StringVectorBatch *>(fieldBatch);
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto lens = lvb->length.data();
    long index = 0;
    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
    }
    if (vector->HasNull()) {
        lvb->hasNulls = true;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                notNulls[index] = 0;
            }
            index++;
        }
    }
    index = 0;
    for (long j = startPos; j < endPos; j++) {
        values[index] = const_cast<char *>(vector->GetValue(j).data());
        lens[index] = vector->GetValue(j).size();
        index++;
    }
}

void WriteLongVectorBatch(JNIEnv *env, DataTypeId typeId, BaseVector *baseVector, ColumnVectorBatch *fieldBatch,
                          bool isSplitWrite = false, long startPos = 0, long endPos = 0)
{
    JNI_FUNC_START
    switch (typeId) {
    case OMNI_BOOLEAN:
        return WriteVector<OMNI_BOOLEAN, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    case OMNI_SHORT:
        return WriteVector<OMNI_SHORT, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    case OMNI_INT:
        return WriteVector<OMNI_INT, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    case OMNI_LONG:
        return WriteVector<OMNI_LONG, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    case OMNI_DATE32:
        return WriteVector<OMNI_DATE32, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    case OMNI_DATE64:
        return WriteVector<OMNI_DATE64, orc::LongVectorBatch>(env, baseVector, fieldBatch, isSplitWrite, startPos, endPos);
    default:
        env->ThrowNew(runtimeExceptionClass, "DealLongVectorBatch not support for type:" + typeId);
    }
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

void WriteVector(JNIEnv *env, long *vecNativeId, int colNums, orc::StructVectorBatch *batch, const int *omniTypes,
                 const unsigned char *dataColumnsIds, bool isSplitWrite = false, long startPos = 0, long endPos = 0)
{
    JNI_FUNC_START
    for (int i = 0; i < colNums; ++i) {
        if (!dataColumnsIds[i]) {
            continue;
        }
        auto vec = (BaseVector *)vecNativeId[i];
        auto typeId = static_cast<DataTypeId>(omniTypes[i]);
        auto fieldBatch = batch->fields[i];
        switch (typeId) {
        case OMNI_BOOLEAN:
        case OMNI_SHORT:
        case OMNI_INT:
        case OMNI_LONG:
        case OMNI_DATE32:
        case OMNI_DATE64:
            WriteLongVectorBatch(env, typeId, vec, fieldBatch, isSplitWrite, startPos, endPos);
            break;
        case OMNI_DOUBLE:
            WriteVector<OMNI_DOUBLE, orc::DoubleVectorBatch>(env, vec, fieldBatch, isSplitWrite, startPos, endPos);
            break;
        case OMNI_VARCHAR:
            WriteVarCharVectorBatch(vec, fieldBatch, isSplitWrite, startPos, endPos);
            break;
        case OMNI_DECIMAL64:
            WriteDecimal64VectorBatch(env, vec, fieldBatch, isSplitWrite, startPos, endPos);
            break;
        case OMNI_DECIMAL128:
            WriteDecimal128VectorBatch(vec, fieldBatch, isSplitWrite, startPos, endPos);
            break;
        default:
            env->ThrowNew(runtimeExceptionClass, "Native columnar write not support for this type: " + typeId);
        }
    }
    JNI_FUNC_END_VOID(runtimeExceptionClass)
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
    orc::Writer *writerPtr = (orc::Writer *)writer;
    auto rowBatch = writerPtr->createRowBatch(numRows);
    rowBatch->numElements = numRows;
    orc::StructVectorBatch *batch = static_cast<orc::StructVectorBatch *>(rowBatch.get());
    WriteVector(env, vecNativeIdPtr, colNums, batch, omniTypesPtr, dataColumnsIdsPtr);
    writerPtr->add(*batch);
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
    orc::Writer *writerPtr = (orc::Writer *)writer;
    auto rowBatch = writerPtr->createRowBatch(writeRows);
    rowBatch->numElements = writeRows;
    orc::StructVectorBatch *batch = static_cast<orc::StructVectorBatch *>(rowBatch.get());
    WriteVector(env, vecNativeIdPtr, colNums, batch, omniTypesPtr, dataColumnsIdsPtr, true, startPos, endPos);
    writerPtr->add(*batch);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
                                                                                          jlong outputStream,
                                                                                          jlong schemaType,
                                                                                          jlong writer)
{
    JNI_FUNC_START

    orc::Writer *writerPtr = (orc::Writer *)writer;
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

    orc::OutputStream *outputStreamPtr = (orc::OutputStream *)outputStream;
    if (outputStreamPtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for write output stream");
    }
    delete outputStreamPtr;

    orc::Type *schemaTypePtr = (orc::Type *)schemaType;
    if (schemaTypePtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for write schema type");
    }
    delete schemaTypePtr;

    delete writerPtr;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
