/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "OrcColumnarBatchJniReader.h"
#include <boost/algorithm/string.hpp>
#include "jni_common.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;
using namespace orc;

jclass runtimeExceptionClass;
jclass jsonClass;
jclass arrayListClass;
jmethodID jsonMethodInt;
jmethodID jsonMethodLong;
jmethodID jsonMethodHas;
jmethodID jsonMethodString;
jmethodID jsonMethodJsonObj;
jmethodID arrayListGet;
jmethodID arrayListSize;
jmethodID jsonMethodObj;

static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;

int initJniId(JNIEnv *env)
{
    /*
     * init table scan log
     */
    jsonClass = env->FindClass("org/json/JSONObject");
    arrayListClass = env->FindClass("java/util/ArrayList");

    arrayListGet = env->GetMethodID(arrayListClass, "get", "(I)Ljava/lang/Object;");
    arrayListSize = env->GetMethodID(arrayListClass, "size", "()I");

    // get int method
    jsonMethodInt = env->GetMethodID(jsonClass, "getInt", "(Ljava/lang/String;)I");
    if (jsonMethodInt == NULL)
        return -1;

    // get long method
    jsonMethodLong = env->GetMethodID(jsonClass, "getLong", "(Ljava/lang/String;)J");
    if (jsonMethodLong == NULL)
        return -1;

    // get has method
    jsonMethodHas = env->GetMethodID(jsonClass, "has", "(Ljava/lang/String;)Z");
    if (jsonMethodHas == NULL)
        return -1;

    // get string method
    jsonMethodString = env->GetMethodID(jsonClass, "getString", "(Ljava/lang/String;)Ljava/lang/String;");
    if (jsonMethodString == NULL)
        return -1;

    // get json object method
    jsonMethodJsonObj = env->GetMethodID(jsonClass, "getJSONObject", "(Ljava/lang/String;)Lorg/json/JSONObject;");
    if (jsonMethodJsonObj == NULL)
        return -1;

    // get json object method
    jsonMethodObj = env->GetMethodID(jsonClass, "get", "(Ljava/lang/String;)Ljava/lang/Object;");
    if (jsonMethodJsonObj == NULL)
        return -1;

    jclass local_class = env->FindClass("Ljava/lang/RuntimeException;");
    runtimeExceptionClass = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    if (runtimeExceptionClass == NULL)
        return -1;

    return 0;
}

void JNI_OnUnload(JavaVM *vm, const void *reserved)
{
    JNIEnv *env = nullptr;
    vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    env->DeleteGlobalRef(runtimeExceptionClass);
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeReader(JNIEnv *env,
    jobject jObj, jstring path, jobject jsonObj)
{
    JNI_FUNC_START
    /*
     * init logger and jni env method id
     */
    initJniId(env);

    /*
     * get tailLocation from json obj
     */
    jlong tailLocation = env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("tailLocation"));
    jstring serTailJstr =
        (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("serializedTail"));
    const char *pathPtr = env->GetStringUTFChars(path, nullptr);
    std::string filePath(pathPtr);
    orc::MemoryPool *pool = orc::getDefaultPool();
    orc::ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    readerOptions.setTailLocation(tailLocation);
    if (serTailJstr != NULL) {
        const char *ptr = env->GetStringUTFChars(serTailJstr, nullptr);
        std::string serTail(ptr);
        readerOptions.setSerializedFileTail(serTail);
        env->ReleaseStringUTFChars(serTailJstr, ptr);
    }

    std::unique_ptr<orc::Reader> reader = createReader(orc::readFile(filePath), readerOptions);
    env->ReleaseStringUTFChars(path, pathPtr);
    orc::Reader *readerNew = reader.release();
    return (jlong)(readerNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

bool StringToBool(const std::string &boolStr)
{
    if (boost::iequals(boolStr, "true")) {
        return true;
    } else if (boost::iequals(boolStr, "false")) {
        return false;
    } else {
        throw std::runtime_error("Invalid input for stringToBool.");
    }
}

int GetLiteral(orc::Literal &lit, int leafType, const std::string &value)
{
    switch ((orc::PredicateDataType)leafType) {
        case orc::PredicateDataType::LONG: {
            lit = orc::Literal(static_cast<int64_t>(std::stol(value)));
            break;
        }
        case orc::PredicateDataType::FLOAT: {
            lit = orc::Literal(static_cast<double>(std::stod(value)));
            break;
        }
        case orc::PredicateDataType::STRING: {
            lit = orc::Literal(value.c_str(), value.size());
            break;
        }
        case orc::PredicateDataType::DATE: {
            lit = orc::Literal(PredicateDataType::DATE, static_cast<int64_t>(std::stol(value)));
            break;
        }
        case orc::PredicateDataType::DECIMAL: {
            vector<std::string> valList;
            // Decimal(22, 6) eg: value ("19999999999998,998000 22 6")
            istringstream tmpAllStr(value);
            string tmpStr;
            while (tmpAllStr >> tmpStr) {
                valList.push_back(tmpStr);
            }
            Decimal decimalVal(valList[0]);
            lit = orc::Literal(decimalVal.value, static_cast<int32_t>(std::stoi(valList[1])),
                static_cast<int32_t>(std::stoi(valList[2])));
            break;
        }
        case orc::PredicateDataType::BOOLEAN: {
            lit = orc::Literal(static_cast<bool>(StringToBool(value)));
            break;
        }
        default: {
            throw std::runtime_error("tableScan jni getLiteral unsupported leafType: " + leafType);
        }
    }
    return 0;
}

int BuildLeaves(PredicateOperatorType leafOp, vector<Literal> &litList, Literal &lit, const std::string &leafNameString,
    PredicateDataType leafType, SearchArgumentBuilder &builder)
{
    switch (leafOp) {
        case PredicateOperatorType::LESS_THAN: {
            builder.lessThan(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::LESS_THAN_EQUALS: {
            builder.lessThanEquals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::EQUALS: {
            builder.equals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::NULL_SAFE_EQUALS: {
            builder.nullSafeEquals(leafNameString, leafType, lit);
            break;
        }
        case PredicateOperatorType::IS_NULL: {
            builder.isNull(leafNameString, leafType);
            break;
        }
        case PredicateOperatorType::IN: {
            builder.in(leafNameString, leafType, litList);
            break;
        }
        case PredicateOperatorType::BETWEEN: {
            throw std::runtime_error("table scan buildLeaves BETWEEN is not supported!");
        }
        default: {
            throw std::runtime_error("table scan buildLeaves illegal input!");
        }
    }
    return 1;
}

int initLeaves(JNIEnv *env, SearchArgumentBuilder &builder, jobject &jsonExp, jobject &jsonLeaves)
{
    jstring leaf = (jstring)env->CallObjectMethod(jsonExp, jsonMethodString, env->NewStringUTF("leaf"));
    jobject leafJsonObj = env->CallObjectMethod(jsonLeaves, jsonMethodJsonObj, leaf);
    jstring leafName = (jstring)env->CallObjectMethod(leafJsonObj, jsonMethodString, env->NewStringUTF("name"));
    std::string leafNameString(env->GetStringUTFChars(leafName, nullptr));
    jint leafOp = (jint)env->CallIntMethod(leafJsonObj, jsonMethodInt, env->NewStringUTF("op"));
    jint leafType = (jint)env->CallIntMethod(leafJsonObj, jsonMethodInt, env->NewStringUTF("type"));
    Literal lit(0L);
    jstring leafValue = (jstring)env->CallObjectMethod(leafJsonObj, jsonMethodString, env->NewStringUTF("literal"));
    if (leafValue != nullptr) {
        std::string leafValueString(env->GetStringUTFChars(leafValue, nullptr));
        if (leafValueString.size() != 0) {
            GetLiteral(lit, leafType, leafValueString);
        }
    }
    std::vector<Literal> litList;
    jobject litListValue = env->CallObjectMethod(leafJsonObj, jsonMethodObj, env->NewStringUTF("literalList"));
    if (litListValue != nullptr) {
        int childs = (int)env->CallIntMethod(litListValue, arrayListSize);
        for (int i = 0; i < childs; i++) {
            jstring child = (jstring)env->CallObjectMethod(litListValue, arrayListGet, i);
            std::string childString(env->GetStringUTFChars(child, nullptr));
            GetLiteral(lit, leafType, childString);
            litList.push_back(lit);
        }
    }
    BuildLeaves((PredicateOperatorType)leafOp, litList, lit, leafNameString, (PredicateDataType)leafType, builder);
    return 1;
}

int initExpressionTree(JNIEnv *env, SearchArgumentBuilder &builder, jobject &jsonExp, jobject &jsonLeaves)
{
    int op = env->CallIntMethod(jsonExp, jsonMethodInt, env->NewStringUTF("op"));
    if (op == (int)(Operator::LEAF)) {
        initLeaves(env, builder, jsonExp, jsonLeaves);
    } else {
        switch ((Operator)op) {
            case Operator::OR: {
                builder.startOr();
                break;
            }
            case Operator::AND: {
                builder.startAnd();
                break;
            }
            case Operator::NOT: {
                builder.startNot();
                break;
            }
            default: {
                throw std::runtime_error("tableScan jni initExpressionTree Unsupported op: " + op);
            }
        }
        jobject childList = env->CallObjectMethod(jsonExp, jsonMethodObj, env->NewStringUTF("child"));
        int childs = (int)env->CallIntMethod(childList, arrayListSize);
        for (int i = 0; i < childs; i++) {
            jobject child = env->CallObjectMethod(childList, arrayListGet, i);
            initExpressionTree(env, builder, child, jsonLeaves);
        }
        builder.end();
    }
    return 0;
}


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeRecordReader(JNIEnv *env,
    jobject jObj, jlong reader, jobject jsonObj)
{
    JNI_FUNC_START
    orc::Reader *readerPtr = (orc::Reader *)reader;
    // get offset from json obj
    jlong offset = env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("offset"));
    jlong length = env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("length"));
    jobjectArray includedColumns =
        (jobjectArray)env->CallObjectMethod(jsonObj, jsonMethodObj, env->NewStringUTF("includedColumns"));
    if (includedColumns == NULL)
        return -1;
    std::list<std::string> includedColumnsLenArray;
    jint arrLen = env->GetArrayLength(includedColumns);
    jboolean isCopy = JNI_FALSE;
    for (int i = 0; i < arrLen; i++) {
        jstring colName = (jstring)env->GetObjectArrayElement(includedColumns, i);
        const char *convertedValue = (env)->GetStringUTFChars(colName, &isCopy);
        std::string colNameString = convertedValue;
        includedColumnsLenArray.push_back(colNameString);
    }
    RowReaderOptions rowReaderOpts;
    if (arrLen != 0) {
        rowReaderOpts.include(includedColumnsLenArray);
    } else {
        std::list<uint64_t> includeFirstCol;
        includeFirstCol.push_back(0);
        rowReaderOpts.include(includeFirstCol);
    }
    rowReaderOpts.range(offset, length);

    jboolean hasExpressionTree = env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("expressionTree"));
    if (hasExpressionTree) {
        jobject expressionTree = env->CallObjectMethod(jsonObj, jsonMethodJsonObj, env->NewStringUTF("expressionTree"));
        jobject leaves = env->CallObjectMethod(jsonObj, jsonMethodJsonObj, env->NewStringUTF("leaves"));
        std::unique_ptr<SearchArgumentBuilder> builder = SearchArgumentFactory::newBuilder();
        initExpressionTree(env, *builder, expressionTree, leaves);
        auto sargBuilded = (*builder).build();
        rowReaderOpts.searchArgument(std::unique_ptr<SearchArgument>(sargBuilded.release()));
    }

    std::unique_ptr<orc::RowReader> rowReader = readerPtr->createRowReader(rowReaderOpts);
    return (jlong)(rowReader.release());
    JNI_FUNC_END(runtimeExceptionClass)
}


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeBatch(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batchSize)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)(rowReader);
    uint64_t batchLen = (uint64_t)batchSize;
    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReaderPtr->createRowBatch(batchLen);
    orc::ColumnVectorBatch *rtn = batch.release();
    return (jlong)rtn;
    JNI_FUNC_END(runtimeExceptionClass)
}

template <DataTypeId TYPE_ID, typename ORC_TYPE> uint64_t CopyFixedWidth(orc::ColumnVectorBatch *field)
{
    using T = typename NativeType<TYPE_ID>::type;
    ORC_TYPE *lvb = dynamic_cast<ORC_TYPE *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto originalVector = new Vector<T>(numElements);
    // Check ColumnVectorBatch has null or not firstly
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                originalVector->SetValue(i, (T)(values[i]));
            } else {
                originalVector->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            originalVector->SetValue(i, (T)(values[i]));
        }
    }
    return (uint64_t)originalVector;
}

template <DataTypeId TYPE_ID, typename ORC_TYPE> uint64_t CopyOptimizedForInt64(orc::ColumnVectorBatch *field)
{
    using T = typename NativeType<TYPE_ID>::type;
    ORC_TYPE *lvb = dynamic_cast<ORC_TYPE *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto originalVector = new Vector<T>(numElements);
    // Check ColumnVectorBatch has null or not firstly
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (!notNulls[i]) {
                originalVector->SetNull(i);
            }
        }
    }
    originalVector->SetValues(0, values, numElements);
    return (uint64_t)originalVector;
}

uint64_t CopyVarWidth(orc::ColumnVectorBatch *field)
{
    orc::StringVectorBatch *lvb = dynamic_cast<orc::StringVectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto lens = lvb->length.data();
    auto originalVector = new Vector<LargeStringContainer<std::string_view>>(numElements);
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                auto data = std::string_view(reinterpret_cast<const char *>(values[i]), lens[i]);
                originalVector->SetValue(i, data);
            } else {
                originalVector->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            auto data = std::string_view(reinterpret_cast<const char *>(values[i]), lens[i]);
            originalVector->SetValue(i, data);
        }
    }
    return (uint64_t)originalVector;
}

inline void FindLastNotEmpty(const char *chars, long &len)
{
    while (len > 0 && chars[len - 1] == ' ') {
        len--;
    }
}

uint64_t CopyCharType(orc::ColumnVectorBatch *field)
{
    orc::StringVectorBatch *lvb = dynamic_cast<orc::StringVectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto lens = lvb->length.data();
    auto originalVector = new Vector<LargeStringContainer<std::string_view>>(numElements);
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                auto chars = reinterpret_cast<const char *>(values[i]);
                auto len = lens[i];
                FindLastNotEmpty(chars, len);
                auto data = std::string_view(chars, len);
                originalVector->SetValue(i, data);
            } else {
                originalVector->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            auto chars = reinterpret_cast<const char *>(values[i]);
            auto len = lens[i];
            FindLastNotEmpty(chars, len);
            auto data = std::string_view(chars, len);
            originalVector->SetValue(i, data);
        }
    }
    return (uint64_t)originalVector;
}

inline void TransferDecimal128(int64_t &highbits, uint64_t &lowbits)
{
    if (highbits < 0) { // int128's 2s' complement code
        lowbits = ~lowbits + 1; // 2s' complement code
        highbits = ~highbits; //1s' complement code
        if (lowbits == 0) {
            highbits += 1; // carry a number as in adding
        }
        highbits ^= ((uint64_t)1 << 63);
    }
}

uint64_t CopyToOmniDecimal128Vec(orc::ColumnVectorBatch *field)
{
    orc::Decimal128VectorBatch *lvb = dynamic_cast<orc::Decimal128VectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    auto originalVector = new Vector<Decimal128>(numElements);
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                auto highbits = values[i].getHighBits();
                auto lowbits = values[i].getLowBits();
                TransferDecimal128(highbits, lowbits);
                Decimal128 d128(highbits, lowbits);
                originalVector->SetValue(i, d128);
            } else {
                originalVector->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            auto highbits = values[i].getHighBits();
            auto lowbits = values[i].getLowBits();
            TransferDecimal128(highbits, lowbits);
            Decimal128 d128(highbits, lowbits);
            originalVector->SetValue(i, d128);
        }
    }
    return (uint64_t)originalVector;
}

uint64_t CopyToOmniDecimal64Vec(orc::ColumnVectorBatch *field)
{
    orc::Decimal64VectorBatch *lvb = dynamic_cast<orc::Decimal64VectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    auto originalVector = new Vector<int64_t>(numElements);
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (!notNulls[i]) {
                originalVector->SetNull(i);
            }
        }
    }
    originalVector->SetValues(0, values, numElements);
    return (uint64_t)originalVector;
}

int CopyToOmniVec(const orc::Type *type, int &omniTypeId, uint64_t &omniVecId, orc::ColumnVectorBatch *field)
{
    switch (type->getKind()) {
        case orc::TypeKind::BOOLEAN:
            omniTypeId = static_cast<jint>(OMNI_BOOLEAN);
            omniVecId = CopyFixedWidth<OMNI_BOOLEAN, orc::LongVectorBatch>(field);
            break;
        case orc::TypeKind::SHORT:
            omniTypeId = static_cast<jint>(OMNI_SHORT);
            omniVecId = CopyFixedWidth<OMNI_SHORT, orc::LongVectorBatch>(field);
            break;
        case orc::TypeKind::DATE:
            omniTypeId = static_cast<jint>(OMNI_DATE32);
            omniVecId = CopyFixedWidth<OMNI_INT, orc::LongVectorBatch>(field);
            break;
        case orc::TypeKind::INT:
            omniTypeId = static_cast<jint>(OMNI_INT);
            omniVecId = CopyFixedWidth<OMNI_INT, orc::LongVectorBatch>(field);
            break;
        case orc::TypeKind::LONG:
            omniTypeId = static_cast<int>(OMNI_LONG);
            omniVecId = CopyOptimizedForInt64<OMNI_LONG, orc::LongVectorBatch>(field);
            break;
        case orc::TypeKind::DOUBLE:
            omniTypeId = static_cast<int>(OMNI_DOUBLE);
            omniVecId = CopyOptimizedForInt64<OMNI_DOUBLE, orc::DoubleVectorBatch>(field);
            break;
        case orc::TypeKind::CHAR:
            omniTypeId = static_cast<int>(OMNI_VARCHAR);
            omniVecId = CopyCharType(field);
            break;
        case orc::TypeKind::STRING:
        case orc::TypeKind::VARCHAR:
            omniTypeId = static_cast<int>(OMNI_VARCHAR);
            omniVecId = CopyVarWidth(field);
            break;
        case orc::TypeKind::DECIMAL:
            if (type->getPrecision() > MAX_DECIMAL64_DIGITS) {
                omniTypeId = static_cast<int>(OMNI_DECIMAL128);
                omniVecId = CopyToOmniDecimal128Vec(field);
            } else {
                omniTypeId = static_cast<int>(OMNI_DECIMAL64);
                omniVecId = CopyToOmniDecimal64Vec(field);
            }
            break;
        default: {
            throw std::runtime_error("Native ColumnarFileScan Not support For This Type: " + type->getKind());
        }
    }
    return 1;
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batch, jintArray typeId, jlongArray vecNativeId)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batch;
    const orc::Type &baseTp = rowReaderPtr->getSelectedType();
    int vecCnt = 0;
    long batchRowSize = 0;
    if (rowReaderPtr->next(*columnVectorBatch)) {
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(columnVectorBatch);
        vecCnt = root->fields.size();
        batchRowSize = root->fields[0]->numElements;
        for (int id = 0; id < vecCnt; id++) {
            auto type = baseTp.getSubtype(id);
            int omniTypeId = 0;
            uint64_t omniVecId = 0;
            CopyToOmniVec(type, omniTypeId, omniVecId, root->fields[id]);
            env->SetIntArrayRegion(typeId, id, 1, &omniTypeId);
            jlong omniVec = static_cast<jlong>(omniVecId);
            env->SetLongArrayRegion(vecNativeId, id, 1, &omniVec);
        }
    }
    return (jlong)batchRowSize;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetRowNumber
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetRowNumber(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    uint64_t rownum = rowReaderPtr->getRowNumber();
    return (jlong)rownum;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetProgress
 * Signature: (J)F
 */
JNIEXPORT jfloat JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetProgress(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    JNI_FUNC_START
    jfloat curProgress = 1;
    env->ThrowNew(runtimeExceptionClass, "recordReaderGetProgress is unsupported");
    return curProgress;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderClose
 * Signature: (J)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderClose(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong reader, jlong batchReader)
{
    JNI_FUNC_START
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batchReader;
    if (nullptr == columnVectorBatch) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for batch reader");
    }
    delete columnVectorBatch;
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    if (nullptr == rowReaderPtr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for row reader");
    }
    delete rowReaderPtr;
    orc::Reader *readerPtr = (orc::Reader *)reader;
    if (nullptr == readerPtr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for reader");
    }
    delete readerPtr;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderSeekToRow
 * Signature: (JJ)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderSeekToRow(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong rowNumber)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    rowReaderPtr->seekToRow((long)rowNumber);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}


JNIEXPORT jobjectArray JNICALL
Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getAllColumnNames(JNIEnv *env, jobject jObj, jlong reader)
{
    JNI_FUNC_START
    orc::Reader *readerPtr = (orc::Reader *)reader;
    int32_t cols = static_cast<int32_t>(readerPtr->getType().getSubtypeCount());
    jobjectArray ret =
        (jobjectArray)env->NewObjectArray(cols, env->FindClass("java/lang/String"), env->NewStringUTF(""));
    for (int i = 0; i < cols; i++) {
        env->SetObjectArrayElement(ret, i, env->NewStringUTF(readerPtr->getType().getFieldName(i).data()));
    }
    return ret;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getNumberOfRows(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batch)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batch;
    rowReaderPtr->next(*columnVectorBatch);
    jlong rows = columnVectorBatch->numElements;
    return rows;
    JNI_FUNC_END(runtimeExceptionClass)
}
