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

#include "OrcColumnarBatchJniReader.h"
#include <boost/algorithm/string.hpp>
#include <memory>
#include "jni_common.h"
#include "common/UriInfo.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;
using namespace orc;

static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;

// vecFildsNames存储文件每列的列名，从orc reader c++侧获取，回传到java侧使用
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_initializeReader(JNIEnv *env,
    jobject jObj, jobject jsonObj, jobject vecFildsNames)
{
    JNI_FUNC_START

    /*
     * get tailLocation from json obj
     */
    jlong tailLocation = env->CallLongMethod(jsonObj, jsonMethodLong, env->NewStringUTF("tailLocation"));
    jstring serTailJstr =
        (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("serializedTail"));

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

    jstring schemaJstr = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("scheme"));
    const char *schemaPtr = env->GetStringUTFChars(schemaJstr, nullptr);
    std::string schemaStr(schemaPtr);
    env->ReleaseStringUTFChars(schemaJstr, schemaPtr);

    jstring fileJstr = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("path"));
    const char *filePtr = env->GetStringUTFChars(fileJstr, nullptr);
    std::string fileStr(filePtr);
    env->ReleaseStringUTFChars(fileJstr, filePtr);

    jstring hostJstr = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("host"));
    const char *hostPtr = env->GetStringUTFChars(hostJstr, nullptr);
    std::string hostStr(hostPtr);
    env->ReleaseStringUTFChars(hostJstr, hostPtr);

    jstring portJstr = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("port"));
    const char *portPtr = env->GetStringUTFChars(portJstr, nullptr);
    std::string portStr(portPtr);
    env->ReleaseStringUTFChars(portJstr, portPtr);

    std::unique_ptr<orc::Reader> reader;
    UriInfo uri{schemaStr, fileStr, hostStr, portStr};
    jboolean notNeedFSCache = env->CallBooleanMethod(jsonObj, jsonMethodHas,
            env->NewStringUTF("notNeedFSCache"));
    reader = createReader(orc::readFileOverride(uri, notNeedFSCache), readerOptions);
    std::vector<std::string> orcColumnNames = reader->getAllFiedsName();
    for (uint32_t i = 0; i < orcColumnNames.size(); i++) {
        jstring fildname = env->NewStringUTF(orcColumnNames[i].c_str());
        // use ArrayList and function
        env->CallBooleanMethod(vecFildsNames, arrayListAdd, fildname);
        env->DeleteLocalRef(fildname);
    }

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
            if (litList.empty()) {
                // build.in方法第一个参数给定空值，即会认为该predictLeaf的TruthValue为YES_NO_NULL（不过滤数据）
                // 即与java orc in中存在null的行为保持一致
                std::string emptyString;
                builder.in(emptyString, leafType, litList);
            } else {
                builder.in(leafNameString, leafType, litList);
            }
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
        const char *leafChars = env->GetStringUTFChars(leafValue, nullptr);
        std::string leafValueString(leafChars);
        env->ReleaseStringUTFChars(leafValue, leafChars);
        if (leafValueString.size() != 0 || (leafValueString.size() == 0 && (orc::PredicateDataType)leafType == orc::PredicateDataType::STRING)) {
            GetLiteral(lit, leafType, leafValueString);
        }
    }
    std::vector<Literal> litList;
    jobject litListValue = env->CallObjectMethod(leafJsonObj, jsonMethodObj, env->NewStringUTF("literalList"));
    if (litListValue != nullptr) {
        int childs = (int)env->CallIntMethod(litListValue, arrayListSize);
        for (int i = 0; i < childs; i++) {
            jstring child = (jstring) env->CallObjectMethod(litListValue, arrayListGet, i);
            if (child == nullptr) {
                // 原生spark-sql PredicateLiteralList如果含有null元素，会捕获NPE，然后产生TruthValue.YES_NO或者TruthValue.YES_NO_NULL
                // 这两者TruthValue在谓词下推都不会过滤该行组的数据
                // 此处将litList清空，作为BuildLeaves的标志，Build时传入相应参数产生上述TruthValue，使表现出的特性与原生保持一致
                litList.clear();
                break;
            } else {
                auto chars = env->GetStringUTFChars(child, nullptr);
                std::string childString(chars);
                env->ReleaseStringUTFChars(child, chars);
                GetLiteral(lit, leafType, childString);
                litList.push_back(lit);
            }
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


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_initializeRecordReader(JNIEnv *env,
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


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_initializeBatch(JNIEnv *env,
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

template <DataTypeId TYPE_ID, typename ORC_TYPE>
std::unique_ptr<BaseVector> CopyFixedWidth(orc::ColumnVectorBatch *field)
{
    using T = typename NativeType<TYPE_ID>::type;
    ORC_TYPE *lvb = dynamic_cast<ORC_TYPE *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto newVector = std::make_unique<Vector<T>>(numElements);
    auto newVectorPtr = newVector.get();
    // Check ColumnVectorBatch has null or not firstly
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                newVectorPtr->SetValue(i, (T)(values[i]));
            } else {
                newVectorPtr->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            newVectorPtr->SetValue(i, (T)(values[i]));
        }
    }
    return newVector;
}

template <DataTypeId TYPE_ID, typename ORC_TYPE>
std::unique_ptr<BaseVector> CopyOptimizedForInt64(orc::ColumnVectorBatch *field)
{
    using T = typename NativeType<TYPE_ID>::type;
    ORC_TYPE *lvb = dynamic_cast<ORC_TYPE *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto newVector = std::make_unique<Vector<T>>(numElements);
    auto newVectorPtr = newVector.get();
    // Check ColumnVectorBatch has null or not firstly
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (!notNulls[i]) {
                newVectorPtr->SetNull(i);
            }
        }
    }
    newVectorPtr->SetValues(0, values, numElements);
    return newVector;
}

std::unique_ptr<BaseVector> CopyVarWidth(orc::ColumnVectorBatch *field)
{
    orc::StringVectorBatch *lvb = dynamic_cast<orc::StringVectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto lens = lvb->length.data();
    auto newVector = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numElements);
    auto newVectorPtr = newVector.get();
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                auto data = std::string_view(reinterpret_cast<const char *>(values[i]), lens[i]);
                newVectorPtr->SetValue(i, data);
            } else {
                newVectorPtr->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            auto data = std::string_view(reinterpret_cast<const char *>(values[i]), lens[i]);
            newVectorPtr->SetValue(i, data);
        }
    }
    return newVector;
}

inline void FindLastNotEmpty(const char *chars, long &len)
{
    while (len > 0 && chars[len - 1] == ' ') {
        len--;
    }
}

std::unique_ptr<BaseVector> CopyCharType(orc::ColumnVectorBatch *field)
{
    orc::StringVectorBatch *lvb = dynamic_cast<orc::StringVectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->data.data();
    auto notNulls = lvb->notNull.data();
    auto lens = lvb->length.data();
    auto newVector = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(numElements);
    auto newVectorPtr = newVector.get();
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                auto chars = reinterpret_cast<const char *>(values[i]);
                auto len = lens[i];
                FindLastNotEmpty(chars, len);
                auto data = std::string_view(chars, len);
                newVectorPtr->SetValue(i, data);
            } else {
                newVectorPtr->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            auto chars = reinterpret_cast<const char *>(values[i]);
            auto len = lens[i];
            FindLastNotEmpty(chars, len);
            auto data = std::string_view(chars, len);
            newVectorPtr->SetValue(i, data);
        }
    }
    return newVector;
}

std::unique_ptr<BaseVector> CopyToOmniDecimal128Vec(orc::ColumnVectorBatch *field)
{
    orc::Decimal128VectorBatch *lvb = dynamic_cast<orc::Decimal128VectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    auto newVector = std::make_unique<Vector<Decimal128>>(numElements);
    auto newVectorPtr = newVector.get();
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (notNulls[i]) {
                __int128_t dst = values[i].getHighBits();
                dst <<= 64;
                dst |= values[i].getLowBits();
                newVectorPtr->SetValue(i, Decimal128(dst));
            } else {
                newVectorPtr->SetNull(i);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            newVectorPtr->SetValue(i, Decimal128(values[i].getHighBits(), values[i].getLowBits()));
        }
    }
    return newVector;
}

std::unique_ptr<BaseVector> CopyToOmniDecimal64Vec(orc::ColumnVectorBatch *field)
{
    orc::Decimal64VectorBatch *lvb = dynamic_cast<orc::Decimal64VectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    auto newVector = std::make_unique<Vector<int64_t>>(numElements);
    auto newVectorPtr = newVector.get();
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (!notNulls[i]) {
                newVectorPtr->SetNull(i);
            }
        }
    }
    newVectorPtr->SetValues(0, values, numElements);
    return newVector;
}

std::unique_ptr<BaseVector> CopyToOmniDecimal128VecFrom64(orc::ColumnVectorBatch *field)
{
    orc::Decimal64VectorBatch *lvb = dynamic_cast<orc::Decimal64VectorBatch *>(field);
    auto numElements = lvb->numElements;
    auto values = lvb->values.data();
    auto notNulls = lvb->notNull.data();
    auto newVector = std::make_unique<Vector<Decimal128>>(numElements);
    auto newVectorPtr = newVector.get();
    if (lvb->hasNulls) {
        for (uint i = 0; i < numElements; i++) {
            if (!notNulls[i]) {
                newVectorPtr->SetNull(i);
            } else {
                Decimal128 d128(values[i]);
                newVectorPtr->SetValue(i, d128);
            }
        }
    } else {
        for (uint i = 0; i < numElements; i++) {
            Decimal128 d128(values[i]);
            newVectorPtr->SetValue(i, d128);
        }
    }

    return newVector;
}

std::unique_ptr<BaseVector> DealLongVectorBatch(DataTypeId id, orc::ColumnVectorBatch *field) {
    switch (id) {
        case omniruntime::type::OMNI_BOOLEAN:
            return CopyFixedWidth<OMNI_BOOLEAN, orc::LongVectorBatch>(field);
        case omniruntime::type::OMNI_SHORT:
            return CopyFixedWidth<OMNI_SHORT, orc::LongVectorBatch>(field);
        case omniruntime::type::OMNI_INT:
            return CopyFixedWidth<OMNI_INT, orc::LongVectorBatch>(field);
        case omniruntime::type::OMNI_LONG:
            return CopyOptimizedForInt64<OMNI_LONG, orc::LongVectorBatch>(field);
        case omniruntime::type::OMNI_DATE32:
            return CopyFixedWidth<OMNI_DATE32, orc::LongVectorBatch>(field);
        case omniruntime::type::OMNI_DATE64:
            return CopyOptimizedForInt64<OMNI_DATE64, orc::LongVectorBatch>(field);
        default: {
            throw std::runtime_error("DealLongVectorBatch not support for type: " + id);
        }
    }
}

std::unique_ptr<BaseVector> DealDoubleVectorBatch(DataTypeId id, orc::ColumnVectorBatch *field) {
    switch (id) {
        case omniruntime::type::OMNI_DOUBLE:
            return CopyOptimizedForInt64<OMNI_DOUBLE, orc::DoubleVectorBatch>(field);
        default: {
            throw std::runtime_error("DealDoubleVectorBatch not support for type: " + id);
        }
    }
}

std::unique_ptr<BaseVector> DealDecimal64VectorBatch(DataTypeId id, orc::ColumnVectorBatch *field) {
    switch (id) {
        case omniruntime::type::OMNI_DECIMAL64:
            return CopyToOmniDecimal64Vec(field);
        case omniruntime::type::OMNI_DECIMAL128:
            return CopyToOmniDecimal128VecFrom64(field);
        default: {
            throw std::runtime_error("DealDecimal64VectorBatch not support for type: " + id);
        }
    }
}

std::unique_ptr<BaseVector> DealDecimal128VectorBatch(DataTypeId id, orc::ColumnVectorBatch *field) {
    switch (id) {
        case omniruntime::type::OMNI_DECIMAL128:
            return CopyToOmniDecimal128Vec(field);
        default: {
            throw std::runtime_error("DealDecimal128VectorBatch not support for type: " + id);
        }
    }
}

std::unique_ptr<BaseVector> CopyToOmniVec(const orc::Type *type, int omniTypeId, orc::ColumnVectorBatch *field)
{
    DataTypeId dataTypeId = static_cast<DataTypeId>(omniTypeId);
    switch (type->getKind()) {
        case orc::TypeKind::BOOLEAN:
        case orc::TypeKind::SHORT:
        case orc::TypeKind::DATE:
        case orc::TypeKind::INT:
        case orc::TypeKind::LONG:
            return DealLongVectorBatch(dataTypeId, field);
        case orc::TypeKind::DOUBLE:
            return DealDoubleVectorBatch(dataTypeId, field);
        case orc::TypeKind::CHAR:
            if (dataTypeId != OMNI_VARCHAR) {
                throw std::runtime_error("Cannot transfer to other OMNI_TYPE but VARCHAR for orc char");
            }
            return CopyCharType(field);
        case orc::TypeKind::STRING:
        case orc::TypeKind::VARCHAR:
            if (dataTypeId != OMNI_VARCHAR) {
                throw std::runtime_error("Cannot transfer to other OMNI_TYPE but VARCHAR for orc string/varchar");
            }
            return CopyVarWidth(field);
        case orc::TypeKind::DECIMAL:
            if (type->getPrecision() > MAX_DECIMAL64_DIGITS) {
                return DealDecimal128VectorBatch(dataTypeId, field);
            } else {
                return DealDecimal64VectorBatch(dataTypeId, field);
            }
        default: {
            throw std::runtime_error("Native ColumnarFileScan Not support For This Type: " + type->getKind());
        }
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batch, jintArray typeId, jlongArray vecNativeId)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batch;
    std::vector<std::unique_ptr<BaseVector>> omniVecs;

    const orc::Type &baseTp = rowReaderPtr->getSelectedType();
    uint64_t batchRowSize = 0;
    auto ptr = env->GetIntArrayElements(typeId, JNI_FALSE);
    if (ptr == NULL) {
        throw std::runtime_error("Types should not be null");
    }
    int32_t arrLen = (int32_t) env->GetArrayLength(typeId);
    if (rowReaderPtr->next(*columnVectorBatch)) {
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(columnVectorBatch);
        batchRowSize = root->fields[0]->numElements;
        int32_t vecCnt = root->fields.size();
        if (vecCnt > arrLen) {
            throw std::runtime_error("Types should align to root fields");
        }
        for (int32_t id = 0; id < vecCnt; id++) {
            auto type = baseTp.getSubtype(id);
            int omniTypeId = ptr[id];
            omniVecs.emplace_back(CopyToOmniVec(type, omniTypeId, root->fields[id]));
        }
        for (int32_t id = 0; id < vecCnt; id++) {
            jlong omniVec = reinterpret_cast<jlong>(omniVecs[id].release());
            env->SetLongArrayRegion(vecNativeId, id, 1, &omniVec);
        }
    }
    return (jlong) batchRowSize;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetRowNumber
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderGetRowNumber(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    uint64_t rownum = rowReaderPtr->getRowNumber();
    return (jlong)rownum;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetProgress
 * Signature: (J)F
 */
JNIEXPORT jfloat JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderGetProgress(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    JNI_FUNC_START
    jfloat curProgress = 1;
    env->ThrowNew(runtimeExceptionClass, "recordReaderGetProgress is unsupported");
    return curProgress;
    JNI_FUNC_END(runtimeExceptionClass)
}

/*
 * Class:     com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderClose
 * Signature: (J)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderClose(JNIEnv *env,
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
 * Class:     com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderSeekToRow
 * Signature: (JJ)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderSeekToRow(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong rowNumber)
{
    JNI_FUNC_START
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    rowReaderPtr->seekToRow((long)rowNumber);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}


JNIEXPORT jobjectArray JNICALL
Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_getAllColumnNames(JNIEnv *env, jobject jObj, jlong reader)
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

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_getNumberOfRows(JNIEnv *env,
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
