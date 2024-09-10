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
#include "common/JulianGregorianRebase.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace std;
using namespace orc;

static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;
uint64_t batchLen = 0;

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

    jint port = (jint)env->CallIntMethod(jsonObj, jsonMethodInt, env->NewStringUTF("port"));

    std::unique_ptr<orc::Reader> reader;
    UriInfo uri{schemaStr, fileStr, hostStr, std::to_string(port)};
    reader = omniruntime::reader::omniCreateReader(orc::readFileOverride(uri), readerOptions);
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
        case orc::PredicateDataType::TIMESTAMP: {
            lit = orc::Literal(PredicateDataType::TIMESTAMP, static_cast<int64_t>(std::stol(value)));
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
    auto *readerPtr = (omniruntime::reader::OmniReaderImpl *)reader;

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

    std::unique_ptr<common::JulianGregorianRebase> julianPtr = common::BuildJulianGregorianRebase(env, jsonObj);

    std::unique_ptr<orc::RowReader> rowReader = readerPtr->createRowReader(rowReaderOpts, julianPtr);
    return (jlong)(rowReader.release());
    JNI_FUNC_END(runtimeExceptionClass)
}


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_initializeBatch(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batchSize)
{
    JNI_FUNC_START
    batchLen = (uint64_t)batchSize;
    return (jlong)0xffff;
    JNI_FUNC_END(runtimeExceptionClass)
}

inline void FindLastNotEmpty(const char *chars, long &len)
{
    while (len > 0 && chars[len - 1] == ' ') {
        len--;
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batch, jintArray typeId, jlongArray vecNativeId)
{
    JNI_FUNC_START
    omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl*)rowReader;
    if (rowReaderPtr == nullptr) {
        throw std::runtime_error("rowReaderPtr is nullptr!!!");
    }

    const orc::Type &baseTp = rowReaderPtr->getSelectedType();
    uint64_t batchRowSize = 0;
    auto ptr = env->GetIntArrayElements(typeId, JNI_FALSE);
    if (ptr == NULL) {
        throw std::runtime_error("Types should not be null");
    }
    int32_t arrLen = (int32_t) env->GetArrayLength(typeId);
    VectorBatch* omniVecBatch = new VectorBatch(batchLen);

    if (rowReaderPtr->next(omniVecBatch, ptr)) {
        batchRowSize = omniVecBatch->GetRowCount();
        int32_t vecCnt = omniVecBatch->GetVectorCount();
        if (vecCnt != arrLen) {
            throw std::runtime_error("Types should align to root fields");
        }

        for (int32_t id = 0; id < vecCnt; id++) {
            auto vec = omniVecBatch->GetVectors();
            jlong omniVec = reinterpret_cast<jlong>((vec[id]));
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
    omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl *)rowReader;
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
    omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl *)rowReader;
    if (nullptr == rowReaderPtr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for row reader");
    }
    delete rowReaderPtr;
    omniruntime::reader::OmniReaderImpl *readerPtr = (omniruntime::reader::OmniReaderImpl *)reader;
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
    omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl *)rowReader;
    rowReaderPtr->seekToRow((long)rowNumber);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}


JNIEXPORT jobjectArray JNICALL
Java_com_huawei_boostkit_scan_jni_OrcColumnarBatchJniReader_getAllColumnNames(JNIEnv *env, jobject jObj, jlong reader)
{
    JNI_FUNC_START
    omniruntime::reader::OmniReaderImpl *readerPtr = (omniruntime::reader::OmniReaderImpl *)reader;
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
    omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl *)rowReader;
    VectorBatch* omniVecBatch = new VectorBatch(batchLen);
    rowReaderPtr->next(omniVecBatch, nullptr);
    jlong rows = omniVecBatch->GetRowCount();
    return rows;
    JNI_FUNC_END(runtimeExceptionClass)
}
