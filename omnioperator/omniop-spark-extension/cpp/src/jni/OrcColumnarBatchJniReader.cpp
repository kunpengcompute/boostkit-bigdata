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
using namespace omniruntime::vec;
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
}

int getLiteral(orc::Literal &lit, int leafType, string value)
{
    switch ((orc::PredicateDataType)leafType) {
        case orc::PredicateDataType::LONG: {
            lit = orc::Literal(static_cast<int64_t>(std::stol(value)));
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
        default: {
            LogsError("ERROR: TYPE ERROR: TYPEID");
        }
    }
    return 0;
}

int buildLeafs(int leafOp, vector<Literal> &litList, Literal &lit, string leafNameString, int leafType,
    SearchArgumentBuilder &builder)
{
    switch ((PredicateOperatorType)leafOp) {
        case PredicateOperatorType::LESS_THAN: {
            builder.lessThan(leafNameString, (PredicateDataType)leafType, lit);
            break;
        }
        case PredicateOperatorType::LESS_THAN_EQUALS: {
            builder.lessThanEquals(leafNameString, (PredicateDataType)leafType, lit);
            break;
        }
        case PredicateOperatorType::EQUALS: {
            builder.equals(leafNameString, (PredicateDataType)leafType, lit);
            break;
        }
        case PredicateOperatorType::NULL_SAFE_EQUALS: {
            builder.nullSafeEquals(leafNameString, (PredicateDataType)leafType, lit);
            break;
        }
        case PredicateOperatorType::IS_NULL: {
            builder.isNull(leafNameString, (PredicateDataType)leafType);
            break;
        }
        case PredicateOperatorType::IN: {
            builder.in(leafNameString, (PredicateDataType)leafType, litList);
            break;
        }
        default: {
            LogsError("ERROR operator ID");
        }
    }
    return 1;
}

int initLeafs(JNIEnv *env, SearchArgumentBuilder &builder, jobject &jsonExp, jobject &jsonLeaves)
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
            getLiteral(lit, leafType, leafValueString);
        }
    }
    std::vector<Literal> litList;
    jobject litListValue = env->CallObjectMethod(leafJsonObj, jsonMethodObj, env->NewStringUTF("literalList"));
    if (litListValue != nullptr) {
        int childs = (int)env->CallIntMethod(litListValue, arrayListSize);
        for (int i = 0; i < childs; i++) {
            jstring child = (jstring)env->CallObjectMethod(litListValue, arrayListGet, i);
            std::string childString(env->GetStringUTFChars(child, nullptr));
            getLiteral(lit, leafType, childString);
            litList.push_back(lit);
        }
    }
    buildLeafs((int)leafOp, litList, lit, leafNameString, (int)leafType, builder);
    return 1;
}

int initExpressionTree(JNIEnv *env, SearchArgumentBuilder &builder, jobject &jsonExp, jobject &jsonLeaves)
{
    int op = env->CallIntMethod(jsonExp, jsonMethodInt, env->NewStringUTF("op"));
    if (op == (int)(Operator::LEAF)) {
        initLeafs(env, builder, jsonExp, jsonLeaves);
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
}


JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeBatch(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batchSize)
{
    orc::RowReader *rowReaderPtr = (orc::RowReader *)(rowReader);
    uint64_t batchLen = (uint64_t)batchSize;
    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReaderPtr->createRowBatch(batchLen);
    orc::ColumnVectorBatch *rtn = batch.release();
    return (jlong)rtn;
}

template <DataTypeId TYPE_ID, typename ORC_TYPE> uint64_t copyFixwidth(orc::ColumnVectorBatch *field)
{
    VectorAllocator *allocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    using T = typename NativeType<TYPE_ID>::type;
    ORC_TYPE *lvb = dynamic_cast<ORC_TYPE *>(field);
    FixedWidthVector<TYPE_ID> *originalVector = new FixedWidthVector<TYPE_ID>(allocator, lvb->numElements);
    for (int i = 0; i < lvb->numElements; i++) {
        if (lvb->notNull.data()[i]) {
            originalVector->SetValue(i, (T)(lvb->data.data()[i]));
        } else {
            originalVector->SetValueNull(i);
        }
    }
    return (uint64_t)originalVector;
}


uint64_t copyVarwidth(int maxLen, orc::ColumnVectorBatch *field, int vcType)
{
    VectorAllocator *allocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    orc::StringVectorBatch *lvb = dynamic_cast<orc::StringVectorBatch *>(field);
    uint64_t totalLen =
        maxLen * (lvb->numElements) > lvb->getMemoryUsage() ? maxLen * (lvb->numElements) : lvb->getMemoryUsage();
    VarcharVector *originalVector = new VarcharVector(allocator, totalLen, lvb->numElements);
    for (int i = 0; i < lvb->numElements; i++) {
        if (lvb->notNull.data()[i]) {
            string tmpStr(reinterpret_cast<const char *>(lvb->data.data()[i]), lvb->length.data()[i]);
            if (vcType == orc::TypeKind::CHAR && tmpStr.back() == ' ') {
                tmpStr.erase(tmpStr.find_last_not_of(" ") + 1);
            }
            originalVector->SetValue(i, reinterpret_cast<const uint8_t *>(tmpStr.data()), tmpStr.length());
        } else {
            originalVector->SetValueNull(i);
        }
    }
    return (uint64_t)originalVector;
}

int copyToOminVec(int maxLen, int vcType, int &ominTypeId, uint64_t &ominVecId, orc::ColumnVectorBatch *field)
{
    switch (vcType) {
        case orc::TypeKind::DATE:
        case orc::TypeKind::INT: {
            if (vcType == orc::TypeKind::DATE) {
                ominTypeId = static_cast<jint>(OMNI_DATE32);
            } else {
                ominTypeId = static_cast<jint>(OMNI_INT);
            }
            ominVecId = copyFixwidth<OMNI_INT, orc::LongVectorBatch>(field);
            break;
        }
        case orc::TypeKind::LONG: {
            ominTypeId = static_cast<int>(OMNI_LONG);
            ominVecId = copyFixwidth<OMNI_LONG, orc::LongVectorBatch>(field);
            break;
        }
        case orc::TypeKind::CHAR:
        case orc::TypeKind::STRING:
        case orc::TypeKind::VARCHAR: {
            ominTypeId = static_cast<int>(OMNI_VARCHAR);
            ominVecId = (uint64_t)copyVarwidth(maxLen, field, vcType);
            break;
        }
        default: {
            LogsError("orc::TypeKind::UNKNOWN  ERROR %d", vcType);
        }
    }
    return 1;
}

int copyToOminDecimalVec(int vcType, int &ominTypeId, uint64_t &ominVecId, orc::ColumnVectorBatch *field)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    if (vcType > 18) {
        ominTypeId = static_cast<int>(OMNI_DECIMAL128);
        orc::Decimal128VectorBatch *lvb = dynamic_cast<orc::Decimal128VectorBatch *>(field);
        FixedWidthVector<OMNI_DECIMAL128> *originalVector =
            new FixedWidthVector<OMNI_DECIMAL128>(allocator, lvb->numElements);
        for (int i = 0; i < lvb->numElements; i++) {
            if (lvb->notNull.data()[i]) {
                bool wasNegative = false;
                int64_t highbits = lvb->values.data()[i].getHighBits();
                uint64_t lowbits = lvb->values.data()[i].getLowBits();
                uint64_t high = 0;
                uint64_t low = 0;
                if (highbits < 0) {
                    low = ~lowbits + 1;
                    high = static_cast<uint64_t>(~highbits);
                    if (low == 0) {
                        high += 1;
                    }
                    highbits = high | ((uint64_t)1 << 63);
                }
                Decimal128 d128(highbits, low);
                originalVector->SetValue(i, d128);
            } else {
                originalVector->SetValueNull(i);
            }
        }
        ominVecId = (uint64_t)originalVector;
    } else {
        ominTypeId = static_cast<int>(OMNI_DECIMAL64);
        orc::Decimal64VectorBatch *lvb = dynamic_cast<orc::Decimal64VectorBatch *>(field);
        FixedWidthVector<OMNI_LONG> *originalVector = new FixedWidthVector<OMNI_LONG>(allocator, lvb->numElements);
        for (int i = 0; i < lvb->numElements; i++) {
            if (lvb->notNull.data()[i]) {
                originalVector->SetValue(i, (int64_t)(lvb->values.data()[i]));
            } else {
                originalVector->SetValueNull(i);
            }
        }
        ominVecId = (uint64_t)originalVector;
    }
    return 1;
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderNext(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong reader, jlong batch, jintArray typeId, jlongArray vecNativeId)
{
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batch;
    orc::Reader *readerPtr = (orc::Reader *)reader;
    const orc::Type &baseTp = rowReaderPtr->getSelectedType();
    int vecCnt = 0;
    long batchRowSize = 0;
    if (rowReaderPtr->next(*columnVectorBatch)) {
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(columnVectorBatch);
        vecCnt = root->fields.size();
        batchRowSize = root->fields[0]->numElements;
        for (int id = 0; id < vecCnt; id++) {
            int vcType = baseTp.getSubtype(id)->getKind();
            int maxLen = baseTp.getSubtype(id)->getMaximumLength();
            int ominTypeId = 0;
            uint64_t ominVecId = 0;
            try {
                if (vcType != orc::TypeKind::DECIMAL) {
                    copyToOminVec(maxLen, vcType, ominTypeId, ominVecId, root->fields[id]);
                } else {
                    copyToOminDecimalVec(baseTp.getSubtype(id)->getPrecision(), ominTypeId, ominVecId,
                        root->fields[id]);
                }
            } catch (omniruntime::exception::OmniException &e) {
                env->ThrowNew(runtimeExceptionClass, e.what());
                return (jlong)batchRowSize;
            }
            env->SetIntArrayRegion(typeId, id, 1, &ominTypeId);
            jlong ominVec = static_cast<jlong>(ominVecId);
            env->SetLongArrayRegion(vecNativeId, id, 1, &ominVec);
        }
    }
    return (jlong)batchRowSize;
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetRowNumber
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetRowNumber(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    uint64_t rownum = rowReaderPtr->getRowNumber();
    return (jlong)rownum;
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderGetProgress
 * Signature: (J)F
 */
JNIEXPORT jfloat JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetProgress(
    JNIEnv *env, jobject jObj, jlong rowReader)
{
    jfloat curProgress = 1;
    throw std::runtime_error("recordReaderGetProgress is unsupported");
    return curProgress;
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderClose
 * Signature: (J)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderClose(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong reader, jlong batchReader)
{
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batchReader;
    if (nullptr == columnVectorBatch) {
        throw std::runtime_error("delete nullptr error for batch reader");
    }
    delete columnVectorBatch;
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    if (nullptr == rowReaderPtr) {
        throw std::runtime_error("delete nullptr error for row reader");
    }
    delete rowReaderPtr;
    orc::Reader *readerPtr = (orc::Reader *)reader;
    if (nullptr == readerPtr) {
        throw std::runtime_error("delete nullptr error for reader");
    }
    delete readerPtr;
}

/*
 * Class:     com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:    recordReaderSeekToRow
 * Signature: (JJ)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderSeekToRow(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong rowNumber)
{
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    rowReaderPtr->seekToRow((long)rowNumber);
}


JNIEXPORT jobjectArray JNICALL
Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getAllColumnNames(JNIEnv *env, jobject jObj, jlong reader)
{
    orc::Reader *readerPtr = (orc::Reader *)reader;
    int32_t cols = static_cast<int32_t>(readerPtr->getType().getSubtypeCount());
    jobjectArray ret =
        (jobjectArray)env->NewObjectArray(cols, env->FindClass("java/lang/String"), env->NewStringUTF(""));
    for (int i = 0; i < cols; i++) {
        env->SetObjectArrayElement(ret, i, env->NewStringUTF(readerPtr->getType().getFieldName(i).data()));
    }
    return ret;
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getNumberOfRows(JNIEnv *env,
    jobject jObj, jlong rowReader, jlong batch)
{
    orc::RowReader *rowReaderPtr = (orc::RowReader *)rowReader;
    orc::ColumnVectorBatch *columnVectorBatch = (orc::ColumnVectorBatch *)batch;
    rowReaderPtr->next(*columnVectorBatch);
    jlong rows = columnVectorBatch->numElements;
    return rows;
}