/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

/* Header for class THESTRAL_PLUGIN_ORCCOLUMNARBATCHJNIREADER_H */

#ifndef THESTRAL_PLUGIN_ORCCOLUMNARBATCHJNIREADER_H
#define THESTRAL_PLUGIN_ORCCOLUMNARBATCHJNIREADER_H

#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Reader.hh"
#include "orc/OrcFile.hh"
#include "orc/MemoryPool.hh"
#include "orc/sargs/SearchArgument.hh"
#include "orc/sargs/Literal.hh"
#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>
#include <stdio.h>
#include "jni.h"
#include "json/json.h"
#include "vector/vector_common.h"
#include "util/omni_exception.h"
#include <time.h>
#include <sstream>
#include "../common/debug.h"

#ifdef __cplusplus
extern "C" {
#endif

enum class Operator {
    OR,
    AND,
    NOT,
    LEAF,
    CONSTANT
};

enum class PredicateOperatorType {
    EQUALS = 0,
    NULL_SAFE_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS, IN, BETWEEN, IS_NULL
};

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      initializeReader
 * Signature:   (Ljava/lang/String;Lorg/json/simple/JSONObject;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeReader
        (JNIEnv* env, jobject jObj, jstring path, jobject job);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      initializeRecordReader
 * Signature:   (JLorg/json/simple/JSONObject;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeRecordReader
        (JNIEnv* env, jobject jObj, jlong reader, jobject job);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      initializeRecordReader
 * Signature:   (JLorg/json/simple/JSONObject;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_initializeBatch
        (JNIEnv* env, jobject jObj, jlong rowReader, jlong batchSize);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      recordReaderNext
 * Signature:   (J[I[J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderNext
        (JNIEnv *, jobject, jlong, jlong, jlong, jintArray, jlongArray);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      recordReaderGetRowNumber
 * Signature:   (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetRowNumber
        (JNIEnv *, jobject, jlong);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      recordReaderGetProgress
 * Signature:   (J)F
 */
JNIEXPORT jfloat JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderGetProgress
        (JNIEnv *, jobject, jlong);


/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      recordReaderClose
 * Signature:   (J)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderClose
        (JNIEnv *, jobject, jlong, jlong, jlong);

/*
 * Class:       come_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader
 * Method:      recordReaderSeekToRow
 * Signature:   (JJ)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_recordReaderSeekToRow
        (JNIEnv *, jobject, jlong, jlong);

JNIEXPORT jobjectArray JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getAllColumnNames
        (JNIEnv *, jobject, jlong);

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_OrcColumnarBatchJniReader_getNumberOfRows(JNIEnv *env,
        jobject jObj, jlong rowReader, jlong batch);

int getLiteral(orc::Literal &lit, int leafType, std::string value);      

int buildLeafs(int leafOp, std::vector<orc::Literal> &litList, orc::Literal &lit, std::string leafNameString, int leafType,
    orc::SearchArgumentBuilder &builder);

int copyToOminVec(int maxLen, int vcType, int &ominTypeId, uint64_t &ominVecId, orc::ColumnVectorBatch *field);  

#ifdef __cplusplus
}
#endif
#endif
