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

#ifndef OMNI_RUNTIME_PARQUETCOLUMNARBATCHJNIWRITER_H
#define OMNI_RUNTIME_PARQUETCOLUMNARBATCHJNIWRITER_H

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <jni.h>
#include <json/json.h>
#include <vector/vector_common.h>
#include <util/omni_exception.h>
#include "common/debug.h"

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Class:       com_huawei_boostkit_writer_jni_ParquetColumnarBatchJniWriter
 * Method:      initializeWriter
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeWriter
    (JNIEnv *env, jobject jObj, jobject job, jlong writer);

/*
 * Class:       com_huawei_boostkit_writer_jni_ParquetColumnarBatchJniWriter
 * Method:      initializeSchema
 * Signature:
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeSchema
    (JNIEnv *env, jobject jObj, jlong writer, jobjectArray filedNames, jintArray fieldTypes,
    jbooleanArray nullables, jobjectArray decimalParam);

/*
 * Class:       com_huawei_boostkit_writer_jni_ParquetColumnarBatchJniWriter
 * Method:      write
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId,
    jintArray omniTypes, jbooleanArray dataColumnsIds, jint numRows);

/*
 * Class:       com_huawei_boostkit_writer_jni_ParquetColumnarBatchJniWriter
 * Method:      splitWrite
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes,
    jbooleanArray dataColumnsIds, jlong startPos, jlong endPos);

/*
 * Class:       com_huawei_boostkit_writer_jni_ParquetColumnarBatchJniWriter
 * Method:      close
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
     jlong writer);

#ifdef __cplusplus
}
#endif
#endif
