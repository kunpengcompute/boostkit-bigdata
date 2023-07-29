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

#ifndef SPARK_THESTRAL_PLUGIN_PARQUETCOLUMNARBATCHJNIREADER_H
#define SPARK_THESTRAL_PLUGIN_PARQUETCOLUMNARBATCHJNIREADER_H

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <jni.h>
#include <json/json.h>
#include <ctime>
#include <vector/vector_common.h>
#include <util/omni_exception.h>
#include <arrow/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include "common/debug.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:       com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader
 * Method:      initializeReader
 * Signature:   (Ljava/lang/String;Lorg/json/simple/JSONObject;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_initializeReader
        (JNIEnv* env, jobject jObj, jobject job);

/*
 * Class:       com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader
 * Method:      recordReaderNext
 * Signature:   (J[I[J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_recordReaderNext
        (JNIEnv *, jobject, jlong, jintArray, jlongArray);

/*
 * Class:       com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader
 * Method:      recordReaderClose
 * Signature:   (J)F
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_ParquetColumnarBatchJniReader_recordReaderClose
        (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
