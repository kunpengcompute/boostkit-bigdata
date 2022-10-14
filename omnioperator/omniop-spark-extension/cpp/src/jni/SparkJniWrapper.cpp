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

#include <fcntl.h>
#include <unistd.h>

#include "../io/SparkFile.hh"
#include "../io/ColumnWriter.hh"
#include "../shuffle/splitter.h"
#include "jni_common.h"
#include "SparkJniWrapper.hh"
#include "concurrent_map.h"

static  jint JNI_VERSION = JNI_VERSION_1_8;

static jclass split_result_class;
static jclass runtime_exception_class;
static jclass excepiton_class;

static jmethodID split_result_constructor;

using namespace spark;
using namespace google::protobuf::io;
using namespace omniruntime::vec;

static ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    illegal_access_exception_class =
        CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");

    split_result_class =
        CreateGlobalClassReference(env, "Lcom/huawei/boostkit/spark/vectorized/SplitResult;");
    split_result_constructor = GetMethodID(env, split_result_class, "<init>", "(JJJJJ[J)V");

    runtime_exception_class = CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    env->DeleteGlobalRef(split_result_class);

    env->DeleteGlobalRef(runtime_exception_class);

    shuffle_splitter_holder_.Clear();
}

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jstring jInputType, jint jNumCols, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs,
    jstring local_dirs_jstr, jlong compress_block_size,
    jint spill_batch_row, jlong spill_memory_threshold) {
    JNI_FUNC_START
    if (partitioning_name_jstr == nullptr) {
        env->ThrowNew(runtime_exception_class,
                    std::string("Short partitioning name can't be null").c_str());
        return 0;
    }

    const char* inputTypeCharPtr = env->GetStringUTFChars(jInputType, JNI_FALSE);
    DataTypes inputVecTypes = Deserialize(inputTypeCharPtr);
    const int32_t *inputVecTypeIds = inputVecTypes.GetIds();
    //
    std::vector<DataTypePtr> inputDataTpyes =  inputVecTypes.Get();
    int32_t size = inputDataTpyes.size();
    uint32_t *inputDataPrecisions = new uint32_t[size];
    uint32_t *inputDataScales = new uint32_t[size];
    for (int i = 0; i < size; ++i) {
        if(inputDataTpyes[i]->GetId() == OMNI_DECIMAL64 || inputDataTpyes[i]->GetId() == OMNI_DECIMAL128) {
            inputDataScales[i] = std::dynamic_pointer_cast<DecimalDataType>(inputDataTpyes[i])->GetScale();
            inputDataPrecisions[i] = std::dynamic_pointer_cast<DecimalDataType>(inputDataTpyes[i])->GetPrecision();               
        }
    }
    inputDataTpyes.clear();

    InputDataTypes inputDataTypesTmp;
    inputDataTypesTmp.inputVecTypeIds = (int32_t *)inputVecTypeIds;
    inputDataTypesTmp.inputDataPrecisions = inputDataPrecisions;
    inputDataTypesTmp.inputDataScales = inputDataScales;

    if (data_file_jstr == nullptr) {
        env->ThrowNew(runtime_exception_class,
                    std::string("Shuffle DataFile can't be null").c_str());
        return 0;
    }
    if (local_dirs_jstr == nullptr) {
        env->ThrowNew(runtime_exception_class,
                    std::string("Shuffle DataFile can't be null").c_str());
        return 0;
    }

    auto partitioning_name_c = env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
    auto partitioning_name = std::string(partitioning_name_c);
    env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

    auto splitOptions = SplitOptions::Defaults();
    if (buffer_size > 0) {
        splitOptions.buffer_size = buffer_size;
    }
    if (num_sub_dirs > 0) {
        splitOptions.num_sub_dirs = num_sub_dirs;
    }
    if (compression_type_jstr != NULL) {
        auto compression_type_result = GetCompressionType(env, compression_type_jstr);
        splitOptions.compression_type = compression_type_result;
    }

    auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
    splitOptions.data_file = std::string(data_file_c);
    env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

    auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
    setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
    env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

    if (spill_batch_row > 0){
        splitOptions.spill_batch_row_num = spill_batch_row;
    }
    if (spill_memory_threshold > 0){
        splitOptions.spill_mem_threshold = spill_memory_threshold;
    }
    if (compress_block_size > 0){
        splitOptions.compress_block_size = compress_block_size;
    }

    jclass cls = env->FindClass("java/lang/Thread");
    jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
    jobject thread = env->CallStaticObjectMethod(cls, mid);
    if (thread == NULL) {
        std::cout << "Thread.currentThread() return NULL" <<std::endl;
    } else {
        jmethodID mid_getid = env->GetMethodID(cls, "getId", "()J");
        jlong sid = env->CallLongMethod(thread, mid_getid);
        splitOptions.thread_id = (int64_t)sid;
    }

    auto splitter = Splitter::Make(partitioning_name, inputDataTypesTmp, jNumCols, num_partitions, std::move(splitOptions));
    return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));
    JNI_FUNC_END(runtime_exception_class)
}

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_split(
    JNIEnv *env, jobject jObj, jlong splitter_id, jlong jVecBatchAddress) {
    JNI_FUNC_START
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        env->ThrowNew(runtime_exception_class, error_message.c_str());
        return -1;
    }

    auto vecBatch = (VectorBatch *) jVecBatchAddress;

    splitter->Split(*vecBatch);
    JNI_FUNC_END(runtime_exception_class)
}

JNIEXPORT jobject JNICALL
Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
    JNI_FUNC_START
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        env->ThrowNew(runtime_exception_class, error_message.c_str());
    }
    splitter->Stop();

    const auto& partition_length = splitter->PartitionLengths();
    auto partition_length_arr = env->NewLongArray(partition_length.size());
    auto src = reinterpret_cast<const jlong*>(partition_length.data());
    env->SetLongArrayRegion(partition_length_arr, 0, partition_length.size(), src);
    jobject split_result = env->NewObject(
        split_result_class, split_result_constructor, splitter->TotalComputePidTime(),
        splitter->TotalWriteTime(),  splitter->TotalSpillTime(),
        splitter->TotalBytesWritten(),  splitter->TotalBytesSpilled(), partition_length_arr);

    return split_result;
    JNI_FUNC_END(runtime_exception_class)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
    JNI_FUNC_START
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        env->ThrowNew(runtime_exception_class, error_message.c_str());
    }
    shuffle_splitter_holder_.Erase(splitter_id);
    JNI_FUNC_END(runtime_exception_class)
}
