/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "type/data_type_serializer.h"
#include "sdk/ock_shuffle_sdk.h"
#include "common/common.h"
#include "concurrent_map.h"
#include "jni_common.h"
#include "shuffle/ock_splitter.h"
#include "OckShuffleJniWriter.h"

using namespace ock::dopspark;

static jclass gSplitResultClass;
static jmethodID gSplitResultConstructor;

static ConcurrentMap<std::shared_ptr<OckSplitter>> gOckSplitterMap;
static const char *exceptionClass = "java/lang/Exception";

JNIEXPORT jboolean JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_initialize(JNIEnv *env, jobject)
{
    gSplitResultClass = CreateGlobalClassReference(env, "Lcom/huawei/boostkit/spark/vectorized/SplitResult;");
    gSplitResultConstructor = GetMethodID(env, gSplitResultClass, "<init>", "(JJJJJ[J)V");

    if (UNLIKELY(!OckShuffleSdk::Initialize())) {
        std::cout << "Failed to load ock shuffle library." << std::endl;
        return JNI_FALSE;
    }

    return JNI_TRUE;
}

JNIEXPORT jlong JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_nativeMake(JNIEnv *env, jobject,
    jstring jAppId, jint jShuffleId, jint jStageId, jint jStageAttemptNum, jint jMapId, jlong jTaskAttemptId,
    jstring jPartitioningMethod, jint jPartitionNum, jstring jColTypes, jint jColNum, jint jRegionSize,
    jint jMinCapacity, jint jMaxCapacity, jboolean jIsCompress)
{
    auto appIdStr = env->GetStringUTFChars(jAppId, JNI_FALSE);
    if (UNLIKELY(appIdStr == nullptr)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("ApplicationId can't be empty").c_str());
    }
    auto appId = std::string(appIdStr);
    env->ReleaseStringUTFChars(jAppId, appIdStr);

    auto partitioningMethodStr = env->GetStringUTFChars(jPartitioningMethod, JNI_FALSE);
    if (UNLIKELY(partitioningMethodStr == nullptr)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("Partitioning method can't be empty").c_str());
    }
    auto partitionMethod = std::string(partitioningMethodStr);
    env->ReleaseStringUTFChars(jPartitioningMethod, partitioningMethodStr);

    auto colTypesStr = env->GetStringUTFChars(jColTypes, JNI_FALSE);
    if (UNLIKELY(colTypesStr == nullptr)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("Columns types can't be empty").c_str());
    }

    DataTypes colTypes = Deserialize(colTypesStr);
    env->ReleaseStringUTFChars(jColTypes, colTypesStr);

    jlong jThreadId = 0L;
    jclass jThreadCls = env->FindClass("java/lang/Thread");
    jmethodID jMethodId = env->GetStaticMethodID(jThreadCls, "currentThread", "()Ljava/lang/Thread;");
    jobject jThread = env->CallStaticObjectMethod(jThreadCls, jMethodId);
    if (UNLIKELY(jThread == nullptr)) {
        std::cout << "Failed to get current thread instance." << std::endl;
    } else {
        jThreadId = env->CallLongMethod(jThread, env->GetMethodID(jThreadCls, "getId", "()J"));
    }

    auto splitter = OckSplitter::Make(partitionMethod, jPartitionNum, colTypes.GetIds(), jColNum, (uint64_t)jThreadId);
    if (UNLIKELY(splitter == nullptr)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("Failed to make ock splitter").c_str());
    }

    bool ret = splitter->SetShuffleInfo(appId, jShuffleId, jStageId, jStageAttemptNum, jMapId, jTaskAttemptId);
    if (UNLIKELY(!ret)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("Failed to set shuffle information").c_str());
    }

    ret = splitter->InitLocalBuffer(jRegionSize, jMinCapacity, jMaxCapacity, (jIsCompress == JNI_TRUE));
    if (UNLIKELY(!ret)) {
        env->ThrowNew(env->FindClass(exceptionClass), std::string("Failed to initialize local buffer").c_str());
    }

    return gOckSplitterMap.Insert(std::shared_ptr<OckSplitter>(splitter));
}

JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_split(JNIEnv *env, jobject,
    jlong splitterId, jlong nativeVectorBatch)
{
    auto splitter = gOckSplitterMap.Lookup(splitterId);
    if (UNLIKELY(!splitter)) {
        std::string errMsg = "Invalid splitter id " + std::to_string(splitterId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
    }

    auto vecBatch = (VectorBatch *)nativeVectorBatch;
    if (UNLIKELY(vecBatch == nullptr)) {
        std::string errMsg = "Invalid address for native vector batch.";
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
    }

    if (UNLIKELY(!splitter->Split(*vecBatch))) {
        std::string errMsg = "Failed to split vector batch by splitter id " + std::to_string(splitterId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
    }

    delete vecBatch;
}

JNIEXPORT jobject JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_stop(JNIEnv *env, jobject,
    jlong splitterId)
{
    auto splitter = gOckSplitterMap.Lookup(splitterId);
    if (UNLIKELY(!splitter)) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitterId);
        env->ThrowNew(env->FindClass(exceptionClass), error_message.c_str());
    }

    splitter->Stop(); // free resource

    const auto &partitionLengths = splitter->PartitionLengths();
    auto jPartitionLengths = env->NewLongArray(partitionLengths.size());
    auto jData = reinterpret_cast<const jlong *>(partitionLengths.data());
    env->SetLongArrayRegion(jPartitionLengths, 0, partitionLengths.size(), jData);

    return env->NewObject(gSplitResultClass, gSplitResultConstructor, 0, 0, 0, splitter->GetTotalWriteBytes(), 0,
        jPartitionLengths);
}

JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_close(JNIEnv *env, jobject,
    jlong splitterId)
{
    auto splitter = gOckSplitterMap.Lookup(splitterId);
    if (UNLIKELY(!splitter)) {
        std::string errMsg = "Invalid splitter id " + std::to_string(splitterId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
    }

    gOckSplitterMap.Erase(splitterId);
}