/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <fcntl.h>
#include "concurrent_map.h"
#include "jni_common.h"
#include "shuffle/ock_type.h"
#include "shuffle/ock_merge_reader.h"
#include "OckShuffleJniReader.h"

using namespace omniruntime::vec;
using namespace ock::dopspark;

static std::mutex gInitLock;
static jclass gLongClass = nullptr;
static jfieldID gLongValueFieldId = nullptr;
static ConcurrentMap<std::shared_ptr<OckMergeReader>> gBlobReader;
static const char *exceptionClass = "java/lang/Exception";

static void JniInitialize(JNIEnv *env)
{
    std::lock_guard<std::mutex> lk(gInitLock);
    if (UNLIKELY(gLongClass == nullptr)) {
        gLongClass = env->FindClass("java/lang/Long");
        if (UNLIKELY(gLongClass == nullptr)) {
            env->ThrowNew(env->FindClass(exceptionClass), "Failed to find class java/lang/Long");
        }

        gLongValueFieldId = env->GetFieldID(gLongClass, "value", "J");
        if (UNLIKELY(gLongValueFieldId == nullptr)) {
            env->ThrowNew(env->FindClass(exceptionClass),
                "Failed to get field id <value> of class java/lang/Long");
        }
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_make(JNIEnv *env, jobject,
    jintArray jTypeIds)
{
    std::shared_ptr<OckMergeReader> instance = std::make_shared<OckMergeReader>();
    if (UNLIKELY(instance == nullptr)) {
        env->ThrowNew(env->FindClass(exceptionClass), "Failed to create instance for ock merge reader");
        return 0;
    }

    bool result = instance->Initialize(env->GetIntArrayElements(jTypeIds, nullptr), env->GetArrayLength(jTypeIds));
    if (UNLIKELY(!result)) {
        env->ThrowNew(env->FindClass(exceptionClass), "Failed to initialize ock merge reader");
        return 0;
    }

    return gBlobReader.Insert(instance);
}

JNIEXPORT jint JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeGetVectorBatch(JNIEnv *env, jobject,
    jlong jReaderId, jlong jAddress, jint jRemain, jint jMaxRow, jint jMaxSize, jobject jRowCnt)
{
    auto mergeReader = gBlobReader.Lookup(jReaderId);
    if (UNLIKELY(!mergeReader)) {
        std::string errMsg = "Invalid reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return -1;
    }

    JniInitialize(env);

    auto *address = reinterpret_cast<uint8_t *>(jAddress);
    if (UNLIKELY(!mergeReader->GetMergeVectorBatch(address, jRemain, jMaxRow, jMaxSize))) {
        std::string errMsg = "Invalid address for vb data address for reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return -1;
    }

    env->SetLongField(jRowCnt, gLongValueFieldId, mergeReader->GetRowNumAfterMerge());

    return mergeReader->GetVectorBatchLength();
}

JNIEXPORT jint JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeGetVecValueLength(JNIEnv *env,
    jobject, jlong jReaderId, jint jColIndex)
{
    auto mergeReader = gBlobReader.Lookup(jReaderId);
    if (UNLIKELY(!mergeReader)) {
        std::string errMsg = "Invalid reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return 0;
    }

    uint32_t length = 0;
    if (UNLIKELY(!mergeReader->CalVectorValueLength(jColIndex, length))) {
        std::string errMsg = "Failed to calculate value length for reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return 0;
    }

    return length;
}

JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeCopyVecDataInVB(JNIEnv *env,
    jobject, jlong jReaderId, jlong dstNativeVec, jint jColIndex)
{
    auto dstVector = reinterpret_cast<Vector *>(dstNativeVec); // get from scala which is real vector
    if (UNLIKELY(dstVector == nullptr)) {
        std::string errMsg = "Invalid dst vector address for reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return;
    }

    auto mergeReader = gBlobReader.Lookup(jReaderId);
    if (UNLIKELY(mergeReader == nullptr)) {
        std::string errMsg = "Invalid reader id " + std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return;
    }

    if (UNLIKELY(!mergeReader->CopyDataToVector(dstVector, jColIndex))) {
        std::string errMsg = "Failed to copy data to vector: " + std::to_string(jColIndex) + " for reader id " +
            std::to_string(jReaderId);
        env->ThrowNew(env->FindClass(exceptionClass), errMsg.c_str());
        return;
    }
}