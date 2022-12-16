/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef THESTRAL_PLUGIN_MASTER_JNI_COMMON_H
#define THESTRAL_PLUGIN_MASTER_JNI_COMMON_H

#include <jni.h>

#include "../common/common.h"

static jclass illegal_access_exception_class;

inline jclass CreateGlobalClassReference(JNIEnv *env, const char *class_name)
{
    jclass local_class = env->FindClass(class_name);
    auto global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    if (global_class == nullptr) {
        std::string errorMessage = "Unable to createGlobalClassReference for" + std::string(class_name);
        env->ThrowNew(illegal_access_exception_class, errorMessage.c_str());
    }
    return global_class;
}

inline jmethodID GetMethodID(JNIEnv *env, jclass this_class, const char *name, const char *sig)
{
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    if (ret == nullptr) {
        std::string errorMessage =
            "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
        env->ThrowNew(illegal_access_exception_class, errorMessage.c_str());
    }

    return ret;
}

#endif // THESTRAL_PLUGIN_MASTER_JNI_COMMON_H