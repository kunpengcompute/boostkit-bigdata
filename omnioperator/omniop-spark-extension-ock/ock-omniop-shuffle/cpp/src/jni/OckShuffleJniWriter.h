/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef JNI_OCK_SHUFFLE_JNI_WRITER
#define JNI_OCK_SHUFFLE_JNI_WRITER

#include <jni.h>
/* Header for class com_huawei_ock_spark_jni_OckShuffleJniWriter */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniWriter
 * Method:    initialize
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_initialize(JNIEnv *env, jobject);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniWriter
 * Method:    nativeMake
 * Signature: (Ljava/lang/String;IIIJLjava/lang/String;ILjava/lang/String;IIIIIZ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_nativeMake(JNIEnv *, jobject, jstring,
    jint, jint, jint, jint, jlong, jstring, jint, jstring, jint, jint, jint, jint, jboolean);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniWriter
 * Method:    split
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_split(JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniWriter
 * Method:    stop
 * Signature: (J)Lcom/huawei/ock/spark/vectorized/SplitResult;
 */
JNIEXPORT jobject JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_stop(JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniWriter
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniWriter_close(JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif // JNI_OCK_SHUFFLE_JNI_WRITER