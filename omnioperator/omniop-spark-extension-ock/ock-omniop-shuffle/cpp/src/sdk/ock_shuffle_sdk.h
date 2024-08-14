/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_SHUFFLE_SDK_H
#define SPARK_THESTRAL_PLUGIN_OCK_SHUFFLE_SDK_H

#include <dlfcn.h>
#include <sstream>

#include "common/common.h"
#include "base_api_shuffle.h"

using FUNC_GET_LOCAL_BLOB = int (*)(const char *, const char *, uint64_t, uint32_t, uint32_t, uint64_t *);
using FUNC_COMMIT_LOCAL_BLOB = int (*)(const char *, uint64_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t,
    uint8_t, uint32_t, uint32_t *);
using FUNC_MAP_BLOB = int (*)(uint64_t, void **, const char *);
using FUNC_UNMAP_BLOB = int (*)(uint64_t, void *);

// rss
using FUNC_RSS_CREATE_WRITER = int (*)(uint32_t, uint32_t, uint32_t *, uint8_t **);
using FUNC_RSS_COMMIT_REGION = int (*)(uint32_t, uint32_t, uint32_t, uint8_t *, uint32_t);
using FUNC_RSS_FLUSH_REGION = int (*)(uint32_t, bool, uint64_t *);
using FUNC_RSS_GET_RELEASE_REGION = int (*)(uint32_t, uint32_t **, uint32_t *);

class OckShuffleSdk {
public:
    static FUNC_GET_LOCAL_BLOB mGetLocalBlobFun;
    static FUNC_COMMIT_LOCAL_BLOB mCommitLocalBlobFun;
    static FUNC_MAP_BLOB mMapBlobFun;
    static FUNC_UNMAP_BLOB mUnmapBlobFun;
    static FUNC_RSS_CREATE_WRITER mRssCreateWriterFun;
    static FUNC_RSS_COMMIT_REGION mRssCommitRegionFun;
    static FUNC_RSS_FLUSH_REGION mRssFlushRegionFun;
    static FUNC_RSS_GET_RELEASE_REGION mRssGetReleaseRegionFun;

#define LoadFunction(name, func)                                                                                     \
    do {                                                                                                             \
        *(func) = dlsym(mHandle, (name));                                                                            \
        if (UNLIKELY(*(func) == nullptr)) {                                                                          \
            std::cout << "Failed to load function <" << (name) << "> with error <" << dlerror() << ">" << std::endl; \
            return false;                                                                                            \
        }                                                                                                            \
    } while (0)

    static bool Initialize()
    {
        const char *library = "libock_shuffle.so";
        mHandle = dlopen(library, RTLD_NOW);
        if (mHandle == nullptr) {
            std::cout << "Failed to open library <" << library << "> with error <" << dlerror() << ">" << std::endl;
            return false;
        }

        void *func = nullptr;
        LoadFunction("ShuffleLocalBlobGet", &func);
        mGetLocalBlobFun = reinterpret_cast<FUNC_GET_LOCAL_BLOB>(func);

        LoadFunction("ShuffleLocalBlobCommit", &func);
        mCommitLocalBlobFun = reinterpret_cast<FUNC_COMMIT_LOCAL_BLOB>(func);

        LoadFunction("ShuffleBlobObtainRawAddress", &func);
        mMapBlobFun = reinterpret_cast<FUNC_MAP_BLOB>(func);

        LoadFunction("ShuffleBlobReleaseRawAddress", &func);
        mUnmapBlobFun = reinterpret_cast<FUNC_UNMAP_BLOB>(func);

        LoadFunction("RSSCreateWriter", &func);
        mRssCreateWriterFun = reinterpret_cast<FUNC_RSS_CREATE_WRITER>(func);

        LoadFunction("RSSCommitRegion", &func);
        mRssCommitRegionFun = reinterpret_cast<FUNC_RSS_COMMIT_REGION>(func);

        LoadFunction("RSSFlushRegion", &func);
        mRssFlushRegionFun = reinterpret_cast<FUNC_RSS_FLUSH_REGION>(func);

        LoadFunction("RSSGetReleaseRegionIds", &func);
        mRssGetReleaseRegionFun = reinterpret_cast<FUNC_RSS_GET_RELEASE_REGION>(func);

        return true;
    }

    static void UnInitialize()
    {
        if (mHandle != nullptr) {
            dlclose(mHandle);
        }

        mHandle = nullptr;
    }

private:
    static void *mHandle;
};

#endif // SPARK_THESTRAL_PLUGIN_OCK_SHUFFLE_SDK_H