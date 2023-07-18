/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef THESTRAL_PLUGIN_MASTER_CONCURRENT_MAP_H
#define THESTRAL_PLUGIN_MASTER_CONCURRENT_MAP_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <jni.h>

/**
 * An utility class that map module id to module pointers.
 * @tparam Holder class of the object to hold.
 */
namespace ock {
namespace dopspark {
template <typename Holder>
class ConcurrentMap {
public:
    ConcurrentMap() : moduleId(initModuleId) {}

    jlong Insert(Holder holder) {
        std::lock_guard<std::mutex> lock(mtx);
        jlong result = moduleId++;
        map.insert(std::pair<jlong, Holder>(result, holder));
        return result;
    }

    void Erase(jlong moduleId) {
        std::lock_guard<std::mutex> lock(mtx);
        map.erase(moduleId);
    }

    Holder Lookup(jlong moduleId) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(moduleId);
        if (it != map.end()) {
            return it->second;
        }
        return nullptr;
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(mtx);
        map.clear();
    }

    size_t Size() {
        std::lock_guard<std::mutex> lock(mtx);
        return map.size();
    }

private:
    // Initialize the module id starting value to a number greater than zero
    // to allow for easier debugging of uninitialized java variables.
    static constexpr int initModuleId = 4;

    int64_t moduleId;
    std::mutex mtx;
    // map from module ids returned to Java and module pointers
    std::unordered_map<jlong, Holder> map;
};
}
}
#endif //THESTRAL_PLUGIN_MASTER_CONCURRENT_MAP_H