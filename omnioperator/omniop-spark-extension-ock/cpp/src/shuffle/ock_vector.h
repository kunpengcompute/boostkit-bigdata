/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_VECTOR_H
#define SPARK_THESTRAL_PLUGIN_OCK_VECTOR_H

#include <vector/vector_common.h>

namespace ock {
namespace dopspark {
class OckVector {
public:
    OckVector() = default;
    ~OckVector() = default;

    [[nodiscard]] inline uint32_t GetSize() const
    {
        return size;
    }

    void SetSize(uint32_t newSize)
    {
        this->size = newSize;
    }

    [[nodiscard]] inline uint32_t GetCapacityInBytes() const
    {
        return capacityInBytes;
    }

    void SetCapacityInBytes(uint32_t capacity)
    {
        capacityInBytes = capacity;
    }

    [[nodiscard]] inline void *GetValueNulls() const
    {
        return valueNullsAddress;
    }

    void SetValueNulls(void *address)
    {
        valueNullsAddress = address;
    }

    [[nodiscard]] inline void *GetValues() const
    {
        return valuesAddress;
    }

    void SetValues(void *address)
    {
        valuesAddress = address;
    }

    [[nodiscard]] inline void *GetValueOffsets() const
    {
        return valueOffsetsAddress;
    }

    int GetValueOffset(int index)
    {
        return static_cast<int32_t *>(valueOffsetsAddress)[index];
    }

    void SetValueOffsets(void *address)
    {
        valueOffsetsAddress = address;
    }

    inline void SetNextVector(OckVector *next)
    {
        mNext = next;
    }

    inline OckVector *GetNextVector()
    {
        return mNext;
    }

private:
    uint32_t size = 0;
    uint32_t capacityInBytes = 0;

    void *valuesAddress = nullptr;
    void *valueNullsAddress = nullptr;
    void *valueOffsetsAddress = nullptr;

    OckVector *mNext = nullptr;
};
}
}
#endif // SPARK_THESTRAL_PLUGIN_OCK_VECTOR_H
