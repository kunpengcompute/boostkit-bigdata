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

#ifndef PREDICATECONDITION_H
#define PREDICATECONDITION_H

#include <cstdint>
#include <memory>
#include <utility>
#include <set>
#include "vector/vector_common.h"
#include "util/bit_util.h"
#include "xsimd/xsimd.hpp"
#include "PredicateOperatorType.h"

using omniruntime::BitUtil;
using omniruntime::mem::AlignedBuffer;
using omniruntime::vec::BaseVector;
using omniruntime::vec::Vector;
using omniruntime::vec::unsafe::UnsafeVector;
using omniruntime::vec::unsafe::UnsafeBaseVector;
using omniruntime::exception::OmniException;

namespace common {

    template <typename V>
    bool equalTo(const V &left, const V &right) {
        return left == right;
    }

    template <typename V>
    bool greater(const V &left, const V &right) {
        return left > right;
    }

    template <typename V>
    bool greaterEqual(const V &left, const V &right) {
        return left >= right;
    }

    template <typename V>
    bool less(const V &left, const V &right) {
        return left < right;
    }

    template <typename V>
    bool lessEqual(const V &left, const V &right) {
        return left <= right;
    }

    template <typename V>
    xsimd::batch_bool<V> batchEqualTo(const xsimd::batch<V> &left, const xsimd::batch<V> &right) {
        return xsimd::eq(left, right);
    }

    template <typename V>
    xsimd::batch_bool<V> batchGreater(const xsimd::batch<V> &left, const xsimd::batch<V> &right) {
        return xsimd::gt(left, right);
    }

    template <typename V>
    xsimd::batch_bool<V> batchGreaterEqual(const xsimd::batch<V> &left, const xsimd::batch<V> &right) {
        return xsimd::ge(left, right);
    }

    template <typename V>
    xsimd::batch_bool<V> batchLess(const xsimd::batch<V> &left, const xsimd::batch<V> &right) {
        return xsimd::lt(left, right);
    }

    template <typename V>
    xsimd::batch_bool<V> batchLessEqual(const xsimd::batch<V> &left, const xsimd::batch<V> &right) {
        return xsimd::le(left, right);
    }

    class PredicateCondition {
    public:
        explicit PredicateCondition(const PredicateOperatorType &opType) {
            this->op = opType;
        }

        virtual ~PredicateCondition() = default;

        virtual uint8_t *compute(std::vector<BaseVector *> &vecBatch) = 0;

        virtual void init(const int32_t size) {
            bitSize = size;
            bitMarkBuf = std::make_unique<AlignedBuffer<uint8_t>>(BitUtil::Nbytes(size) + 8);
            bitMark = bitMarkBuf->GetBuffer();
        }

        virtual bool isAllNull(int32_t columnIndex) {
            return false;
        }

        virtual bool isAllNotNull(int32_t columnIndex) {
            return false;
        }

        void buildNullColumns(int32_t columnCount) {
            for (int32_t i = 0; i < columnCount; i++) {
                if (isAllNull(i)) {
                    isAllNullColumns.insert(i);
                }
                if (isAllNotNull(i)) {
                    isAllNotNullColumns.insert(i);
                }
            }
        }

        std::set<int32_t> &getIsAllNullColumns() {
            return isAllNullColumns;
        }

        std::set<int32_t> &getIsAllNotNullColumns() {
            return isAllNotNullColumns;
        }

    protected:
        PredicateOperatorType op;
        int32_t bitSize = 0;
        std::unique_ptr<AlignedBuffer<uint8_t>> bitMarkBuf;
        uint8_t *bitMark = nullptr;
        std::set<int32_t> isAllNullColumns;
        std::set<int32_t> isAllNotNullColumns;
    };

    template<typename T>
    class LeafPredicateCondition final : public PredicateCondition {
    public:
        LeafPredicateCondition(const PredicateOperatorType &opType, int32_t index, T value)
            : PredicateCondition(opType), index(index), value(value) {
        }

        template <xsimd::batch_bool<T> (*BATCH_OP)(const xsimd::batch<T> &, const xsimd::batch<T> &), bool (*OP)(const T &, const T &)>
        void computeCompare(BaseVector *vector, int32_t vectorSize) {
            xsimd::batch<T> broadcast = xsimd::batch<T>(value);
            int32_t step = static_cast<int32_t>(xsimd::batch<T>::size);
            Vector<T> *realVector = reinterpret_cast<Vector<T> *>(vector);
            T *values = UnsafeVector::GetRawValues(realVector);
            int32_t index = 0;
            for (; index + step <= vectorSize; index += step) {
                xsimd::batch<T> valuesBatch = xsimd::load_aligned(values + index);
                xsimd::batch_bool<T> result = BATCH_OP(valuesBatch, broadcast);
                uint64_t mask = result.mask();
                BitUtil::StoreBits<uint64_t>(reinterpret_cast<uint64_t *>(bitMark), index, mask, 64);
            }
            for (; index < vectorSize; index++) {
                bool result = OP(values[index], value);
                BitUtil::SetBit(bitMark, index, result);
            }
        }

        uint8_t *compute(std::vector<BaseVector *> &vecBatch) override {
            auto vector = vecBatch[index];
            auto vectorSize = vector->GetSize();
            switch (op) {
                case TRUE: {
                    errno_t opTrueRet = memset_s(bitMark, BitUtil::Nbytes(bitSize), -1, BitUtil::Nbytes(bitSize));
                    if (UNLIKELY(opTrueRet != EOK)) {
                        throw OmniException("OPERATOR_RUNTIME_ERROR", "LeafPredicateCondition TRUE memset_s fail.");
                    }
                    break;
                }
                case EQUAL_TO: {
                    computeCompare<batchEqualTo, equalTo>(vector, vectorSize);
                    break;
                }
                case GREATER_THAN: {
                    computeCompare<batchGreater, greater>(vector, vectorSize);
                    break;
                }
                case GREATER_THAN_OR_EQUAL: {
                    computeCompare<batchGreaterEqual, greaterEqual>(vector, vectorSize);
                    break;
                }
                case LESS_THAN: {
                    computeCompare<batchLess, less>(vector, vectorSize);
                    break;
                }
                case LESS_THAN_OR_EQUAL: {
                    computeCompare<batchLessEqual, lessEqual>(vector, vectorSize);
                    break;
                }
                case IS_NOT_NULL: {
                    uint8_t *nulls = reinterpret_cast<uint8_t *>(UnsafeBaseVector::GetNulls(vector));
                    int32_t step = static_cast<int32_t>(xsimd::batch<uint8_t>::size);
                    int32_t byteLen = BitUtil::Nbytes(vectorSize);
                    int32_t index = 0;
                    for (; index + step <= byteLen; index += step) {
                        xsimd::batch<uint8_t> valuesBatch = xsimd::load_aligned(nulls + index);
                        xsimd::batch<uint8_t> result = xsimd::bitwise_not(valuesBatch);
                        result.store_aligned(bitMark + index);
                    }
                    for (; index < byteLen; index++) {
                        bitMark[index] = ~nulls[index];
                    }
                    break;
                }
                case IS_NULL: {
                    errno_t opIsNullRet = memcpy_s(bitMark, BitUtil::Nbytes(vectorSize),
                         UnsafeBaseVector::GetNulls(vector), BitUtil::Nbytes(vectorSize));
                    if (UNLIKELY(opIsNullRet != EOK)) {
                        throw OmniException("OPERATOR_RUNTIME_ERROR", "LeafPredicateCondition IS_NULL memcpy_s fail.");
                    }
                    break;
                }
                default:
                    throw OmniException("OPERATOR_RUNTIME_ERROR", 
                        "LeafPredicateCondition UnSupport OperatorType: " + std::to_string(op));
            }
            return bitMark;
        }

        bool isAllNull(int32_t columnIndex) override {
            return columnIndex == index && op == IS_NULL;
        }

        bool isAllNotNull(int32_t columnIndex) override {
            return columnIndex == index && op == IS_NOT_NULL;
        }

    private:
        int32_t index;
        T value;
    };

    class NotPredicateCondition : public PredicateCondition {
    public:
        explicit NotPredicateCondition(std::unique_ptr<PredicateCondition> child) : PredicateCondition(NOT),
            child(std::move(child)) {
        }

        void init(const int32_t size) override {
            PredicateCondition::init(size);
            child->init(size);
        }

        uint8_t *compute(std::vector<BaseVector *> &vecBatch) override {
            auto vectorSize = vecBatch[0]->GetSize();
            uint8_t *childResult = child->compute(vecBatch);
            int32_t step = static_cast<int32_t>(xsimd::batch<uint8_t>::size);
            int32_t byteLen = BitUtil::Nbytes(vectorSize);
            int32_t index = 0;
            for (; index + step <= byteLen; index += step) {
                xsimd::batch<uint8_t> valuesBatch = xsimd::load_aligned(childResult + index);
                xsimd::batch<uint8_t> result = xsimd::bitwise_not(valuesBatch);
                result.store_aligned(bitMark + index);
            }
            for (; index < byteLen; index++) {
                bitMark[index] = ~childResult[index];
            }
            return bitMark;
        }

    private:
        std::unique_ptr<PredicateCondition> child;
    };

    class AndPredicateCondition : public PredicateCondition {
    public:
        AndPredicateCondition(std::unique_ptr<PredicateCondition> left, std::unique_ptr<PredicateCondition> right)
            : PredicateCondition(AND), left(std::move(left)), right(std::move(right)) {
        }

        void init(const int32_t size) override {
            PredicateCondition::init(size);
            left->init(size);
            right->init(size);
        }

        uint8_t *compute(std::vector<BaseVector *> &vecBatch) override {
            auto vectorSize = vecBatch[0]->GetSize();
            uint8_t *leftResult = left->compute(vecBatch);
            uint8_t *rightResult = right->compute(vecBatch);
            int32_t step = static_cast<int32_t>(xsimd::batch<uint8_t>::size);
            int32_t byteLen = BitUtil::Nbytes(vectorSize);
            int32_t index = 0;
            for (; index + step <= byteLen; index += step) {
                xsimd::batch<uint8_t> leftBatch = xsimd::load_aligned(leftResult + index);
                xsimd::batch<uint8_t> rightBatch = xsimd::load_aligned(rightResult + index);
                xsimd::batch<uint8_t> result = xsimd::bitwise_and(leftBatch, rightBatch);
                result.store_aligned(bitMark + index);
            }
            for (; index < byteLen; index++) {
                bitMark[index] = leftResult[index] & rightResult[index];
            }
            return bitMark;
        }

        bool isAllNull(int32_t columnIndex) override {
            return left->isAllNull(columnIndex) | right->isAllNull(columnIndex);
        }

        bool isAllNotNull(int32_t columnIndex) override {
            return left->isAllNotNull(columnIndex) | right->isAllNotNull(columnIndex);
        }

    private:
        std::unique_ptr<PredicateCondition> left;
        std::unique_ptr<PredicateCondition> right;
    };

    class OrPredicateCondition : public PredicateCondition {
    public:
        OrPredicateCondition(std::unique_ptr<PredicateCondition> left, std::unique_ptr<PredicateCondition> right)
            : PredicateCondition(OR), left(std::move(left)), right(std::move(right)) {
        }

        void init(const int32_t size) override {
            PredicateCondition::init(size);
            left->init(size);
            right->init(size);
        }

        uint8_t *compute(std::vector<BaseVector *> &vecBatch) override {
            auto vectorSize = vecBatch[0]->GetSize();
            uint8_t *leftResult = left->compute(vecBatch);
            uint8_t *rightResult = right->compute(vecBatch);
            int32_t step = static_cast<int32_t>(xsimd::batch<uint8_t>::size);
            int32_t byteLen = BitUtil::Nbytes(vectorSize);
            int32_t index = 0;
            for (; index + step <= byteLen; index += step) {
                xsimd::batch<uint8_t> leftBatch = xsimd::load_aligned(leftResult + index);
                xsimd::batch<uint8_t> rightBatch = xsimd::load_aligned(rightResult + index);
                xsimd::batch<uint8_t> result = xsimd::bitwise_or(leftBatch, rightBatch);
                result.store_aligned(bitMark + index);
            }
            for (; index < byteLen; index++) {
                bitMark[index] = leftResult[index] | rightResult[index];
            }
            return bitMark;
        }

    private:
        std::unique_ptr<PredicateCondition> left;
        std::unique_ptr<PredicateCondition> right;
    };
}

#endif //PREDICATECONDITION_H
