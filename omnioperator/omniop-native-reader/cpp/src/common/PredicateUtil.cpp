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

#include "PredicateUtil.h"
#include <algorithm>
#include <nlohmann/json.hpp>

using namespace omniruntime::vec;

namespace common {
    std::unique_ptr<PredicateCondition> buildLeafPredicateCondition(PredicateOperatorType &op,
                                                                    nlohmann::json &jsonCondition) {
        using namespace omniruntime::type;
        int32_t index = jsonCondition["index"].get<int32_t>();
        std::string value = jsonCondition["value"];
        DataTypeId typeId = jsonCondition["dataType"].get<DataTypeId>();
        switch (typeId) {
            case OMNI_SHORT: {
                return std::make_unique<LeafPredicateCondition<int16_t>>(op, index, static_cast<int16_t>(stoi(value)));
            }
            case OMNI_INT: {
                return std::make_unique<LeafPredicateCondition<int32_t>>(op, index, stoi(value));
            }
            case OMNI_LONG: {
                return std::make_unique<LeafPredicateCondition<int64_t>>(op, index, stol(value));
            }
            case OMNI_DOUBLE: {
                return std::make_unique<LeafPredicateCondition<double>>(op, index, stod(value));
            }
            case OMNI_DATE32: {
                return std::make_unique<LeafPredicateCondition<int32_t>>(op, index, stoi(value));
            }
            case OMNI_BOOLEAN: {
                return std::make_unique<LeafPredicateCondition<int8_t>>(op, index, value == "true" ? 1 : 0);
            }
            default: {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "buildLeafPredicateCondition UnSupport DataTypeId: " + std::to_string(typeId));
            }
        }
    }

    std::unique_ptr<PredicateCondition> buildPredicateCondition(nlohmann::json &jsonCondition) {
        PredicateOperatorType op = jsonCondition["op"].get<PredicateOperatorType>();
        switch (op) {
            case TRUE:
            case EQUAL_TO:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case IS_NOT_NULL:
            case IS_NULL: {
                return buildLeafPredicateCondition(op, jsonCondition);
            }
            case OR: {
                auto orLeft = buildPredicateCondition(jsonCondition["left"]);
                auto orRight = buildPredicateCondition(jsonCondition["right"]);
                return std::make_unique<OrPredicateCondition>(std::move(orLeft), std::move(orRight));
            }
            case AND: {
                auto andLeft = buildPredicateCondition(jsonCondition["left"]);
                auto andRight = buildPredicateCondition(jsonCondition["right"]);
                return std::make_unique<AndPredicateCondition>(std::move(andLeft), std::move(andRight));
            }
            case NOT: {
                auto child = buildPredicateCondition(jsonCondition["child"]);
                return std::make_unique<NotPredicateCondition>(std::move(child));
            }
            default: {
                throw OmniException("OPERATOR_RUNTIME_ERROR", "buildPredicateCondition UnSupport PredicateOperatorType: " + std::to_string(op));
            }
        }
    }

    std::unique_ptr<PredicateCondition> BuildVecPredicateCondition(JNIEnv *env, jobject jsonObj, int32_t columnCount) {
        if (!env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("vecPredicateCondition"))) {
            return nullptr;
        }

        auto condition = static_cast<jstring>(env->CallObjectMethod(jsonObj, jsonMethodString,
                                                                    env->NewStringUTF("vecPredicateCondition")));
        auto conditionPtr = env->GetStringUTFChars(condition, JNI_FALSE);
        std::string conditionStr(conditionPtr);
        auto jsonCondition = nlohmann::json::parse(conditionStr);
        env->ReleaseStringUTFChars(condition, conditionPtr);
        auto predicate= buildPredicateCondition(jsonCondition);
        predicate->buildNullColumns(columnCount);
        return predicate;
    }

    struct BitMaskIndex {
        BitMaskIndex() {
            for (int i = 0; i < (1 << N); i++) {
                int32_t startIndex = i * (N + 1);
                int32_t index = startIndex;
                for (int bit = 0; bit < N; bit++) {
                    if (i & (1 << bit)) {
                        memo_[++index] = bit;
                    }
                }
                memo_[startIndex] = index - startIndex;
            }
        }

        const inline uint8_t* operator[](size_t i) const {
            return memo_ + (i * (N + 1));
        }

    private:
        static constexpr int N = 8;
        uint8_t memo_[(1 << N) * (N + 1)]{0};
    };

    const BitMaskIndex bitMaskIndex;
    const int32_t batchStep = 8;

    template <typename FlatVector, typename RAW_DATA_TYPE>
    void SetFlatVectorValue(int32_t rowCount, BaseVector *baseVector, BaseVector *selectedBaseVector,
        const uint8_t *bitMark, bool isAllNull, bool isAllNotNull)
    {
        int32_t index = 0;
        int32_t j = 0;
        uint8_t mask;
        // 该列过滤出来都是全部为空的情况
        if (isAllNull) {
            auto *nulls = reinterpret_cast<uint8_t *>(UnsafeBaseVector::GetNulls(selectedBaseVector));
            memset_s(nulls, BitUtil::Nbytes(selectedBaseVector->GetSize()), -1, BitUtil::Nbytes(selectedBaseVector->GetSize()));
            return;
        }
        // 该列过滤出来都是全部不为空的情况
        if (isAllNotNull) {
            auto *srcValues = UnsafeVector::GetRawValues(static_cast<FlatVector *>(baseVector));
            auto *destValues = UnsafeVector::GetRawValues(static_cast<FlatVector *>(selectedBaseVector));
            for (; j + batchStep <= rowCount; j += batchStep) {
                mask = bitMark[j >> 3];
                if (mask == 0) {
                    continue;
                }
                if (mask == 255) {
                    memcpy_s(destValues + index, batchStep * sizeof(RAW_DATA_TYPE), srcValues + j, batchStep * sizeof(RAW_DATA_TYPE));
                    index += batchStep;
                    continue;
                }
                const uint8_t *maskArr = bitMaskIndex[mask];
                for (int i = 1; i <= *maskArr; i++) {
                    auto offset = j + maskArr[i];
                    auto value = static_cast<FlatVector *>(baseVector)->GetValue(offset);
                    static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
                }
            }
            for (; j < rowCount; j++) {
                if (BitUtil::IsBitSet(bitMark, j)) {
                    auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                    static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
                }
            }
            return;
        }
        // 其他情况
        for (; j + batchStep <= rowCount; j += batchStep) {
            mask = bitMark[j >> 3];
            if (mask == 0) {
                continue;
            }
            const uint8_t *maskArr = bitMaskIndex[mask];
            for (int i = 1; i <= *maskArr; i++) {
                auto offset = j + maskArr[i];
                if (UNLIKELY(baseVector->IsNull(offset))) {
                    static_cast<FlatVector *>(selectedBaseVector)->SetNull(index++);
                } else {
                    auto value = static_cast<FlatVector *>(baseVector)->GetValue(offset);
                    static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
                }
            }
        }
        for (; j < rowCount; j++) {
            if (BitUtil::IsBitSet(bitMark, j)) {
                if (UNLIKELY(baseVector->IsNull(j))) {
                    static_cast<FlatVector *>(selectedBaseVector)->SetNull(index++);
                } else {
                    auto value = static_cast<FlatVector *>(baseVector)->GetValue(j);
                    static_cast<FlatVector *>(selectedBaseVector)->SetValue(index++, value);
                }
            }
        }
    }

    void SetStringVectorValue(int32_t rowCount, Vector<LargeStringContainer<std::string_view>> *baseVector,
        Vector<LargeStringContainer<std::string_view>> *selectedBaseVector, const uint8_t *bitMark, bool isAllNull,
        bool isAllNotNull)
    {
        int32_t index = 0;
        int32_t j = 0;
        uint8_t mask;
        // 该列过滤出来都是全部为空的情况
        if (isAllNull) {
            for (; j < selectedBaseVector->GetSize(); j++) {
                selectedBaseVector->SetNull(j);
            }
            return;
        }
        // 该列过滤出来都是全部不为空的情况
        if (isAllNotNull) {
            for (; j + batchStep <= rowCount; j += batchStep) {
                mask = bitMark[j >> 3];
                if (mask == 0) {
                    continue;
                }
                const uint8_t *maskArr = bitMaskIndex[mask];
                for (int i = 1; i <= *maskArr; i++) {
                    auto offset = j + maskArr[i];
                    auto value = baseVector->GetValue(offset);
                    selectedBaseVector->SetValue(index++, value);
                }
            }
            for (; j < rowCount; j++) {
                if (BitUtil::IsBitSet(bitMark, j)) {
                    auto value = baseVector->GetValue(j);
                    selectedBaseVector->SetValue(index++, value);
                }
            }
            return;
        }
        // 其他情况
        for (; j + batchStep <= rowCount; j += batchStep) {
            mask = bitMark[j >> 3];
            if (mask == 0) {
                continue;
            }
            const uint8_t *maskArr = bitMaskIndex[mask];
            for (int i = 1; i <= *maskArr; i++) {
                auto offset = j + maskArr[i];
                if (UNLIKELY(baseVector->IsNull(offset))) {
                    selectedBaseVector->SetNull(index++);
                } else {
                    auto value = baseVector->GetValue(offset);
                    selectedBaseVector->SetValue(index++, value);
                }
            }
        }
        for (; j < rowCount; j++) {
            if (BitUtil::IsBitSet(bitMark, j)) {
                if (UNLIKELY(baseVector->IsNull(j))) {
                    selectedBaseVector->SetNull(index++);
                } else {
                    auto value = baseVector->GetValue(j);
                    selectedBaseVector->SetValue(index++, value);
                }
            }
        }
    }

    bool GetFlatBaseVectorsFromBitMark(std::vector<BaseVector *> &baseVectors, std::vector<BaseVector *> &result,
        uint8_t *bitMark, int32_t vectorSize, const std::set<int32_t>& isNullSet, const std::set<int32_t>& isNotNullSet) {
        int32_t resultSize = BitUtil::CountBits(reinterpret_cast<const uint64_t *>(bitMark), 0, vectorSize);
        if (resultSize == vectorSize) {
            // all selected, filtering is not required.
            return false;
        }
        if (UNLIKELY(baseVectors.empty())) {
            return false;
        }
        int32_t rowCount = baseVectors[0]->GetSize();
        int32_t encodingType = baseVectors[0]->GetEncoding();
        if (UNLIKELY(encodingType == OMNI_DICTIONARY)) {
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "OMNI_DICTIONARY is unsupported.");
        }
        result.resize(baseVectors.size(), nullptr);
        for (int32_t i = 0; i < baseVectors.size(); i++) {
            auto baseVector = baseVectors[i];
            auto dataType = baseVector->GetTypeId();
            auto selectedBaseVector = VectorHelper::CreateVector(OMNI_FLAT, dataType, static_cast<int32_t>(resultSize));
            auto isAllNull = isNullSet.count(i);
            auto isAllNotNull = isNotNullSet.count(i);
            switch (dataType) {
                case OMNI_INT:
                case OMNI_DATE32: {
                    SetFlatVectorValue<Vector<int32_t>, int32_t>(rowCount, baseVector, selectedBaseVector, bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_SHORT: {
                    SetFlatVectorValue<Vector<int16_t>, int16_t>(rowCount, baseVector, selectedBaseVector, bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64: {
                    SetFlatVectorValue<Vector<int64_t>, int64_t>(rowCount, baseVector, selectedBaseVector, bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_DOUBLE: {
                    SetFlatVectorValue<Vector<double>, double>(rowCount, baseVector, selectedBaseVector, bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_BOOLEAN: {
                    SetFlatVectorValue<Vector<bool>, bool>(rowCount, baseVector, selectedBaseVector, bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_DECIMAL128: {
                    SetFlatVectorValue<Vector<Decimal128>, Decimal128>(rowCount, baseVector, selectedBaseVector,
                        bitMark, isAllNull, isAllNotNull);
                    break;
                }
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    SetStringVectorValue(rowCount, dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector),
                        dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(selectedBaseVector), bitMark,
                        isAllNull, isAllNotNull);
                    break;
                }
                default: {
                    LogError("No such %d type support", dataType);
                    throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR",
                        "unsupported selectivity type: " + std::to_string(static_cast<int>(dataType)));
                }
            }
            result[i] = selectedBaseVector;
        }
        return true;
    }
}
