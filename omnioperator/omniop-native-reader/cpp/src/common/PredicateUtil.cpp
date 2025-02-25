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

    std::unique_ptr<PredicateCondition> BuildVecPredicateCondition(JNIEnv *env, jobject jsonObj) {
        if (!env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("vecPredicateCondition"))) {
            return nullptr;
        }

        auto condition = static_cast<jstring>(env->CallObjectMethod(jsonObj, jsonMethodString,
                                                                    env->NewStringUTF("vecPredicateCondition")));
        auto conditionPtr = env->GetStringUTFChars(condition, JNI_FALSE);
        std::string conditionStr(conditionPtr);
        auto jsonCondition = nlohmann::json::parse(conditionStr);
        env->ReleaseStringUTFChars(condition, conditionPtr);
        return buildPredicateCondition(jsonCondition);
    }
}
