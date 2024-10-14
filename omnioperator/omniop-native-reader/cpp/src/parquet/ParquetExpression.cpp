/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "ParquetExpression.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/array/builder_base.h"

namespace omniruntime::reader {
using namespace arrow;

Result<std::shared_ptr<DataType>> GetLiteralDataType(JNIEnv *env, jobject &expressionTree)
{
    auto type = env->CallIntMethod(expressionTree, jsonMethodInt, env->NewStringUTF("type"));
    switch ((PredicateDataType) type) {
        case PredicateDataType::Int:
            return arrow::int32();
        case PredicateDataType::Long:
            return arrow::int64();
        case PredicateDataType::Timestamp:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case PredicateDataType::String:
            return arrow::utf8();
        case PredicateDataType::Date32:
            return arrow::date32();
        case PredicateDataType::Decimal: {
            int32_t precision = (int32_t)env->CallIntMethod(expressionTree, jsonMethodInt, env->NewStringUTF("precision"));
            int32_t scale = (int32_t)env->CallIntMethod(expressionTree, jsonMethodInt, env->NewStringUTF("scale"));
            return arrow::decimal(precision, scale);
        }
        case PredicateDataType::Bool:
            return arrow::boolean();
        case PredicateDataType::Short:
            return arrow::int16();
        case PredicateDataType::Double:
            return arrow::float64();
        default:
            return Status::Invalid("Unsupported literal data type " + type);
    }
}

inline Result<std::shared_ptr<Scalar>> ParseStringToScalar(const std::shared_ptr<DataType>& type, const std::string& str)
{
    if (type->id() == arrow::Type::DECIMAL128) {
        return std::make_shared<Decimal128Scalar>(Decimal128(str), type);
    }
    if (type->id() == arrow::Type::DECIMAL256) {
        return std::make_shared<Decimal256Scalar>(Decimal256(str), type);
    }
    return arrow::Scalar::Parse(type, str);
}

arrow::Result<Expression> GetSubExpr(JNIEnv *env, jobject &expressionTree, const char *sub)
{
    jobject subJson = env->CallObjectMethod(expressionTree, jsonMethodObj, env->NewStringUTF(sub));
    return ParseToArrowExpression(env, subJson);
}

arrow::Result<Expression> GetFieldExpr(JNIEnv *env, jobject &expressionTree)
{
    jstring field = (jstring) env->CallObjectMethod(expressionTree, jsonMethodString, env->NewStringUTF("field"));
    const char *ptr = env->GetStringUTFChars(field, JNI_FALSE);
    return arrow::compute::field_ref(ptr);
}

arrow::Result<Expression> GetLiteralExpr(JNIEnv *env, jobject &expressionTree)
{
    ARROW_ASSIGN_OR_RAISE(auto dataType, GetLiteralDataType(env, expressionTree));
    jstring jstr = (jstring) env->CallObjectMethod(expressionTree, jsonMethodString, env->NewStringUTF("literal"));
    const char *cstr = env->GetStringUTFChars(jstr, JNI_FALSE);
    std::string str(cstr);
    ARROW_ASSIGN_OR_RAISE(auto scalar, ParseStringToScalar(dataType, str));
    env->ReleaseStringUTFChars(jstr, cstr);
    return arrow::compute::literal(scalar);
}

arrow::Result<std::shared_ptr<Array>> GetSetLiteralExpr(JNIEnv *env, jobject &expressionTree)
{
    ARROW_ASSIGN_OR_RAISE(auto dataType, GetLiteralDataType(env, expressionTree));
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(dataType));

    jobjectArray strArray = (jobjectArray) env->CallObjectMethod(expressionTree, jsonMethodObj, env->NewStringUTF("literal"));
    auto length = static_cast<int32_t>(env->GetArrayLength(strArray));
    for (int32_t i = 0; i < length; i++) {
        jstring jstr = (jstring) env->GetObjectArrayElement(strArray, i);
        if (jstr == NULL) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            const char *cstr = env->GetStringUTFChars(jstr, JNI_FALSE);
            std::string str(cstr);
            ARROW_ASSIGN_OR_RAISE(auto scalar, ParseStringToScalar(dataType, str));
            ARROW_RETURN_NOT_OK(builder->AppendScalar(*scalar));
            env->ReleaseStringUTFChars(jstr, cstr);
        }
    }
    return builder->Finish();
}

arrow::Result<Expression> ParseToArrowExpression(JNIEnv *env, jobject &expressionTree)
{
    if (expressionTree == NULL) {
        return Status::Invalid("expressionTree is null");
    }
    int op = env->CallIntMethod(expressionTree, jsonMethodInt, env->NewStringUTF("op"));
    switch ((ParquetPredicateOperator) op) {
        case ParquetPredicateOperator::And: {
            ARROW_ASSIGN_OR_RAISE(auto leftExpr, GetSubExpr(env, expressionTree, "left"));
            ARROW_ASSIGN_OR_RAISE(auto rightExpr, GetSubExpr(env, expressionTree, "right"));
            return arrow::compute::and_(leftExpr, rightExpr);
        }
        case ParquetPredicateOperator::Or: {
            ARROW_ASSIGN_OR_RAISE(auto leftExpr, GetSubExpr(env, expressionTree, "left"));
            ARROW_ASSIGN_OR_RAISE(auto rightExpr, GetSubExpr(env, expressionTree, "right"));
            return arrow::compute::or_(leftExpr, rightExpr);
        }
        case ParquetPredicateOperator::Not: {
            ARROW_ASSIGN_OR_RAISE(auto predicateExpr, GetSubExpr(env, expressionTree, "predicate"));
            return arrow::compute::not_(predicateExpr);
        }
        case ParquetPredicateOperator::Eq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(env, expressionTree));
            return arrow::compute::equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::Gt: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(env, expressionTree));
            return arrow::compute::greater(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::GtEq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(env, expressionTree));
            return arrow::compute::greater_equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::Lt: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(env, expressionTree));
            return arrow::compute::less(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::LtEq: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto literalExpr, GetLiteralExpr(env, expressionTree));
            return arrow::compute::less_equal(fieldExpr, literalExpr);
        }
        case ParquetPredicateOperator::IsNotNull: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            return arrow::compute::is_valid(fieldExpr);
        }
        case ParquetPredicateOperator::IsNull: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            return arrow::compute::is_null(fieldExpr);
        }
        case ParquetPredicateOperator::IN: {
            ARROW_ASSIGN_OR_RAISE(auto fieldExpr, GetFieldExpr(env, expressionTree));
            ARROW_ASSIGN_OR_RAISE(auto set, GetSetLiteralExpr(env, expressionTree));
            return arrow::compute::call("is_in", {fieldExpr}, compute::SetLookupOptions{set});
        }
        default:
            return Status::Invalid("ParquetPredicateOperator is not supported: " + op);
    }
}

}