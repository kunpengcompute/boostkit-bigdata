/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "test_utils.h"

using namespace omniruntime::vec;

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataTypePtr> &dataTypes)
{
    for (int i = 0; i < dataTypeCount; ++i) {
        if (dataTypeIds[i] == OMNI_VARCHAR) {
            dataTypes.push_back(std::make_shared<VarcharDataType>(50));
            continue;
        } else if (dataTypeIds[i] == OMNI_CHAR) {
            dataTypes.push_back(std::make_shared<CharDataType>(50));
            continue;
        }
        dataTypes.push_back(std::make_shared<DataType>(dataTypeIds[i]));
    }
}

VectorBatch* CreateInputData(const int32_t numRows,
                              const int32_t numCols,
                              int32_t* inputTypeIds,
                              int64_t* allData) 
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<omniruntime::vec::DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(omniruntime::vec::GetProcessGlobalVecAllocator(), inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_BOOLEAN:
                ((BooleanVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_LONG:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            case OMNI_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], numRows);
                break;
            case OMNI_SHORT:
                ((ShortVector *)vecBatch->GetVector(i))->SetValues(0, (int16_t *)allData[i], numRows);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                for (int j = 0; j < numRows; ++j) {
                    int64_t addr = (reinterpret_cast<int64_t *>(allData[i]))[j];
                    std::string s (reinterpret_cast<char *>(addr));
                    ((VarcharVector *)vecBatch->GetVector(i))->SetValue(j, (uint8_t *)(s.c_str()), s.length());
                }
                break;
            }
            case OMNI_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            default:{
                LogError("No such data type %d", inputTypeIds[i]);
            }
        }
    }
    return vecBatch;
}

VarcharVector *CreateVarcharVector(VarcharDataType type, std::string *values, int32_t length)
{
    VectorAllocator * vecAllocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    uint32_t width = type.GetWidth();
    VarcharVector *vector = std::make_unique<VarcharVector>(vecAllocator, length * width, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
    }
    return vector;
}

Decimal128Vector *CreateDecimal128Vector(Decimal128 *values, int32_t length)
{
    VectorAllocator *vecAllocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    Decimal128Vector *vector = std::make_unique<Decimal128Vector>(vecAllocator, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

Vector *CreateVector(DataType &vecType, int32_t rowCount, va_list &args)
{
    switch (vecType.GetId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            return CreateVector<IntVector>(va_arg(args, int32_t *), rowCount);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return CreateVector<LongVector>(va_arg(args, int64_t *), rowCount);
        case OMNI_DOUBLE:
            return CreateVector<DoubleVector>(va_arg(args, double *), rowCount);
        case OMNI_BOOLEAN:
            return CreateVector<BooleanVector>(va_arg(args, bool *), rowCount);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return CreateVarcharVector(static_cast<VarcharDataType &>(vecType), va_arg(args, std::string *), rowCount);
        case OMNI_DECIMAL128:
            return CreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount);
        default:
            std::cerr << "Unsupported type : " << vecType.GetId() << std::endl;
            return nullptr;
    }
}

DictionaryVector *CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    Vector *dictionary = CreateVector(dataType, rowCount, args);
    va_end(args);
    auto vec = new DictionaryVector(dictionary, ids, idsCount);
    delete dictionary;
    return vec;
}

Vector *buildVector(const DataType &aggType, int32_t rowNumber)
{
    VectorAllocator *vecAllocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    switch (aggType.GetId()) {
        case OMNI_NONE: {
            LongVector *col = new LongVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValueNull(j);
            }
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            IntVector *col = new IntVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            LongVector *col = new LongVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DOUBLE: {
            DoubleVector *col = new DoubleVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            BooleanVector *col = new BooleanVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            Decimal128Vector *col = new Decimal128Vector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, Decimal128(0, 1));
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharDataType charType = (VarcharDataType &)aggType;
            VarcharVector *col = new VarcharVector(vecAllocator, charType.GetWidth() * rowNumber, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                std::string str =  std::to_string(j);
                col->SetValue(j, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
            }
            return col;
        }
        default: {
            LogError("No such %d type support", aggType.GetId());
            return nullptr;
        }
    }
}

VectorBatch *CreateVectorBatch(const DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new VectorBatch(typesCount, rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataTypePtr type = types.GetType(i);
        vectorBatch->SetVector(i, CreateVector(*type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

/**
 * create a VectorBatch with 1 col 1 row varchar value and it's partition id
 * 
 * @param {int} pid partition id for this row
 * @param {string} inputString varchar row value
 * @return {VectorBatch} a VectorBatch
 */
VectorBatch* CreateVectorBatch_1row_varchar_withPid(int pid, std::string inputString) {
    // gen vectorBatch
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;
    const int32_t numRows = 1;
    auto* col1 = new int32_t[numRows];
    col1[0] = pid;
    auto* col2 = new int64_t[numRows];
    std::string* strTmp = new std::string(inputString);
    col2[0] = (int64_t)(strTmp->c_str());

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete strTmp;
    return in;
}

/**
 * create a VectorBatch with 4col OMNI_INT OMNI_LONG OMNI_DOUBLE OMNI_VARCHAR and it's partition id
 * 
 * @param {int} parNum partition number
 * @param {int} rowNum row number
 * @return {VectorBatch} a VectorBatch
 */
VectorBatch* CreateVectorBatch_4col_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    const int32_t numCols = 5;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_INT;
    inputTypes[2] = OMNI_LONG;
    inputTypes[3] = OMNI_DOUBLE;
    inputTypes[4] = OMNI_VARCHAR;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int32_t[numRows];
    auto* col2 = new int64_t[numRows];
    auto* col3 = new double[numRows];
    auto* col4 = new int64_t[numRows];
    string startStr = "_START_";
    string endStr = "_END_";

    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i+1) % partitionNum;
        col1[i] = i + 1;
        col2[i] = i + 1;
        col3[i] = i + 1;
        std::string* strTmp = new std::string(startStr + to_string(i + 1) + endStr);
        string_cache_test_.push_back(strTmp);
        col4[i] = (int64_t)((*strTmp).c_str());
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3),
                                reinterpret_cast<int64_t>(col4)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string_cache_test_.size(); p++) {
        delete string_cache_test_[p]; // release memory
    }
    return in;
}

VectorBatch* CreateVectorBatch_1FixCol_withPid(int parNum, int rowNum, int32_t fixColType) {
    int partitionNum = parNum;
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = fixColType;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i+1) % partitionNum;
        col1[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    return in; 
}

VectorBatch* CreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;
    inputTypes[2] = OMNI_INT;

    const int32_t numRows = 1;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    auto* col2 = new int32_t[numRows];

    col0[0] = pid;
    std::string* strTmp = new std::string(strVar);
    col1[0] = (int64_t)(strTmp->c_str());
    col2[0] = intVar;

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete strTmp;
    return in;
}

VectorBatch* CreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    const int32_t numCols = 5;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;
    inputTypes[2] = OMNI_VARCHAR;
    inputTypes[3] = OMNI_VARCHAR;
    inputTypes[4] = OMNI_VARCHAR;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    auto* col2 = new int64_t[numRows];
    auto* col3 = new int64_t[numRows];
    auto* col4 = new int64_t[numRows];

    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i+1) % partitionNum;
        std::string* strTmp1 = new std::string("Col1_START_" + to_string(i + 1) + "_END_");
        col1[i] = (int64_t)((*strTmp1).c_str());
        std::string* strTmp2 = new std::string("Col2_START_" + to_string(i + 1) + "_END_");
        col2[i] = (int64_t)((*strTmp2).c_str());
        std::string* strTmp3 = new std::string("Col3_START_" + to_string(i + 1) + "_END_");
        col3[i] = (int64_t)((*strTmp3).c_str());
        std::string* strTmp4 = new std::string("Col4_START_" + to_string(i + 1) + "_END_");
        col4[i] = (int64_t)((*strTmp4).c_str());
        string_cache_test_.push_back(strTmp1);
        string_cache_test_.push_back(strTmp2);
        string_cache_test_.push_back(strTmp3);
        string_cache_test_.push_back(strTmp4);
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3),
                                reinterpret_cast<int64_t>(col4)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string_cache_test_.size(); p++) {
        delete string_cache_test_[p]; // release memory
    }
    return in;
}

VectorBatch* CreateVectorBatch_4charCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    const int32_t numCols = 5;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_CHAR;
    inputTypes[2] = OMNI_CHAR;
    inputTypes[3] = OMNI_CHAR;
    inputTypes[4] = OMNI_CHAR;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    auto* col2 = new int64_t[numRows];
    auto* col3 = new int64_t[numRows];
    auto* col4 = new int64_t[numRows];

    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i+1) % partitionNum;
        std::string* strTmp1 = new std::string("Col1_CHAR_" + to_string(i + 1) + "_END_");
        col1[i] = (int64_t)((*strTmp1).c_str());
        std::string* strTmp2 = new std::string("Col2_CHAR_" + to_string(i + 1) + "_END_");
        col2[i] = (int64_t)((*strTmp2).c_str());
        std::string* strTmp3 = new std::string("Col3_CHAR_" + to_string(i + 1) + "_END_");
        col3[i] = (int64_t)((*strTmp3).c_str());
        std::string* strTmp4 = new std::string("Col4_CHAR_" + to_string(i + 1) + "_END_");
        col4[i] = (int64_t)((*strTmp4).c_str());
        string_cache_test_.push_back(strTmp1);
        string_cache_test_.push_back(strTmp2);
        string_cache_test_.push_back(strTmp3);
        string_cache_test_.push_back(strTmp4);
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3),
                                reinterpret_cast<int64_t>(col4)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string_cache_test_.size(); p++) {
        delete string_cache_test_[p]; // release memory
    }
    return in;
}

VectorBatch* CreateVectorBatch_5fixedCols_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;

    // gen vectorBatch
    const int32_t numCols = 6;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_BOOLEAN;
    inputTypes[2] = OMNI_SHORT;
    inputTypes[3] = OMNI_INT;
    inputTypes[4] = OMNI_LONG;
    inputTypes[5] = OMNI_DOUBLE;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new bool[numRows];
    auto* col2 = new int16_t[numRows];
    auto* col3 = new int32_t[numRows];
    auto* col4 = new int64_t[numRows];
    auto* col5 = new double[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = i % partitionNum;
        col1[i] = (i % 2) == 0 ? true : false;
        col2[i] = i + 1;
        col3[i] = i + 1;
        col4[i] = i + 1;
        col5[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3),
                                reinterpret_cast<int64_t>(col4),
                                reinterpret_cast<int64_t>(col5)};
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    return in; 
}

VectorBatch* CreateVectorBatch_2dictionaryCols_withPid(int partitionNum) {
    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {111, 112, 113, 114, 115, 116};
    int64_t data1[dataSize] = {221, 222, 223, 224, 225, 226};
    void *datas[2] = {data0, data1};
    DataTypes sourceTypes(std::vector<omniruntime::vec::DataTypePtr>({ std::make_unique<IntDataType>(), std::make_unique<LongDataType>()}));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vectorBatch = new VectorBatch(3, dataSize);
    VectorAllocator *allocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    IntVector *intVectorTmp = new IntVector(allocator, 6);
    for (int i = 0; i < intVectorTmp->GetSize(); i++) {
        intVectorTmp->SetValue(i, (i+1) % partitionNum);
    }
    for (int32_t i = 0; i < 3; i ++) {
        if (i == 0) {
            vectorBatch->SetVector(i, intVectorTmp);
        } else {
            omniruntime::vec::DataType dataType = *(sourceTypes.Get()[i - 1]);
            vectorBatch->SetVector(i, CreateDictionaryVector(dataType, dataSize, ids, dataSize, datas[i - 1]));
        }
    }
    return vectorBatch;
}

VectorBatch* CreateVectorBatch_1decimal128Col_withPid(int partitionNum, int rowNum) {
    auto decimal128InputVec = buildVector(Decimal128DataType(38, 2), rowNum);
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    IntVector *intVectorPid = new IntVector(allocator, rowNum);
    for (int i = 0; i < intVectorPid->GetSize(); i++) {
        intVectorPid->SetValue(i, (i+1) % partitionNum);
    }
    VectorBatch *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, intVectorPid);
    vecBatch->SetVector(1, decimal128InputVec);
    return vecBatch;
}

VectorBatch* CreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum) {
    auto decimal64InputVec = buildVector(Decimal64DataType(7, 2), rowNum);
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    IntVector *intVectorPid = new IntVector(allocator, rowNum);
    for (int i = 0; i < intVectorPid->GetSize(); i++) {
        intVectorPid->SetValue(i, (i+1) % partitionNum);
    }
    VectorBatch *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, intVectorPid);
    vecBatch->SetVector(1, decimal64InputVec);
    return vecBatch;
}

VectorBatch* CreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum) {
    auto decimal64InputVec = buildVector(Decimal64DataType(7, 2), rowNum);
    auto decimal128InputVec = buildVector(Decimal128DataType(38, 2), rowNum);
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    IntVector *intVectorPid = new IntVector(allocator, rowNum);
    for (int i = 0; i < intVectorPid->GetSize(); i++) {
        intVectorPid->SetValue(i, (i+1) % partitionNum);
    }
    VectorBatch *vecBatch = new VectorBatch(3);
    vecBatch->SetVector(0, intVectorPid);
    vecBatch->SetVector(1, decimal64InputVec);
    vecBatch->SetVector(2, decimal128InputVec);
    return vecBatch;
}

VectorBatch* CreateVectorBatch_someNullRow_vectorBatch() {
    const int32_t numRows = 6;
    bool data0[numRows] = {true, false, true, false, true, false};
    int16_t data1[numRows] = {0, 1, 2, 3, 4, 6};
    int32_t data2[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data3[numRows] = {0, 1, 2, 3, 4, 5};
    double data4[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data5[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    auto vec0 = CreateVector<BooleanVector>(data0, numRows);
    auto vec1 = CreateVector<ShortVector>(data1, numRows);
    auto vec2 = CreateVector<IntVector>(data2, numRows);
    auto vec3 = CreateVector<LongVector>(data3, numRows);
    auto vec4 = CreateVector<DoubleVector>(data4, numRows);
    auto vec5 = CreateVarcharVector(VarcharDataType(5), data5, numRows);
    for (int i = 0; i < numRows; i = i + 2) {
        vec0->SetValueNull(i);
        vec1->SetValueNull(i);
        vec2->SetValueNull(i);
        vec3->SetValueNull(i);
        vec4->SetValueNull(i);
        vec5->SetValueNull(i);
    }
    VectorBatch *vecBatch = new VectorBatch(6);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);
    vecBatch->SetVector(2, vec2);
    vecBatch->SetVector(3, vec3);
    vecBatch->SetVector(4, vec4);
    vecBatch->SetVector(5, vec5);
    return vecBatch;
}

VectorBatch* CreateVectorBatch_someNullCol_vectorBatch() {
    const int32_t numRows = 6;
    int32_t data1[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data2[numRows] = {0, 1, 2, 3, 4, 5};
    double data3[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data4[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    auto vec0 = CreateVector<IntVector>(data1, numRows);
    auto vec1 = CreateVector<LongVector>(data2, numRows);
    auto vec2 = CreateVector<DoubleVector>(data3, numRows);
    auto vec3 = CreateVarcharVector(VarcharDataType(5), data4, numRows);
    for (int i = 0; i < numRows; i = i + 1) {
        vec1->SetValueNull(i);
        vec3->SetValueNull(i);
    }
    VectorBatch *vecBatch = new VectorBatch(4);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);
    vecBatch->SetVector(2, vec2);
    vecBatch->SetVector(3, vec3);
    return vecBatch;
}

void Test_Shuffle_Compression(std::string compStr, int32_t numPartition, int32_t numVb, int32_t numRow) {
    std::string shuffleTestsDir = s_shuffle_tests_dir;
    std::string tmpDataFilePath = shuffleTestsDir + "/shuffle_" + compStr;
    if (!IsFileExist(shuffleTestsDir)) {
        mkdir(shuffleTestsDir.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);
    }
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = numPartition;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              compStr.c_str(),
                                              tmpDataFilePath,
                                              0,
                                              shuffleTestsDir);
    for (uint64_t j = 0; j < numVb; j++) {
        VectorBatch* vb = CreateVectorBatch_4col_withPid(partitionNum, numRow);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
    if (IsFileExist(tmpDataFilePath)) {
        remove(tmpDataFilePath.c_str());
    }
}

long Test_splitter_nativeMake(std::string partitioning_name,
                              int num_partitions,
                              InputDataTypes inputDataTypes,
                              int numCols,
                              int buffer_size,
                              const char* compression_type_jstr,
                              std::string data_file_jstr,
                              int num_sub_dirs,
                              std::string local_dirs_jstr) {
    auto splitOptions = SplitOptions::Defaults();
    if (buffer_size > 0) {
        splitOptions.buffer_size = buffer_size;
    }
    if (num_sub_dirs > 0) {
        splitOptions.num_sub_dirs = num_sub_dirs;
    }
    setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs_jstr.c_str(), 1);
    auto compression_type_result = GetCompressionType(compression_type_jstr);
    splitOptions.compression_type = compression_type_result;
    splitOptions.data_file = data_file_jstr;
    auto splitter = Splitter::Make(partitioning_name, inputDataTypes, numCols, num_partitions, std::move(splitOptions));
    return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));
}

int Test_splitter_split(long splitter_id, VectorBatch* vb) {
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    //初始化split各全局变量
    splitter->Split(*vb);
}

void Test_splitter_stop(long splitter_id) {
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        throw std::runtime_error("Test no splitter.");
    }
    splitter->Stop();
}

void Test_splitter_close(long splitter_id) {
    auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        throw std::runtime_error("Test no splitter.");
    }
    shuffle_splitter_holder_.Erase(splitter_id);
}

void GetFilePath(const char *path, const char *filename, char *filepath) {
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/') {
        strcat(filepath, "/");
    }
    strcat(filepath, filename);
}

void DeletePathAll(const char* path) {
    DIR *dir;
    struct dirent *dirInfo;
    struct stat statBuf;
    char filepath[256] = {0};
    lstat(path, &statBuf);
    if (S_ISREG(statBuf.st_mode)) {
        remove(path);
    } else if (S_ISDIR(statBuf.st_mode)) {
        if ((dir = opendir(path)) != NULL) {
            while ((dirInfo = readdir(dir)) != NULL) {
                GetFilePath(path, dirInfo->d_name, filepath);
                if (strcmp(dirInfo->d_name, ".") == 0 || strcmp(dirInfo->d_name, "..") == 0) {
                    continue;
                }
                DeletePathAll(filepath);
                rmdir(filepath);
            }
            closedir(dir);
            rmdir(path);
        }
    }
}