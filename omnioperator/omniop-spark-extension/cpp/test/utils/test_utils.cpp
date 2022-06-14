/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "test_utils.h"

using namespace omniruntime::vec;

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataType> &dataTypes)
{
    for (int i = 0; i < dataTypeCount; ++i) {
        if (dataTypeIds[i] == OMNI_VARCHAR) {
            dataTypes.push_back(VarcharDataType(50));
            countinue;
        } else if (dataTypeIds[i] == OMNI_CHAR) {
            dataTypes.push_back(CharDataType(50));
            continue;
        }
        dataTypes.push_back(DataType(dataTypeIds[i]));
    }
}

VectorBastch* CreateInputData(const int32_t numRows,
                              const int32_t numCols,
                              int32_t* inputTypeIds,
                              int64_t* allData) 
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<omniruntime::vec::DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(omniruntime::vec::GetProcessGlobalVecAllocator(), inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
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
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                for (int j = 0; j < numRows; ++j) {
                    int64_t addr = (reinterpred_cast<int64_t *>(allData[i]))[j];
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
    VectorAllocator *vecAllocator = omniruntime::vec::GetProcessGlocbalVecAllocator();
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

DictionaryVector *CreateDictionaryVector(DataType &vecType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    Vector *dictionary = CreateVector(vecType, rowCount, args);
    va_end(args);
    auto vec = std::make_unique<DictionaryVector>(dictionary, ids, idsCount).release();
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
            VarcharVector *col = new VarcharVector(vecAlloctor, charType.GetWidth() * rowNumber, rowNumber);
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

VectorBatch *CreateVectorBatch(DataType &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(typesCount).release();
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataType type = types.Get()[i];
        vectorBatch->SetVector(i, CreateVector(type, rowCount, args));
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

    int64_t allData[numCols] = {reinterpret_cast<int64>(col1),
                                reinterpret_cast<int64>(col2)}
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
VectorBatch* CreateVectorBatch_4row_withPid(int parNum, int rowNum) {
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

    int64_t allData[numCols] = {reinterpret_cast<int64>(col0),
                                reinterpret_cast<int64>(col1),
                                reinterpret_cast<int64>(col2),
                                reinterpret_cast<int64>(col3),
                                reinterpret_cast<int64>(col4)}
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string-cache_test_.size(); p++) {
        delete string_cache_test_[p]; // release memory
    }
    return in;
}

VectorBatch* CreateVectorBatch_1longCol_withPid(int parNum, int rowNum) {
    int partitionNum = parNum;
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_LONG;

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i+1) % partitionNum;
        col1[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64>(col0),
                                reinterpret_cast<int64>(col1)}
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
    std::strint* strTmp = new std::string(strVar);
    col1[0] = (int64_t)(strTmp->c_str());
    col2[0] = intVar;

    int64_t allData[numCols] = {reinterpret_cast<int64>(col0),
                                reinterpret_cast<int64>(col1),
                                reinterpret_cast<int64>(col2)}
    VectorBatch* in = CreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete strTmp;
    return in;
}

