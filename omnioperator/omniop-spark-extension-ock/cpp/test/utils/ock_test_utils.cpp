/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <sstream>
#include <utility>

#include "ock_test_utils.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

void OckToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataType> &dataTypes)
{
    for (int i = 0; i < dataTypeCount; ++i) {
        if (dataTypeIds[i] == OMNI_VARCHAR) {
            dataTypes.emplace_back(VarcharDataType(50));
            continue;
        } else if (dataTypeIds[i] == OMNI_CHAR) {
            dataTypes.emplace_back(CharDataType(50));
            continue;
        }
        dataTypes.emplace_back(DataType(dataTypeIds[i]));
    }
}

VectorBatch *OckCreateInputData(const int32_t numRows, const int32_t numCols, int32_t *inputTypeIds, int64_t *allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    std::vector<omniruntime::vec::DataType> inputTypes;
    OckToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(VectorAllocator::GetGlobalAllocator(), inputTypes);
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
                    int64_t addr = (reinterpret_cast<int64_t *>(allData[i]))[j];
                    std::string s(reinterpret_cast<char *>(addr));
                    ((VarcharVector *)vecBatch->GetVector(i))->SetValue(j, (uint8_t *)(s.c_str()), s.length());
                }
                break;
            }
            case OMNI_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            default: {
                LogError("No such data type %d", inputTypeIds[i]);
            }
        }
    }
    return vecBatch;
}

VarcharVector *OckCreateVarcharVector(VarcharDataType type, std::string *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    uint32_t width = type.GetWidth();
    VarcharVector *vector = std::make_unique<VarcharVector>(vecAllocator, length * width, length).release();
    uint32_t offset = 0;
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
        bool isNull = values[i].empty() ? true : false;
        vector->SetValueNull(i, isNull);
        vector->SetValueOffset(i, offset);
        offset += values[i].length();
    }

    if (length > 0) {
        vector->SetValueOffset(values->size(), offset);
    }

    std::stringstream offsetValue;
    offsetValue << "{ ";
    for (uint32_t index = 0; index < length; index++) {
        offsetValue << vector->GetValueOffset(index) << ", ";
    }

    offsetValue << vector->GetValueOffset(values->size()) << " }";

    LOG_INFO("%s", offsetValue.str().c_str());

    return vector;
}

Decimal128Vector *OckCreateDecimal128Vector(Decimal128 *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    Decimal128Vector *vector = std::make_unique<Decimal128Vector>(vecAllocator, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

Vector *OckCreateVector(DataType &vecType, int32_t rowCount, va_list &args)
{
    switch (vecType.GetId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            return OckCreateVector<IntVector>(va_arg(args, int32_t *), rowCount);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return OckCreateVector<LongVector>(va_arg(args, int64_t *), rowCount);
        case OMNI_DOUBLE:
            return OckCreateVector<DoubleVector>(va_arg(args, double *), rowCount);
        case OMNI_BOOLEAN:
            return OckCreateVector<BooleanVector>(va_arg(args, bool *), rowCount);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return OckCreateVarcharVector(static_cast<VarcharDataType &>(vecType), va_arg(args, std::string *),
                rowCount);
        case OMNI_DECIMAL128:
            return OckCreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount);
        default:
            std::cerr << "Unsupported type : " << vecType.GetId() << std::endl;
            return nullptr;
    }
}

DictionaryVector *OckCreateDictionaryVector(DataType &vecType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    Vector *dictionary = OckCreateVector(vecType, rowCount, args);
    va_end(args);
    auto vec = std::make_unique<DictionaryVector>(dictionary, ids, idsCount).release();
    delete dictionary;
    return vec;
}

Vector *OckbuildVector(const DataType &aggType, int32_t rowNumber)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    switch (aggType.GetId()) {
        case OMNI_SHORT: {
            auto *col = new ShortVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValueNull(j);
            }
            return col;
            break;
        }
        case OMNI_NONE: {
            auto *col = new LongVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValueNull(j);
            }
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            auto *col = new IntVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            auto *col = new LongVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DOUBLE: {
            auto *col = new DoubleVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            auto *col = new BooleanVector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            auto *col = new Decimal128Vector(vecAllocator, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                col->SetValue(j, Decimal128(0, 1));
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharDataType charType = (VarcharDataType &)aggType;
            auto *col = new VarcharVector(vecAllocator, charType.GetWidth() * rowNumber, rowNumber);
            for (int32_t j = 0; j < rowNumber; ++j) {
                std::string str = std::to_string(j);
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

Vector *OckNewbuildVector(const DataTypeId &typeId, int32_t rowNumber)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    switch (typeId) {
        case OMNI_SHORT: {
            auto *col = new ShortVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_NONE: {
            auto *col = new LongVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            auto *col = new IntVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            auto *col = new LongVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_DOUBLE: {
            auto *col = new DoubleVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_BOOLEAN: {
            auto *col = new BooleanVector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_DECIMAL128: {
            auto *col = new Decimal128Vector(vecAllocator, rowNumber);
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharDataType charType = (VarcharDataType &)typeId;
            auto *col = new VarcharVector(vecAllocator, charType.GetWidth() * rowNumber, rowNumber);
            return col;
        }
        default: {
            LogError("No such %d type support", typeId);
            return nullptr;
        }
    }
}

VectorBatch *OckCreateVectorBatch(DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(typesCount).release();
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataType type = types.Get()[i];
        vectorBatch->SetVector(i, OckCreateVector(type, rowCount, args));
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
VectorBatch *OckCreateVectorBatch_1row_varchar_withPid(int pid, const std::string &inputString)
{
    // gen vectorBatch
    const int32_t numCols = 2;
    auto inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;

    const int32_t numRows = 1;
    auto *col1 = new int32_t[numRows];
    col1[0] = pid;
    auto *col2 = new int64_t[numRows];
    auto *strTmp = new std::string(std::move(inputString));
    col2[0] = (int64_t)(strTmp->c_str());

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
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
VectorBatch *OckCreateVectorBatch_4col_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;
    const int32_t numCols = 6;
    auto *inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_INT;
    inputTypes[2] = OMNI_LONG;
    inputTypes[3] = OMNI_DOUBLE;
    inputTypes[4] = OMNI_VARCHAR;
    inputTypes[5] = OMNI_SHORT;

    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int32_t[numRows];
    auto *col2 = new int64_t[numRows];
    auto *col3 = new double[numRows];
    auto *col4 = new int64_t[numRows];
    auto *col5 = new int16_t[numRows];
    std::string startStr = "_START_";
    std::string endStr = "_END_";

    std::vector<std::string *> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
        col2[i] = i + 1;
        col3[i] = i + 1;
        auto *strTmp = new std::string(startStr + std::to_string(i + 1) + endStr);
        string_cache_test_.push_back(strTmp);
        col4[i] = (int64_t)((*strTmp).c_str());
        col5[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3),
                                reinterpret_cast<int64_t>(col4),
                                reinterpret_cast<int64_t>(col5)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string_cache_test_.size(); p++) {
        delete string_cache_test_[p]; // 释放内存
    }
    return in;
}

VectorBatch *OckCreateVectorBatch_1longCol_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;
    const int32_t numCols = 2;
    auto *inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_LONG;

    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    for (int i = 0; i < 2; i++) {
        delete (int64_t *)allData[i]; // 释放内存
    }
    return in;
}

VectorBatch *OckCreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar)
{
    const int32_t numCols = 3;
    auto *inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;
    inputTypes[2] = OMNI_INT;

    const int32_t numRows = 1;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    auto *col2 = new int32_t[numRows];

    col0[0] = pid;
    auto *strTmp = new std::string(strVar);
    col1[0] = (int64_t)(strTmp->c_str());
    col2[0] = intVar;

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete strTmp;
    return in;
}

VectorBatch *OckCreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;
    const int32_t numCols = 5;
    auto *inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_VARCHAR;
    inputTypes[2] = OMNI_VARCHAR;
    inputTypes[3] = OMNI_VARCHAR;
    inputTypes[4] = OMNI_VARCHAR;

    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    auto *col2 = new int64_t[numRows];
    auto *col3 = new int64_t[numRows];
    auto *col4 = new int64_t[numRows];

    std::vector<std::string *> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        auto *strTmp1 = new std::string("Col1_START_" + std::to_string(i + 1) + "_END_");
        col1[i] = (int64_t)((*strTmp1).c_str());
        auto *strTmp2 = new std::string("Col2_START_" + std::to_string(i + 1) + "_END_");
        col2[i] = (int64_t)((*strTmp2).c_str());
        auto *strTmp3 = new std::string("Col3_START_" + std::to_string(i + 1) + "_END_");
        col3[i] = (int64_t)((*strTmp3).c_str());
        auto *strTmp4 = new std::string("Col4_START_" + std::to_string(i + 1) + "_END_");
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
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;

    for (int p = 0; p < string_cache_test_.size(); p++) {
        delete string_cache_test_[p]; // 释放内存
    }
    return in;
}

VectorBatch *OckCreateVectorBatch_1fixedCols_withPid(int parNum, int32_t rowNum)
{
    int partitionNum = parNum;

    // gen vectorBatch
    const int32_t numCols = 1;
    auto *inputTypes = new int32_t[numCols];
    // inputTypes[0] = OMNI_INT;
    inputTypes[0] = OMNI_LONG;

    const uint32_t numRows = rowNum;

    std::cout << "gen row " << numRows << std::endl;
    // auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        // col0[i] = 0; // i % partitionNum;
        col1[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    // delete[] col0;
    delete[] col1;
    return in;
}

VectorBatch *OckCreateVectorBatch_3fixedCols_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;

    // gen vectorBatch
    const int32_t numCols = 4;
    auto *inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_INT;
    inputTypes[1] = OMNI_INT;
    inputTypes[2] = OMNI_LONG;
    inputTypes[3] = OMNI_DOUBLE;

    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int32_t[numRows];
    auto *col2 = new int64_t[numRows];
    auto *col3 = new double[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = i % partitionNum;
        col1[i] = i + 1;
        col2[i] = i + 1;
        col3[i] = i + 1;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0),
                                reinterpret_cast<int64_t>(col1),
                                reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3)};
    VectorBatch *in = OckCreateInputData(numRows, numCols, inputTypes, allData);
    delete[] inputTypes;
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    return in;
}

VectorBatch *OckCreateVectorBatch_2dictionaryCols_withPid(int partitionNum)
{
    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {111, 112, 113, 114, 115, 116};
    int64_t data1[dataSize] = {221, 222, 223, 224, 225, 226};
    int64_t data2[dataSize] = {111, 222, 333, 444, 555, 666};
    Decimal128 data3[dataSize] = {Decimal128(0, 1), Decimal128(0, 2), Decimal128(0, 3), Decimal128(0, 4), Decimal128(0, 5), Decimal128(0, 6)};
    void *datas[4] = {data0, data1, data2, data3};

    DataTypes sourceTypes(std::vector<omniruntime::vec::DataType>({ IntDataType(), LongDataType(), Decimal64DataType(7, 2), Decimal128DataType(38, 2)}));

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto vectorBatch = new VectorBatch(5, dataSize);
    VectorAllocator *allocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    auto intVectorTmp = new IntVector(allocator, 6);
    for (int i = 0; i < intVectorTmp->GetSize(); i++) {
        intVectorTmp->SetValue(i, (i + 1) % partitionNum);
    }
    for (int32_t i = 0; i < 5; i++) {
        if (i == 0) {
            vectorBatch->SetVector(i, intVectorTmp);
        } else {
            omniruntime::vec::DataType dataType = sourceTypes.Get()[i - 1];
            vectorBatch->SetVector(i, OckCreateDictionaryVector(dataType, dataSize, ids, dataSize, datas[i - 1]));
        }
    }
    return vectorBatch;
}

VectorBatch *OckCreateVectorBatch_1decimal128Col_withPid(int partitionNum)
{
    int32_t ROW_PER_VEC_BATCH = 999;
    auto decimal128InputVec = OckbuildVector(Decimal128DataType(38, 2), ROW_PER_VEC_BATCH);
    VectorAllocator *allocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    auto *intVectorPid = new IntVector(allocator, ROW_PER_VEC_BATCH);
    for (int i = 0; i < intVectorPid->GetSize(); i++) {
        intVectorPid->SetValue(i, (i + 1) % partitionNum);
    }
    auto *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, intVectorPid);
    vecBatch->SetVector(1, decimal128InputVec);
    return vecBatch;
}

VectorBatch *OckCreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum) {
    auto decimal64InputVec = OckbuildVector(Decimal64DataType(7, 2), rowNum);
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

VectorBatch *OckCreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum) {
    auto decimal64InputVec = OckbuildVector(Decimal64DataType(7, 2), rowNum);
    auto decimal128InputVec = OckbuildVector(Decimal128DataType(38, 2), rowNum);
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

VectorBatch *OckCreateVectorBatch_someNullRow_vectorBatch()
{
    const int32_t numRows = 6;
    int32_t data1[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data2[numRows] = {0, 1, 2, 3, 4, 5};
    double data3[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data4[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    auto vec0 = OckCreateVector<IntVector>(data1, numRows);
    auto vec1 = OckCreateVector<LongVector>(data2, numRows);
    auto vec2 = OckCreateVector<DoubleVector>(data3, numRows);
    auto vec3 = OckCreateVarcharVector(VarcharDataType(varcharType), data4, numRows);
    for (int i = 0; i < numRows; i = i + 2) {
        vec0->SetValueNull(i, false);
        vec1->SetValueNull(i, false);
        vec2->SetValueNull(i, false);
    }
    auto *vecBatch = new VectorBatch(4);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);
    vecBatch->SetVector(2, vec2);
    vecBatch->SetVector(3, vec3);
    return vecBatch;
}

VectorBatch *OckCreateVectorBatch_someNullCol_vectorBatch()
{
    const int32_t numRows = 6;
    int32_t data1[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data2[numRows] = {0, 1, 2, 3, 4, 5};
    double data3[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data4[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    auto vec0 = OckCreateVector<IntVector>(data1, numRows);
    auto vec1 = OckCreateVector<LongVector>(data2, numRows);
    auto vec2 = OckCreateVector<DoubleVector>(data3, numRows);
    auto vec3 = OckCreateVarcharVector(VarcharDataType(varcharType), data4, numRows);
    for (int i = 0; i < numRows; i = i + 1) {
        vec1->SetValueNull(i);
        vec3->SetValueNull(i);
    }
    auto *vecBatch = new VectorBatch(4);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);
    vecBatch->SetVector(2, vec2);
    vecBatch->SetVector(3, vec3);
    return vecBatch;
}

void OckTest_Shuffle_Compression(std::string compStr, int32_t partitionNum, int32_t numVb, int32_t numRow)
{
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};

    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), true, 40960, 41943040, 134217728);

    for (uint64_t j = 0; j < numVb; j++) {
        VectorBatch *vb = OckCreateVectorBatch_4col_withPid(partitionNum, numRow);
        OckTest_splitter_split(splitterId, vb);
    }

    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

long OckTest_splitter_nativeMake(std::string partitionMethod, int partitionNum, const int32_t *colTypeIds, int colNum,
    bool isCompress, uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity)
{
    std::string appId = "application_1647507332264_0880";

    LOG_INFO("col num %d", colNum);

    auto splitter = ock::dopspark::OckSplitter::Make(partitionMethod, partitionNum, colTypeIds, colNum, 0);
    if (splitter == nullptr) {
        LOG_ERROR("Failed to make ock splitter");
        return -1;
    }

    bool ret = splitter->SetShuffleInfo(appId, 0, 0, 0, 1, 1);
    if (UNLIKELY(!ret)) {
        throw std::logic_error("Failed to set shuffle information");
    }

    ret = splitter->InitLocalBuffer(regionSize, minCapacity, maxCapacity, isCompress);
    if (UNLIKELY(!ret)) {
        throw std::logic_error("Failed to initialize local buffer");
    }

    return Ockshuffle_splitter_holder_.Insert(std::shared_ptr<ock::dopspark::OckSplitter>(splitter));
}

int OckTest_splitter_split(long splitter_id, VectorBatch *vb)
{
    auto splitter = Ockshuffle_splitter_holder_.Lookup(splitter_id);
    // 初始化split各全局变量
    splitter->Split(*vb);
    return 0;
}

ock::dopspark::OckHashWriteBuffer *OckGetLocalBuffer(long splitterId)
{
    auto splitter = Ockshuffle_splitter_holder_.Lookup(splitterId);
    if (UNLIKELY(splitter == nullptr)) {
        LOG_ERROR("Can't find splitter for id %lu", splitterId);
        return nullptr;
    }

    return splitter->mOckBuffer;
}

void OckTest_splitter_stop(long splitter_id)
{
    auto splitter = Ockshuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        throw std::runtime_error("Test no splitter.");
    }

    const std::vector<int64_t> &pLengths = splitter->PartitionLengths();
    for (auto length : pLengths) {
    };

    splitter->Stop();
}

void OckTest_splitter_close(long splitter_id)
{
    auto splitter = Ockshuffle_splitter_holder_.Lookup(splitter_id);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
        throw std::runtime_error("Test no splitter.");
    }
    Ockshuffle_splitter_holder_.Erase(splitter_id);
}