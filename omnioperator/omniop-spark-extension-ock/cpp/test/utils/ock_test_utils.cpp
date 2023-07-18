/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <sstream>
#include <utility>

#include "ock_test_utils.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

/*void OckToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataType> &dataTypes)
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
}*/

VectorBatch *OckCreateInputData(const DataType &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vecBatch = new VectorBatch(rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i< typesCount; i++) {
        dataTypePtr = type = types.GetType(i);
        VectorBatch->Append(CreateVector(*type, rowCount, args).release());
    }
    va_end(args);
    return vecBatch;
}

std::unique_ptr<BaseVector> CreateVector(DataType &dataType, int32_t rowCount, va_list &args)
{
    return DYNAMIC_TYPE_DISPATCH(CreateFlatVector, dataType.GetId(), rowCount, args);
}


std::unique_ptr<BaseVector> CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount,
     ..)
{
    va_list args;
    va_start(args, idsCount);
    std::unique_ptr<BaseVector> dictionary = CreateVector(dataType, rowCount, args);
    va_end(args);
    return DYNAMIC_TYPE_DISPATCH(CreateDictionary, dataType.GetId(), dictionary.get(), ids, idsCount);
}

/*
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
}*/

BaseVector *OckNewbuildVector(const DataTypeId &typeId, int32_t rowNumber)
{
        switch (typeId) {
        case OMNI_SHORT: {
            return new Vector<uint16_t>(rowNumber);
        }
        case OMNI_NONE: {
            return new Vector<uint64_t>(rowNumber);
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            return new Vector<uint32_t>(rowNumber);
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            return new Vector<uint64_t>(rowNumber);
        }
        case OMNI_DOUBLE: {
            return new Vector<uint64_t>(rowNumber);
        }
        case OMNI_BOOLEAN: {
            return new Vector<uint8_t>(rowNumber);
        }
        case OMNI_DECIMAL128: {
            return new Vector<Decimal128>(rowNumber);
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            return new Vector<LargeStringContainer<std::string_view>>(rowNumber);
        }
        default: {
            LogError("No such %d type support", typeId);
            return nullptr;
        }
    }
}

VectorBatch *OckCreateVectorBatch(const DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new vecBatch(rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        dataTypePtr type = types.GetType(i);
        vectorBatch->Append(OckCreateVector(*type, rowCount, args).release());
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
    DataTypes inputTypes(std::vector<DataTypePtr>)({ IntType(), VarcharType()});
    const int32_t numRows = 1;
    auto *col1 = new int32_t[numRows];
    col1[0] = pid;
    auto *col2 = new std::string[numRows];
    col2[0] = std::move(inputString);
    VectorBatch *in = OckCreateInputData(inputTypes, numCols, col1, col2);
    delete[] col1;
    delete[] col2;
    return in;
}

VectorBatch *OckCreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;
    const int32_t numCols = 5;
    DataTypes inputTypes(std::vector<DataTypePtr>)({ IntType(), VarcharType(), VarcharType(), VarcharType(), VarcharType() });
    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new std::string[numRows];
    auto *col2 = new std::string[numRows];
    auto *col3 = new std::string[numRows];
    auto *col4 = new std::string[numRows];
    col0[i] = (i + 1) % partitionNum;
        std::string strTmp1 = std::string("Col1_START_" + to_string(i + 1) + "_END_");
        col1[i] = std::move(strTmp1);
        std::string strTmp2 = std::string("Col2_START_" + to_string(i + 1) + "_END_");
        col2[i] = std::move(strTmp2);
        std::string strTmp3 = std::string("Col3_START_" + to_string(i + 1) + "_END_");
        col3[i] = std::move(strTmp3);
        std::string strTmp4 = std::string("Col4_START_" + to_string(i + 1) + "_END_");
        col4[i] = std::move(strTmp4);
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
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
    DataTypes inputTypes(std::vector<DataTypePtr>)({ IntType(), VarcharType(), VarcharType(), VarcharType(), VarcharType() });
    
    const int32_t numRows = rowNum;
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int32_t[numRows];
    auto *col2 = new int64_t[numRows];
    auto *col3 = new double[numRows];
    auto *col4 = new std::string[numRows];
    std::string startStr = "_START_";
    std::string endStr = "_END_";
    std::vector<std::string*> string_cache_test_;
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
        col2[i] = i + 1;
        col3[i] = i + 1;
        std::string strTmp = std::string(startStr + to_string(i + 1) + endStr);
        col4[i] = std::move(strTmp);
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    return in;
}

VectorBatch* CreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar) {
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), VarcharType(), IntType() }));

    const int32_t numRows = 1;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new std::string[numRows];
    auto* col2 = new int32_t[numRows];

    col0[0] = pid;
    col1[0] = std::move(strVar);
    col2[0] = intVar;

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    return in;
}

VectorBatch *OckCreateVectorBatch_1fixedCols_withPid(int parNum, int rowNum, dataTypePtr fixColType)
{
    int partitionNum = parNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), std::move(fixColType) }));

    const int32_t numRows = rowNum;
    auto* col0 = new int32_t[numRows];
    auto* col1 = new int64_t[numRows];
    for (int i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = i + 1;
    }

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in; 
}

VectorBatch *OckCreateVectorBatch_5fixedCols_withPid(int parNum, int rowNum)
{
    int partitionNum = parNum;
    // gen vectorBatch
    DataTypes inputTypes(
        std::vector<DataTypePtr>({ IntType(), BooleanType(), ShortType(), IntType(), LongType(), DoubleType() }));

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

    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4, col5);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    return in; 
}

VectorBatch *OckCreateVectorBatch_2dictionaryCols_withPid(int partitionNum)
{
    // dictionary test
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    auto *col0 = new int32_t[dataSize];
    for (int32_t i = 0; i< dataSize; i++) {
        col0[i] = (i + 1) % partitionNum;
    }
    int32_t col1[dataSize] = {111, 112, 113, 114, 115, 116};
    int64_t col2[dataSize] = {221, 222, 223, 224, 225, 226};
    void *datas[2] = {col1, col2};
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};

    VectorBatch *vectorBatch = new VectorBatch(dataSize);
    auto Vec0 = CreateVector<int32_t>(dataSize, col0);
    vectorBatch->Append(Vec0.release());
    auto dicVec0 = CreateDictionaryVector(*sourceTypes.GetType(0), dataSize, ids, dataSize, datas[0]);
    auto dicVec1 = CreateDictionaryVector(*sourceTypes.GetType(1), dataSize, ids, dataSize, datas[1]);
    vectorBatch->Append(dicVec0.release());
    vectorBatch->Append(dicVec1.release());

    delete[] col0;
    return vectorBatch;
}

VectorBatch *OckCreateVectorBatch_1decimal128Col_withPid(int partitionNum)
{
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal128Type(38, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new Decimal128[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = Decimal128(0, 1);
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in;
}

VectorBatch *OckCreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum) {
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal64Type(7, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = 1;
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1);
    delete[] col0;
    delete[] col1;
    return in;
}

VectorBatch *OckCreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum) {
    const int32_t numRows = rowNum;
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), Decimal64Type(7, 2), Decimal128Type(38, 2) }));
    
    auto *col0 = new int32_t[numRows];
    auto *col1 = new int64_t[numRows];
    auto *col2 = new Decimal128[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = (i + 1) % partitionNum;
        col1[i] = 1;
        col2[i] = Decimal128(0, 1);
    }
    
    VectorBatch* in = CreateVectorBatch(inputTypes, numRows, col0, col1, col2);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    return in;
}

VectorBatch *OckCreateVectorBatch_someNullRow_vectorBatch()
{
    const int32_t numRows = 6;
    const int32_t numCols = 6;
    bool data0[numRows] = {true, false, true, false, true, false};
    int16_t data1[numRows] = {0, 1, 2, 3, 4, 6};
    int32_t data2[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data3[numRows] = {0, 1, 2, 3, 4, 5};
    double data4[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data5[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    DataTypes inputTypes(
        std::vector<DataTypePtr>({ BooleanType(), ShortType(), IntType(), LongType(), DoubleType(), VarcharType(5) }));
    VectorBatch* vecBatch = CreateVectorBatch(inputTypes, numRows, data0, data1, data2, data3, data4, data5);
    for (int32_t i = 0; i < numCols; i++) {
        for (int32_t j = 0; j < numRows; j = j + 2) {
            vecBatch->Get(i)->SetNull(j);
        }
    }
    return vecBatch;
}

VectorBatch *OckCreateVectorBatch_someNullCol_vectorBatch()
{
    const int32_t numRows = 6;
    const int32_t numCols = 4;
    int32_t data1[numRows] = {0, 1, 2, 0, 1, 2};
    int64_t data2[numRows] = {0, 1, 2, 3, 4, 5};
    double data3[numRows] = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5};
    std::string data4[numRows] = {"abcde", "fghij", "klmno", "pqrst", "", ""};

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), VarcharType(5) }));
    VectorBatch* vecBatch = CreateVectorBatch(inputTypes, numRows, data1, data2, data3, data4);
    for (int32_t i = 0; i < numCols; i = i + 2) {
        for (int32_t j = 0; j < numRows; j++) {
            vecBatch->Get(i)->SetNull(j);
        }
    }
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