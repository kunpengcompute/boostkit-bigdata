/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <shuffle/ock_vector.h>
#include "gtest/gtest.h"
#include "../utils/ock_test_utils.h"
#include "sdk/ock_shuffle_sdk.h"
#include "../../src/jni/OckShuffleJniReader.cpp"

static ConcurrentMap<void *> gLocalBlobMap;
static bool gIsCompress = true;
static uint32_t gLocalBlobSize = 0;
static int gTempSplitId = 0;
static int32_t *gVecTypeIds = nullptr;
static uint32_t gColNum = 0;

using namespace ock::dopspark;
using ValidateResult = bool (*)();

bool PrintVectorBatch(uint8_t **startAddress, uint32_t &length)
{
    uint8_t *address = *startAddress;
    auto *vbDesc = (VBDataHeaderDesc *)address;
    if (UNLIKELY(vbDesc == nullptr)) {
        LOG_ERROR("Invalid address for vb data address for reader id ");
        return false;
    }

    address += sizeof(VBDataHeaderDesc);

    uint32_t rowNum = vbDesc->rowNum;
    length = vbDesc->length;
    LOG_INFO("Get vector batch { row_num: %d, length: %d address %lu}", rowNum, length, (int64_t)vbDesc);

    std::shared_ptr<OckMergeReader> instance = std::make_shared<OckMergeReader>();
    if (UNLIKELY(instance == nullptr)) {
        LOG_ERROR("Invalid address for vb data address for reader id ");
        return false;
    }

    bool result = instance->Initialize(gVecTypeIds, gColNum);
    if (UNLIKELY(!result)) {
        LOG_ERROR("Invalid address for vb data address for reader id ");
        return false;
    }
    if (UNLIKELY(!instance->GetMergeVectorBatch(*startAddress, length, 256, 256))) {
        LOG_ERROR("GetMergeVectorBatch fails ");
    };
    rowNum = instance->GetRowNumAfterMerge();
    uint32_t vblength = instance->GetVectorBatchLength();

    std::stringstream info;
    info << "vector_batch: { ";
    for (uint32_t colIndex = 0; colIndex < gColNum; colIndex++) {
        auto typeId = static_cast<DataTypeId>(gVecTypeIds[colIndex]);
        BaseVector *vector = OckNewbuildVector(typeId, rowNum);
        if (typeId == OMNI_VARCHAR) {
            uint32_t varlength = 0;
            instance->CalVectorValueLength(colIndex, varlength);
            LOG_INFO("varchar vector value length : %d", varlength);
        }

        if(UNLIKELY(!instance->CopyDataToVector(vector, colIndex))) {
            LOG_ERROR("CopyDataToVector fails ");
        }

        if (rowNum > 999) {
            continue;
        }
        LOG_DEBUG("typeId %d OMNI_INT: %d OMNI_LONG %d OMNI_DOUBLE %d OMNI_VARCHAR %d", typeId, OMNI_INT, OMNI_LONG,
            OMNI_DOUBLE, OMNI_VARCHAR);

        info << "vector length:" << instance->GetVectorBatchLength() << "colIndex" << colIndex << ": { ";
        for (uint32_t rowIndex = 0; rowIndex < rowNum; rowIndex++) {
            LOG_DEBUG("%d", const_cast<uint8_t*>((uint8_t*)(VectorHelper::GetNullsAddr(vector)))[rowIndex]);
            info << "{ rowIndex: " << rowIndex << ", nulls: " <<
                std::to_string(const_cast<uint8_t*>((uint8_t*)(omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vector)))[rowIndex]);
            switch (typeId) {
                case OMNI_SHORT:
                    info << ", value: " << static_cast<Vector<uint16_t> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                case OMNI_INT: {
                    info << ", value: " << static_cast<Vector<uint32_t> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                }
                case OMNI_LONG: {
                    info << ", value: " << static_cast<Vector<uint64_t> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                }
                case OMNI_DOUBLE: {
                    info << ", value: " << static_cast<Vector<uint64_t> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                }
                case OMNI_DECIMAL64: {
                    info << ", value: " << static_cast<Vector<uint64_t> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                }
                case OMNI_DECIMAL128: {
                    info << ", value: " << static_cast<Vector<Decimal128> *>(vector)->GetValue(rowIndex) << " }, ";
                    break;
                }
                case OMNI_VARCHAR: { // unknown length for value vector, calculate later
                    // will add offset_vector_len when the length of values_vector is variable
                    LOG_DEBUG("hello %lu", (int64_t)vector->GetValues());
                    LOG_DEBUG("value %s, address %lu, offset %d, length %d",
                        std::string((char *)vector->GetValues()).c_str(), (int64_t)vector->GetValues(),
                        vector->GetValueOffset(rowIndex),
                        vector->GetValueOffset(rowIndex + 1) - vector->GetValueOffset(rowIndex));
                    LOG_DEBUG("offset %d", vector->GetValueOffset(rowIndex));
              /*      valueAddress = static_cast<uint8_t *>(vector->GetValues());
                    if (vector->GetValueOffset(rowIndex) == 0) {
                        info << ", value: null, offset 0";
                    } else {
                        info << ", value: " <<
                             std::string((char *)((uint8_t *)valueAddress), vector->GetValueOffset(rowIndex)) <<
                             ", offset: " << vector->GetValueOffset(rowIndex) << " }, ";
                        valueAddress += vector->GetValueOffset(rowIndex);
                    }*/
                    uint8_t *valueAddress = nullptr;
                    int32_t length = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    std::string valueString(valueAddress, valueAddress + length);
                    uint32_t length = 0;
                    std::string_view value;
                    if (!vc->IsNull(rowIndex)) {
                        value = vc->GetValue();
                        valueAddress = reinterpret_cast<uint8_t *>(reinterpret_cast<uint64_t>(value.data()));
                        length = static_cast<uint32_t>(value.length());
                    }
                    info << ", value: " << value << " }, ";
                    break;
                }
                default:
                    LOG_ERROR("Unexpected ");
                    return false;
            }
        }
        info << "}";
    }
    info << " }";

    LOG_INFO("%s", info.str().c_str());
    std::cout << std::endl;

    return true;
}

static uint32_t DecodeBigEndian32(const uint8_t *buf)
{
    uint64_t result = 0;
    for (uint32_t index = 0; index < sizeof(uint32_t); index++) {
        result |= (static_cast<uint32_t>(static_cast<unsigned char>(buf[index])) << (24 - index * 8));
    }

    return result;
}

static bool PrintfLocalBlobMetaInfo(int splitterId)
{
    OckHashWriteBuffer *buffer = OckGetLocalBuffer(splitterId);
    if (UNLIKELY(buffer == nullptr)) {
        LOG_ERROR("Invalid buffer for splitter id %d", splitterId);
        return false;
    }

    auto regionPtRecord = reinterpret_cast<uint32_t *>(buffer->mBaseAddress + buffer->mRegionPtRecordOffset);
    auto regionUsedRecord = reinterpret_cast<uint32_t *>(buffer->mBaseAddress + buffer->mRegionUsedRecordOffset);

    std::stringstream metaInfo;
    metaInfo << "{ partition_num: " << buffer->mPartitionNum << ", regions: [";
    // write meta information for those partition regions in the local blob
    for (uint32_t index = 0; index < buffer->mPartitionNum; index++) {
        metaInfo << "{regionId: " << index << ", partitionId: " <<
            DecodeBigEndian32((uint8_t *)&regionPtRecord[index]) << ", size: " <<
            DecodeBigEndian32((uint8_t *)&regionUsedRecord[index]) << "},";
    }
    metaInfo << "};";

    LOG_INFO("%s", metaInfo.str().c_str());
    std::cout << std::endl;

    for (uint32_t index = 0; index < buffer->mPartitionNum; index++) {
        uint32_t regionSize = buffer->mRegionUsedSize[index];
        if (regionSize == 0) {
            continue;
        }

        uint8_t *address = (index % 2) ?
            (buffer->mBaseAddress + (index + 1) * buffer->mEachPartitionSize - regionSize) :
            (buffer->mBaseAddress + buffer->mEachPartitionSize * index);

        LOG_DEBUG("buffer base_address: %lu, capacity: %d, each_region_capacity: %d, region_address: %lu, size: %d, "
            "index %d, compress %d",
            (int64_t)buffer->mBaseAddress, buffer->mDataCapacity, buffer->mEachPartitionSize, (int64_t)address,
            regionSize, index, buffer->IsCompress());

        while (regionSize > 0) {
            uint32_t length = 0;
            if (!PrintVectorBatch(&address, length)) {
                LOG_ERROR("Failed to print vector batch");
                return false;
            }

            regionSize -= length;
        }
    }

    return true;
}

class OckShuffleTest : public testing::Test {
protected:
    static int ShuffleLocalBlobGet(const char *ns, const char *taskId, uint64_t size, uint32_t partitionNums,
        uint32_t flags, uint64_t *blobId)
    {
        void *address = malloc(size);
        if (UNLIKELY(address == nullptr)) {
            LOG_ERROR("Failed to malloc local blob for taskId %s with size %lu", taskId, size);
            return -1;
        }

        gLocalBlobSize = size;

        *blobId = gLocalBlobMap.Insert(address);
        return 0;
    }

    static int ShuffleLocalBlobCommit(const char *ns, uint64_t blobId, uint32_t flags, uint32_t mapId, uint32_t taskId,
        uint32_t partitionNum, uint32_t stageId, uint8_t stageAttemptNumber, uint32_t offset, uint32_t *metric)
    {
        uint8_t *address = reinterpret_cast<uint8_t *>(gLocalBlobMap.Lookup(blobId));
        if (UNLIKELY(!address)) {
            LOG_ERROR("Failed to get address for blob id %lu", blobId);
            return -1;
        }

        PrintfLocalBlobMetaInfo(gTempSplitId);

        free(address);
        return 0;
    }

    static int ShuffleBlobObtainRawAddress(uint64_t blobId, void **ptr, const char *ns)
    {
        *ptr = gLocalBlobMap.Lookup(blobId);
        if (UNLIKELY(!*ptr)) {
            LOG_ERROR("Failed to get address for blob id %lu", blobId);
            return -1;
        }

        return 0;
    }

    static int ShuffleBlobReleaseRawAddress(uint64_t blobId, void *ptr)
    {
        gLocalBlobMap.Erase(blobId);
        return 0;
    }

    // run before first case...
    static void SetUpTestSuite()
    {
        if (UNLIKELY(!OckShuffleSdk::Initialize())) {
             throw std::logic_error("Failed to load ock shuffle library.");
        }

        // repoint to stub function
        OckShuffleSdk::mMapBlobFun = ShuffleBlobObtainRawAddress;
        OckShuffleSdk::mUnmapBlobFun = ShuffleBlobReleaseRawAddress;
        OckShuffleSdk::mGetLocalBlobFun = ShuffleLocalBlobGet;
        OckShuffleSdk::mCommitLocalBlobFun = ShuffleLocalBlobCommit;
    }

    // run after last case...
    static void TearDownTestSuite() {}

    // run before each case...
    virtual void SetUp() override {}

    // run after each case...
    virtual void TearDown() override {}
};

TEST_F(OckShuffleTest, Split_SingleVarChar)
{
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR};
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", 4, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 36, 176, 512);
    VectorBatch *vb1 = OckCreateVectorBatch_1row_varchar_withPid(3, "A");
    gTempSplitId = splitterId; // very important
    OckTest_splitter_split(splitterId, vb1);
    VectorBatch *vb2 = OckCreateVectorBatch_1row_varchar_withPid(1, "B");
    OckTest_splitter_split(splitterId, vb2);
    VectorBatch *vb3 = OckCreateVectorBatch_1row_varchar_withPid(3, "C");
    OckTest_splitter_split(splitterId, vb3);
    VectorBatch *vb4 = OckCreateVectorBatch_1row_varchar_withPid(3, "D");
    OckTest_splitter_split(splitterId, vb4);
    VectorBatch *vb5 = OckCreateVectorBatch_1row_varchar_withPid(1, "E"); // will get new region, cost 3
    OckTest_splitter_split(splitterId, vb5);
    VectorBatch *vb6 = OckCreateVectorBatch_1row_varchar_withPid(2, "F"); //
    OckTest_splitter_split(splitterId, vb6);
    VectorBatch *vb7 = OckCreateVectorBatch_1row_varchar_withPid(0, "G"); // will get new blob, cost 1
    OckTest_splitter_split(splitterId, vb7);
    VectorBatch *vb8 = OckCreateVectorBatch_1row_varchar_withPid(3, "H"); //
    OckTest_splitter_split(splitterId, vb8);
    VectorBatch *vb9 = OckCreateVectorBatch_1row_varchar_withPid(3, "I"); //
    OckTest_splitter_split(splitterId, vb9);
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Fixed_Long_Cols)
{
    int32_t inputVecTypeIds[] = {OMNI_LONG}; // 8Byte + 1Byte
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int partitionNum = 1;
    int splitterId = OckTest_splitter_nativeMake("single", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    // for (uint64_t j = 0; j < 999; j++) {
    VectorBatch *vb = OckCreateVectorBatch_1fixedCols_withPid(partitionNum, 10000, LongType());
    OckTest_splitter_split(splitterId, vb);
    // }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Fixed_Cols)
{
    int32_t inputVecTypeIds[] = {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE}; // 4Byte + 8Byte + 8Byte + 3Byte
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int partitionNum = 4;
    int splitterId = OckTest_splitter_nativeMake("hash", 4, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
                               // for (uint64_t j = 0; j < 999; j++) {
    VectorBatch *vb = OckCreateVectorBatch_5fixedCols_withPid(partitionNum, 999);
    OckTest_splitter_split(splitterId, vb);
    // }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Fixed_SinglePartition_SomeNullRow)
{
    int32_t inputVecTypeIds[] = {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR}; // 4 + 8 + 8 + 4 + 4
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int partitionNum = 1;
    int splitterId = OckTest_splitter_nativeMake("single", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
                               // for (uint64_t j = 0; j < 100; j++) {
    VectorBatch *vb = OckCreateVectorBatch_someNullRow_vectorBatch();
    OckTest_splitter_split(splitterId, vb);
    // }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Fixed_SinglePartition_SomeNullCol)
{
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int partitionNum = 1;
    int splitterId = OckTest_splitter_nativeMake("single", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch *vb = OckCreateVectorBatch_someNullCol_vectorBatch();
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Mix_LargeSize)
{
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR, OMNI_SHORT};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
                               // for (uint64_t j = 0; j < 999; j++) {
    VectorBatch *vb = OckCreateVectorBatch_4col_withPid(partitionNum, 999);
    OckTest_splitter_split(splitterId, vb);
    // }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Long_10WRows)
{
    int32_t inputVecTypeIds[] = {OMNI_LONG};
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int partitionNum = 10;
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch *vb = OckCreateVectorBatch_1fixedCols_withPid(partitionNum, 10000, LongType());
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_VarChar_LargeSize)
{
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 99; j++) {
        VectorBatch *vb = OckCreateVectorBatch_4varcharCols_withPid(partitionNum, 99);
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_VarChar_First)
{
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_INT};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), true, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    VectorBatch *vb0 = OckCreateVectorBatch_2column_1row_withPid(0, "corpbrand #4", 1);
    OckTest_splitter_split(splitterId, vb0);
    VectorBatch *vb1 = OckCreateVectorBatch_2column_1row_withPid(3, "brandmaxi #4", 1);
    OckTest_splitter_split(splitterId, vb1);
    VectorBatch *vb2 = OckCreateVectorBatch_2column_1row_withPid(1, "edu packnameless #9", 1);
    OckTest_splitter_split(splitterId, vb2);
    VectorBatch *vb3 = OckCreateVectorBatch_2column_1row_withPid(1, "amalgunivamalg #11", 1);
    OckTest_splitter_split(splitterId, vb3);
    VectorBatch *vb4 = OckCreateVectorBatch_2column_1row_withPid(0, "brandcorp #2", 1);
    OckTest_splitter_split(splitterId, vb4);
    VectorBatch *vb5 = OckCreateVectorBatch_2column_1row_withPid(0, "scholarbrand #2", 1);
    OckTest_splitter_split(splitterId, vb5);
    VectorBatch *vb6 = OckCreateVectorBatch_2column_1row_withPid(2, "edu packcorp #6", 1);
    OckTest_splitter_split(splitterId, vb6);
    VectorBatch *vb7 = OckCreateVectorBatch_2column_1row_withPid(2, "edu packamalg #1", 1);
    OckTest_splitter_split(splitterId, vb7);
    VectorBatch *vb8 = OckCreateVectorBatch_2column_1row_withPid(0, "brandnameless #8", 1);
    OckTest_splitter_split(splitterId, vb8);
    VectorBatch *vb9 = OckCreateVectorBatch_2column_1row_withPid(2, "univmaxi #2", 1);
    OckTest_splitter_split(splitterId, vb9);
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_Dictionary)
{
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 2; j++) {
        VectorBatch *vb = OckCreateVectorBatch_2dictionaryCols_withPid(partitionNum);
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F(OckShuffleTest, Split_OMNI_DECIMAL128)
{
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL128};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
        sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 2; j++) {
        VectorBatch *vb = OckCreateVectorBatch_1decimal128Col_withPid(partitionNum, 999);
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F  (OckShuffleTest, Split_Decimal64) {
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL64};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
                                                 sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]), true, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 2; j++) {
        VectorBatch *vb = OckCreateVectorBatch_1decimal64Col_withPid(partitionNum, 999);
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}

TEST_F  (OckShuffleTest, Split_Decimal64_128) {
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL64, OMNI_DECIMAL128};
    int partitionNum = 4;
    gVecTypeIds = &inputVecTypeIds[0];
    gColNum = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    int splitterId = OckTest_splitter_nativeMake("hash", partitionNum, inputVecTypeIds,
                                                 gColNum, false, 40960, 41943040, 134217728);
    gTempSplitId = splitterId; // very important
    for (uint64_t j = 0; j < 2; j++) {
        VectorBatch *vb = OckCreateVectorBatch_2decimalCol_withPid(partitionNum, 4);
        OckTest_splitter_split(splitterId, vb);
    }
    OckTest_splitter_stop(splitterId);
    OckTest_splitter_close(splitterId);
}
