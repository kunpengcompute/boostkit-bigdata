/**
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

#ifndef OMNI_RLEV2_HH
#define OMNI_RLEV2_HH

#include "orc/RLEv2.hh"
#include "orc/RLEV2Util.hh"
#include <vector/vector_common.h>

namespace omniruntime::reader {

    std::unique_ptr<omniruntime::vec::BaseVector> makeFixLenthVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId, bool is_row, std::shared_ptr<void>& rVec, std::vector<int32_t>& rowVecColumns);

    std::unique_ptr<omniruntime::vec::BaseVector> makeDoubleVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId, bool is_row, std::shared_ptr<void>& rVec, std::vector<int32_t>& rowVecColumns);

    std::unique_ptr<omniruntime::vec::BaseVector> makeNewVector(uint64_t numValues, const orc::Type* baseTp,
          omniruntime::type::DataTypeId dataTypeId, bool is_row, std::shared_ptr<void>& rVec, std::vector<int32_t>& rowVecColumns);

    class OmniRleDecoderV2 : public orc::RleDecoderV2 {
    public:
        OmniRleDecoderV2(std::unique_ptr<orc::SeekableInputStream> input,
                         bool isSigned, orc::MemoryPool& pool) : orc::RleDecoderV2(std::move(input), isSigned, pool){
        }
        /**
        * direct read VectorBatch in next
        * @param omnivec the BaseVector to push
        * @param numValues the numValues to push
        * @param notNull the nullarrays to push
        * @param baseTp the orcType to push
        * @param omniTypeId the int* of omniType to push
        */
        void next(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
                  const orc::Type* baseTp, int omniTypeId, bool is_row, std::shared_ptr<void>& rVec, std::vector<int32_t>& rowVecColumns);

        void next(int64_t* data, uint64_t numValues, const char* notNull) {
            orc::RleDecoderV2::next(data, numValues, notNull);
        }

        uint64_t nextDirect(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset, uint64_t numValues,
                            const char* const notNull, omniruntime::type::DataTypeId dataTypeId, bool is_row);

        uint64_t nextShortRepeatsByType(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset, uint64_t numValues,
                                        const char* const notNull, omniruntime::type::DataTypeId dataTypeId, bool is_row);


        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextShortRepeatsRowVec(omniruntime::vec::BaseVector*& OmniVec,
                                            uint64_t offset, uint64_t numValues, const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextShortRepeatsNorVec(omniruntime::vec::BaseVector*& OmniVec,
                                            uint64_t offset, uint64_t numValues, const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextShortRepeats(omniruntime::vec::BaseVector*& omnivec, uint64_t offset, uint64_t numValues,
                                  const char* notNull, bool is_row);


        uint64_t nextPatchedByType(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset, uint64_t numValues,
                                        const char* const notNull, omniruntime::type::DataTypeId dataTypeId, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextPatchedNorVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                            uint64_t numValues, const char* const notNull,
                                            omniruntime::type::DataTypeId dataTypeId);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextPatchedRowVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                            uint64_t numValues, const char* const notNull,
                                            omniruntime::type::DataTypeId dataTypeId);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextPatched(omniruntime::vec::BaseVector*& omnivec, uint64_t offset, uint64_t numValues,
                             const char* notNull, omniruntime::type::DataTypeId dataTypeId, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextDelta(omniruntime::vec::BaseVector*& omnivec, uint64_t offset, uint64_t numValues,
                           const char* notNull, omniruntime::type::DataTypeId dataTypeId, bool is_row);

        uint64_t nextDeltaByType(omniruntime::vec::BaseVector*& OmniVec,
                                                   uint64_t offset, uint64_t numValues, const char* const notNull,
                                                   omniruntime::type::DataTypeId dataTypeId, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextDeltaRowVec(omniruntime::vec::BaseVector*& OmniVec,
                                                  uint64_t offset, uint64_t numValues, const char* const notNull,
                                                  omniruntime::type::DataTypeId dataTypeId);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t nextDeltaNorVec(omniruntime::vec::BaseVector*& OmniVec,
                                                  uint64_t offset, uint64_t numValues, const char* const notNull,
                                                  omniruntime::type::DataTypeId dataTypeId);

        uint64_t copyDataFromBufferByType(omniruntime::vec::BaseVector*& tempOmniVec, uint64_t offset,
                                                    uint64_t numValues, const char* notNull,
                                                    omniruntime::type::DataTypeId dataTypeId, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBufferTo64Bit(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                            uint64_t numValues, const char* notNull, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBufferTo64BitRowVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                    uint64_t numValues, const char* notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBufferTo64BitNorVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                    uint64_t numValues, const char* notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBuffer(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                uint64_t numValues, const char* notNull, bool is_row);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBufferRowVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                uint64_t numValues, const char* notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        uint64_t copyDataFromBufferNorVec(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                uint64_t numValues, const char* notNull);

        void readLongsByType(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                                uint64_t len, uint64_t omniOffset, uint64_t numValues, uint64_t fbs,
                                                omniruntime::type::DataTypeId dataTypeId, const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void readLongs(omniruntime::vec::BaseVector*& OmniVec, int64_t *data, uint64_t offset,
                                                uint64_t len, uint64_t omniOffset, uint64_t numValues, uint64_t fbs,
                                                const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack4(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                                uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                                const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack8(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                             uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                             const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack16(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                           uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                           const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack24(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                         uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                         const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack32(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                       uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                       const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack40(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                               uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                               const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack48(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                       uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                       const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack56(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                       uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                       const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void unrolledUnpack64(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                       uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                       const char* const notNull);

        template <omniruntime::type::DataTypeId TYPE_ID>
        void plainUnpackLongs(omniruntime::vec::BaseVector*& OmniVec, int64_t* data, uint64_t offset,
                                     uint64_t len, uint64_t omniOffset, uint64_t omniNumValues,
                                     const char* const notNull, uint64_t fbs);
    };
}

#endif