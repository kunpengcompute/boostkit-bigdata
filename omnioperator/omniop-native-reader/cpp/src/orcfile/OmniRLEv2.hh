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
        omniruntime::type::DataTypeId dataTypeId);

    std::unique_ptr<omniruntime::vec::BaseVector> makeDoubleVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId);

    std::unique_ptr<omniruntime::vec::BaseVector> makeNewVector(uint64_t numValues, const orc::Type* baseTp,
          omniruntime::type::DataTypeId dataTypeId);

    class OmniRleDecoderV2 : public orc::RleDecoderV2 {
    public:
        OmniRleDecoderV2(std::unique_ptr<orc::SeekableInputStream> input,
                         bool isSigned, orc::MemoryPool& pool) : orc::RleDecoderV2(std::move(input), isSigned, pool){
        }
        /**
        * direct read VectorBatch in next
        * @param omnivec the BaseVector to push
        * @param numValues the numValues to push
        * @param nulls the nulls bits to push
        * @param baseTp the orcType to push
        * @param omniTypeId the int* of omniType to push
        */
        void next(omniruntime::vec::BaseVector *omnivec, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls, int omniTypeId);

        void next(int64_t *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        void next(int32_t *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        void next(int16_t *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        void next(bool *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        void next(T *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        uint64_t nextShortRepeats(T *data, uint64_t offset, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        uint64_t nextDirect(T *data, uint64_t offset, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        uint64_t nextPatched(T *data, uint64_t offset, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        uint64_t nextDelta(T *data, uint64_t offset, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        template<typename T>
        uint64_t copyDataFromBuffer(T *data, uint64_t offset, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

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