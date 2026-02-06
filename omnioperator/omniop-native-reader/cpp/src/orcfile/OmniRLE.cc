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

#include "OmniRLE.hh"
#include "common/RebaseDate.h"
namespace omniruntime::writer {


      std::unique_ptr<OmniRleEncoder> createOmniRleEncoder
                             (std::unique_ptr<orc::BufferedOutputStream> output,
                              bool isSigned,
                              orc::RleVersion version,
                              orc::MemoryPool& pool,
                              bool alignedBitpacking) {
        return std::make_unique<OmniRleEncoder>(std::move(orc::createRleEncoder(std::move(output), isSigned, version, pool, alignedBitpacking)), isSigned);
      }


        OmniRleEncoder::OmniRleEncoder(std::unique_ptr<orc::RleEncoder> rleEncoder, bool hasSigned)
        : orc::RleEncoder(nullptr, hasSigned)
        ,delegate_(std::move(rleEncoder)) {}

        void OmniRleEncoder::add(const int64_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer) {
            bool hasNull = (nullsBuffer != nullptr) && nullsBuffer->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                if (!hasNull || !nullsBuffer->IsNull(offset + i)) {
                    delegate_->write(data[i]);
                }
            }
        }

        void OmniRleEncoder::add(const int32_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer) {
            bool hasNull = (nullsBuffer != nullptr) && nullsBuffer->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                if (!hasNull || !nullsBuffer->IsNull(offset + i)) {
                    delegate_->write(data[i]);
                }
            }
        }

        void OmniRleEncoder::add(const int16_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer) {
            bool hasNull = (nullsBuffer != nullptr) && nullsBuffer->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                if (!hasNull || !nullsBuffer->IsNull(offset + i)) {
                    delegate_->write(data[i]);
                }
            }
        }

        void OmniRleEncoder::add(StringFlatVector *vector, uint64_t offset, uint64_t numValues) {
            bool hasNull = vector->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                uint64_t curPos = offset + i;
                if(!hasNull || !vector->IsNull(curPos)) {
                    std::string_view sv = vector->GetValue(curPos);
                    delegate_->write(sv.length());
                }
            }
        }

        void OmniRleEncoder::add(StringDictVector *vector, uint64_t offset, uint64_t numValues) {
            bool hasNull = vector->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                uint64_t curPos = offset + i;
                if(!hasNull || !vector->IsNull(curPos)) {
                    std::string_view sv = vector->GetValue(curPos);
                    delegate_->write(sv.length());
                }
            }
        }

        void OmniRleEncoder::addDate(I32DictVector *vector, int32_t *convertData, uint64_t offset, uint64_t numValues) {
            bool notNull = !vector->HasNull();
            uint64_t curPos;
            for (uint64_t i = 0; i < numValues; ++i) {
                if(notNull || !vector->IsNull(curPos = offset + i)) {
                    convertData[i] = common::rebaseGregorianToJulianDays(vector->GetValue(curPos));
                    delegate_->write(convertData[i]);
                }
            }
        }

        void OmniRleEncoder::addDate(int32_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer) {
            bool notNull = !nullsBuffer->HasNull();
           for (uint64_t i = 0; i < numValues; ++i) {
                if(notNull || !nullsBuffer->IsNull(offset + i)) {
                    data[i] = common::rebaseGregorianToJulianDays(data[i]);
                    delegate_->write(data[i]);
                }
            }
        }

        void OmniRleEncoder::add(const int64_t* data, uint64_t numValues,
                                      const char* notNull) {
            delegate_->add(data, numValues, notNull);
        }

        uint64_t OmniRleEncoder::flush() {
            return delegate_->flush();
        }

        void OmniRleEncoder::recordPosition(orc::PositionRecorder* recorder) const {
            delegate_->recordPosition(recorder);
        }

        void OmniRleEncoder::write(int64_t val) {
            delegate_->write(val);
        }

        void OmniRleEncoder::writeByte(char c) {
            //noop
        }

        void OmniRleEncoder::writeVulong(int64_t val) {
            //noop
        }

        void OmniRleEncoder::writeVslong(int64_t val) {
            //noop
        }
}