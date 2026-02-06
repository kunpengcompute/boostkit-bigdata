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

#ifndef OMNI_RLE_HH
#define OMNI_RLE_HH

#include "orc/RLE.hh"
#include <vector/vector_common.h>

namespace omniruntime::writer {

    using StringFlatVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using StringDictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<std::string_view>>;
    using I64FlatVector = omniruntime::vec::Vector<int64_t>;
    using I64DictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<int64_t>>;
    using I32FlatVector = omniruntime::vec::Vector<int32_t>;
    using I32DictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<int32_t>>;
    using I16FlatVector = omniruntime::vec::Vector<int16_t>;
    using I16DictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<int16_t>>;
    using DoubleFlatVector = omniruntime::vec::Vector<double>;
    using DoubleDictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<double>>;
    using FloatFlatVector = omniruntime::vec::Vector<float>;
    using FloatDictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<float>>;
    using Decimal128FlatVector = omniruntime::vec::Vector<omniruntime::type::Decimal128>;
    using Decimal128DictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<omniruntime::type::Decimal128>>;

    class OmniRleEncoder : public orc::RleEncoder
    {
        public:
        OmniRleEncoder(std::unique_ptr<orc::RleEncoder> rleEncoder, bool hasSigned);

        ~OmniRleEncoder() = default;

        void add(const int64_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer);

        void add(const int32_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer);

        void add(const int16_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer);

        void add(StringFlatVector *vector, uint64_t offset, uint64_t numValues);

        void add(StringDictVector *vector, uint64_t offset, uint64_t numValues);

        void addDate(I32DictVector *vector, int32_t *convertData, uint64_t offset, uint64_t numValues);

        void addDate(int32_t *data, uint64_t offset, uint64_t numValues, omniruntime::vec::NullsBuffer *nullsBuffer);

        template<typename T>
        void add(T *vector, uint64_t offset, uint64_t numValues) {
            bool hasNull = vector->HasNull();
            for (uint64_t i = 0; i < numValues; ++i) {
                uint64_t curPos = offset + i;
                if(!hasNull || !vector->IsNull(curPos)) {
                    delegate_->write(vector->GetValue(curPos));
                }
            }
        }

        uint64_t getBufferSize() const {
            return delegate_->getBufferSize();
        }

        virtual void add(const int64_t* data, uint64_t numValues,
                                      const char* notNull) override;

        virtual uint64_t flush() override;

        virtual void recordPosition(orc::PositionRecorder* recorder) const override;

        virtual void write(int64_t val) override;

        protected:
        virtual void writeByte(char c) override;

        virtual void writeVulong(int64_t val) override;

        virtual void writeVslong(int64_t val) override;

        private:
        std::unique_ptr<orc::RleEncoder> delegate_;
    };

      /**
       * Create an RLE encoder.
       * @param output the output stream to write to
       * @param isSigned true if the number sequence is signed
       * @param version version of RLE decoding to do
       * @param pool memory pool to use for allocation
       */
      std::unique_ptr<OmniRleEncoder> createOmniRleEncoder
                             (std::unique_ptr<orc::BufferedOutputStream> output,
                              bool isSigned,
                              orc::RleVersion version,
                              orc::MemoryPool& pool,
                              bool alignedBitpacking);
}
#endif