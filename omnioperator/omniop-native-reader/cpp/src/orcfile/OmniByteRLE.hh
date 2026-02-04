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
#ifndef OMNI_BYTE_RLE_HH
#define OMNI_BYTE_RLE_HH

#include "orc/ByteRLE.hh"
#include <vector/vector_common.h>

namespace omniruntime::reader {
    class OmniByteRleDecoder: public orc::ByteRleDecoder {
    public:
        OmniByteRleDecoder(std::unique_ptr<orc::SeekableInputStream> input);

        virtual ~OmniByteRleDecoder();

        /**
        * Seek to a particular spot.
        */
        virtual void seek(orc::PositionProvider&);

        /**
        * Seek over a given number of values.
        */
        virtual void skip(uint64_t numValues);

        /**
        * Read a number of values into the batch.
        */
        virtual void next(char* data, uint64_t numValues, char* notNull);

        void nextBatch(char *data, uint64_t numValues, uint64_t *nulls);

    protected:
        inline void nextBuffer();
        inline signed char readByte();
        inline void readHeader();

        std::unique_ptr<orc::SeekableInputStream> inputStream;
        size_t remainingValues;
        char value;
        const char *bufferStart;
        const char *bufferEnd;
        bool repeating;
    };

    class OmniBooleanRleDecoder: public OmniByteRleDecoder {
    public:
        OmniBooleanRleDecoder(std::unique_ptr<orc::SeekableInputStream> input);

        virtual ~OmniBooleanRleDecoder();

        /**
        * Seek to a particular spot.
        */
        virtual void seek(orc::PositionProvider&);

        /**
        * Seek over a given number of values.
        */
        virtual void skip(uint64_t numValues);

        /**
         * Read nulls flag to data.
         */
        void nextNulls(char *data, uint64_t numValues, uint64_t *nulls);

        /**
        * Read a number of values into the batch by nulls.
        */
        void next(omniruntime::vec::BaseVector *omnivec, uint64_t numValues, 
            uint64_t *nulls, int omniTypeId);

        template <typename T>
        void next(T *data, uint64_t numValues, uint64_t *nulls);

        void next(char *data, uint64_t numValues, uint64_t *nulls);

    protected:
        size_t remainingBits;
        char lastByte;
        char reversedAndFlipLastByte;
    };
}

namespace omniruntime::writer {

    using BoolFlatVector = omniruntime::vec::Vector<bool>;
    using BoolDictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<bool>>;
    using ByteFlatVector = omniruntime::vec::Vector<int8_t>;
    using ByteDictVector = omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<int8_t>>;

    /* omni encoder start */
    class OmniByteRleEncoder : public orc::ByteRleEncoder
    {
        public:
            OmniByteRleEncoder(std::unique_ptr<orc::BufferedOutputStream> output);
            virtual ~OmniByteRleEncoder() override;
         /**
         * Encode the next batch of values.
         * @param data to be encoded
         * @param numValues the number of values to be encoded
         * @param notNull If the pointer is null, all values are read. If the
         *    pointer is not null, positions that are false are skipped.
         */
         virtual void add(const char* data, uint64_t numValues,
                              const char* notNull) override;

         void add(const int8_t* data,
                  uint64_t offset,
                  uint64_t numValues,
                  omniruntime::vec::NullsBuffer* nullsBuffer);

         void add(ByteDictVector* data,
                  uint64_t offset,
                  uint64_t numValue);

        /**
         * Get size of buffer used so far.
         */
        virtual uint64_t getBufferSize() const override;

        /**
         * Flush underlying BufferedOutputStream.
         */
        virtual uint64_t flush() override;

        virtual void recordPosition(orc::PositionRecorder* recorder) const override;

        protected:
          std::unique_ptr<orc::BufferedOutputStream> outputStream;
          char* literals;
          int numLiterals;
          bool repeat;
          int tailRunLength;
          int bufferPosition;
          int bufferLength;
          char* buffer;

          void writeByte(char c);
          void writeValues();
          void write(char c);
    };

    std::unique_ptr<orc::ByteRleEncoder> createOmniByteRleEncoder(std::unique_ptr<orc::BufferedOutputStream> output);


    class OmniBooleanRleEncoder : public OmniByteRleEncoder
    {
        public:
        OmniBooleanRleEncoder(std::unique_ptr<orc::BufferedOutputStream> output);
        virtual ~OmniBooleanRleEncoder() override = default;

        void add(omniruntime::vec::NullsBuffer *nullsBuffer,
            omniruntime::vec::NullsBuffer *pNullsBuffer,
            uint64_t offset,
            uint64_t numValues);

        void add(const bool* data,
            uint64_t offset,
            uint64_t numValues,
            omniruntime::vec::NullsBuffer *nullsBuffer);

        /**
         * Encode the next batch of values
         * @param data to be encoded
         * @param numValues the number of values to be encoded
         * @param notNull If the pointer is null, all values are read. If the
         *    pointer is not null, positions that are false are skipped.
         */
        virtual void add(const char* data, uint64_t numValues,
                          const char* notNull) override;

        void add(BoolDictVector *vec, uint64_t offset, uint64_t numValues);

        /**
         * Flushing underlying BufferedOutputStream
         */
        virtual uint64_t flush() override;

        virtual void recordPosition(orc::PositionRecorder* recorder) const override;

        private:
            int bitsRemained;
            char current;
    };


      /**
       * Create a boolean RLE encoder.
       * @param output the output stream to write to
       */
      std::unique_ptr<orc::ByteRleEncoder> createOmniBooleanRleEncoder
                                     (std::unique_ptr<orc::BufferedOutputStream> output);

    /* omni encoder start */
}
#endif
