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

#ifndef OMNI_COL_READER_HH
#define OMNI_COL_READER_HH

#include "orc/ColumnReader.hh"
#include "orc/RLE.hh"
#include "orc/Type.hh"
#include "orc/io/InputStream.hh"
#include "OmniRLEv2.hh"
#include "orc/Int128.hh"
#include "OmniByteRLE.hh"
#include "common/JulianGregorianRebase.h"

namespace omniruntime::reader {

    class OmniColumnReader: public orc::ColumnReader {
    public:
        OmniColumnReader(const orc::Type& type, orc::StripeStreams& stripe);

        virtual ~OmniColumnReader() {}

        /**
         * Skip number of specified rows.
         */
        virtual uint64_t skip(uint64_t numValues);

        /**
         * Read OmniVector in OmniTypeId, which contains specified rows.
         */
        virtual void next(
            omniruntime::vec::BaseVector *omniVector,
            uint64_t numValues,
            uint64_t *incomingNulls,
            int omniTypeId) {
            throw std::runtime_error("next() in base class should not be called");
        }

        /**
         * Seek to beginning of a row group in the current stripe
         * @param positions a list of PositionProviders storing the positions
         */
        virtual void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider> &positions);

        std::unique_ptr<OmniBooleanRleDecoder> notNullDecoder;
    };

    class OmniStructColumnReader: public OmniColumnReader {
    private:
        std::vector<std::unique_ptr<ColumnReader>> children;

    public:
        OmniStructColumnReader(const orc::Type& type, orc::StripeStreams& stipe,
            common::JulianGregorianRebase *julianPtr);

        uint64_t skip(uint64_t numValues) override;

        /**
         * direct read VectorBatch in next
         * @param omniVecBatch the VectorBatch to push
         * @param numValues the numValues of VectorBatch
         * @param notNull the notNull array indicates value not null
         * @param baseTp the orc type
         * @param omniTypeId the omniTypeId to push
         */
        void next(void *&omniVecBatch, uint64_t numValues, char *notNull, const orc::Type& baseTp,
                  int* omniTypeId) override;

        void seekToRowGroup(
                std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;

    private:
        /**
         * direct read VectorBatch in next for omni
         * @param omniVecBatch the VectorBatch to push
         * @param numValues the numValues of VectorBatch
         * @param notNull the notNull array indicates value not null
         * @param baseTp the orc type
         * @param omniTypeId the omniTypeId to push
         */
        template<bool encoded>
        void nextInternal(std::vector<omniruntime::vec::BaseVector*> &vecs, uint64_t numValues, 
            uint64_t *incomingNulls, const orc::Type& baseTp, int* omniTypeId);

        // Get default omni type from orc type.
        omniruntime::type::DataTypeId getDefaultOmniType(const orc::Type *type);
    };

    class OmniBooleanColumnReader: public OmniColumnReader {
    protected:
        std::unique_ptr<OmniByteRleDecoder> rle;

    public:
        OmniBooleanColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
        ~OmniBooleanColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniByteColumnReader: public OmniColumnReader {
    protected:
        std::unique_ptr<OmniByteRleDecoder> rle;

    public:
        OmniByteColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
        ~OmniByteColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniIntegerColumnReader: public OmniColumnReader {
    protected:
        std::unique_ptr<OmniRleDecoderV2> rle;

    public:
        OmniIntegerColumnReader(const orc::Type& type, orc::StripeStreams& stripe);
        ~OmniIntegerColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniTimestampColumnReader: public OmniColumnReader {
    public:
        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        template <typename T>
        void nextByType(T *data, uint64_t numValues, uint64_t *nulls);

    private:
        std::unique_ptr<OmniRleDecoderV2> secondsRle;
        std::unique_ptr<OmniRleDecoderV2> nanoRle;
        const orc::Timezone& writerTimezone;
        const orc::Timezone& readerTimezone;
        const int64_t epochOffset;
        const bool sameTimezone;
        common::JulianGregorianRebase *julianPtr;

    public:
        OmniTimestampColumnReader(const orc::Type& type,
                                orc::StripeStreams& stripe,
                                bool isInstantType, common::JulianGregorianRebase *julianPtr);
        ~OmniTimestampColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniDoubleColumnReader: public OmniColumnReader {
    public:
        OmniDoubleColumnReader(const orc::Type& type, orc::StripeStreams& stripe);
        ~OmniDoubleColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        template <typename T>
        void nextByType(T *data, uint64_t numValues, uint64_t *nulls);

        void seekToRowGroup(
            std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;

    private:
        std::unique_ptr<orc::SeekableInputStream> inputStream;
        orc::TypeKind columnKind;
        const uint64_t bytesPerValue;
        const char *bufferPointer;
        const char *bufferEnd;

        unsigned char readByte() {
            if (bufferPointer == bufferEnd) {
                int length;
                if (!inputStream->Next
                    (reinterpret_cast<const void**>(&bufferPointer), &length)) {
                        throw orc::ParseError("bad read in DoubleColumnReader::next()");
                }
                bufferEnd = bufferPointer + length;
            }
            return static_cast<unsigned char>(*(bufferPointer++));
        }

        double readDouble() {
            int64_t bits = 0;
            for (uint64_t i=0; i < 8; i++) {
                bits |= static_cast<int64_t>(readByte()) << (i*8);
            }
            double *result = reinterpret_cast<double*>(&bits);
            return *result;
        }

        double readFloat() {
            int32_t bits = 0;
            for (uint64_t i=0; i < 4; i++) {
                bits |= readByte() << (i*8);
            }
            float *result = reinterpret_cast<float*>(&bits);
            return static_cast<double>(*result);
        }

    };

    class OmniStringDictionaryColumnReader: public OmniColumnReader {
    public:
        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;
        
        private:
            std::shared_ptr<orc::StringDictionary> dictionary;
            std::unique_ptr<OmniRleDecoderV2> rle;
            bool isChar = false;

        public:
            OmniStringDictionaryColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
            ~OmniStringDictionaryColumnReader() override;

            uint64_t skip(uint64_t numValues) override;

            void seekToRowGroup(std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniStringDirectColumnReader: public OmniColumnReader {
    public:
        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

    private:
        std::unique_ptr<OmniRleDecoderV2> lengthRle;
        std::unique_ptr<orc::SeekableInputStream> blobStream;
        const char *lastBuffer;
        size_t lastBufferLength;
        bool isChar = false;

        /**
         * Compute the total length of the values.
         * @param lengths the array of lengths
         * @param nulls the bits of nulls flags
         * @param numValues the lengths of the arrays
         * @return the total number of bytes for the non-null values
         */
        size_t computeSize(const int64_t *lengths, uint64_t *nulls,
            uint64_t numValues);

    public:
        OmniStringDirectColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
        ~OmniStringDirectColumnReader() override;

        uint64_t skip(uint64_t numValues) override;

        void seekToRowGroup(
                std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniDecimal64ColumnReader: public OmniColumnReader {
    public:
        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

        public:
            static const uint32_t MAX_PRECISION_64 = 18;
            static const uint32_t MAX_PRECISION_128 = 38;
            static const int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1];
        
        protected:
            std::unique_ptr<orc::SeekableInputStream> valueStream;
            int32_t precision;
            int32_t scale;
            const char* buffer;
            const char* bufferEnd;

            std::unique_ptr<OmniRleDecoderV2> scaleDecoder;

            /**
             * Read the valueStream for more bytes.
             */
            void readBuffer() {
                while (buffer == bufferEnd) {
                    int length;
                    if (!valueStream->Next(reinterpret_cast<const void**>(&buffer),
                                           &length)) {
                        throw orc::ParseError("Read past end of stream in Decimal64ColumnReader "+
                                         valueStream->getName());
                    }
                    bufferEnd = buffer + length;
                }
            }

            void readInt64(int64_t& value, int32_t currentScale) {
                value = 0;
                size_t offset = 0;
                while (true) {
                    readBuffer();
                    unsigned char ch = static_cast<unsigned char>(*(buffer++));
                    value |= static_cast<uint64_t>(ch & 0x7f) << offset;
                    offset += 7;
                    if (!(ch & 0x80)) {
                        break;
                    }
                }
                value = orc::unZigZag(static_cast<uint64_t>(value));
                if (scale > currentScale &&
                    static_cast<uint64_t>(scale - currentScale) <= MAX_PRECISION_64) {
                    value *= POWERS_OF_TEN[scale - currentScale];
                } else if (scale < currentScale &&
                           static_cast<uint64_t>(currentScale - scale) <= MAX_PRECISION_64) {
                    value /= POWERS_OF_TEN[currentScale - scale];
                } else if (scale != currentScale) {
                    throw orc::ParseError("Decimal scale out of range");
                }
            }

            void ReadValuesBatch(int64_t *data, int64_t numValues) {
                for (int i = 0; i < numValues; ++i) {
                    int64_t value = 0;
                    size_t offset = 0;
                    while (true) {
                        readBuffer();
                        unsigned char ch = static_cast<unsigned char>(*(buffer++));
                        value |= static_cast<uint64_t>(ch & 0x7f) << offset;
                        offset += 7;
                        if (!(ch & 0x80)) {
                            break;
                        }
                    }
                    data[i] = value;
                }
            }

            void UnScaleBatch(int64_t *data, int64_t *scales, int64_t numValues) {
                for (int i = 0; i < numValues; i++) {
                    int32_t currentScale = static_cast<int32_t>(scales[i]);
                    if (scale > currentScale &&
                        static_cast<uint64_t>(scale - currentScale) <= MAX_PRECISION_64) {
                        data[i] *= POWERS_OF_TEN[scale - currentScale];
                    } else if (scale < currentScale &&
                               static_cast<uint64_t>(currentScale - scale) <= MAX_PRECISION_64) {
                        data[i] /= POWERS_OF_TEN[currentScale - scale];
                    } else if (scale != currentScale) {
                        throw orc::ParseError("Decimal scale out of range");
                    }
                }
            }

        public:
            OmniDecimal64ColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
            ~OmniDecimal64ColumnReader() override;
            
            uint64_t skip(uint64_t numValues) override;

            void seekToRowGroup(
                    std::unordered_map<uint64_t, orc::PositionProvider>& positions) override;
    };

    class OmniDecimal128ColumnReader : public OmniDecimal64ColumnReader {
    public:
        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;

    public:
        OmniDecimal128ColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
        ~OmniDecimal128ColumnReader() override;
    
    private:
        void readInt128(orc::Int128& value, int32_t currentScale);
    };

    class OmniDecimalHive11ColumnReader : public OmniDecimal64ColumnReader {
    private:
    	bool throwOnOverflow;
    	std::ostream* errorStream;

    	bool readInt128(orc::Int128& value, int32_t currentScale);

    public:
    	OmniDecimalHive11ColumnReader(const orc::Type& type, orc::StripeStreams& stipe);
    	~OmniDecimalHive11ColumnReader() override;

        void next(omniruntime::vec::BaseVector *vec, uint64_t numValues, 
            uint64_t *incomingNulls, int omniTypeId) override;
    };

    std::unique_ptr<orc::ColumnReader> omniBuildReader(const orc::Type& type,
                                                    orc::StripeStreams& stripe,
                                                    common::JulianGregorianRebase *julianPtr);

    void scaleInt128(orc::Int128& value, uint32_t scale, uint32_t currentScale);

    void omniUnZigZagInt128(orc::Int128& value);
}

#endif