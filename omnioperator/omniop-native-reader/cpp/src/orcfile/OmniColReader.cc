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

#include "OmniColReader.hh"
#include "orc/ByteRLE.hh"
#include "orc/wrap/orc-proto-wrapper.hh"
#include "orc/orc_proto.pb.h"
#include "orc/Writer.hh"
#include "orc/MemoryPool.hh"
#include "util/omni_exception.h"
#include "OrcDecodeUtils.hh"

using omniruntime::vec::VectorBatch;
using omniruntime::vec::BaseVector;
using omniruntime::exception::OmniException;
using omniruntime::vec::NullsBuffer;
using orc::ColumnReader;
using orc::ByteRleDecoder;
using orc::Type;
using orc::StripeStreams;
using orc::RleVersion;
using orc::SeekableInputStream;
using orc::RleDecoder;
using orc::MemoryPool;
using orc::PositionProvider;

namespace omniruntime::reader {
    /**
    * Global funcs To inline
    */
    RleVersion omniConvertRleVersion(orc::proto::ColumnEncoding_Kind kind) {
        switch (static_cast<int64_t>(kind)) {
            case orc::proto::ColumnEncoding_Kind_DIRECT:
            case orc::proto::ColumnEncoding_Kind_DICTIONARY:
                return orc::RleVersion_1;
            case orc::proto::ColumnEncoding_Kind_DIRECT_V2:
            case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2:
                return orc::RleVersion_2;
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "Unknown encoding in omniConvertRleVersion");
        }
    }

    std::unique_ptr<OmniRleDecoderV2> createOmniRleDecoder(std::unique_ptr<SeekableInputStream> input, bool isSigned,
                                                        RleVersion version, MemoryPool& pool) {
        switch (static_cast<int64_t>(version)) {
            case orc::RleVersion_1:
                // should not use
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "RleVersion_1 Not supported yet");
            case orc::RleVersion_2:
                return std::unique_ptr<OmniRleDecoderV2>(new OmniRleDecoderV2(std::move(input), isSigned, pool));
            default:
                throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "Not implemented yet");
        }
    }

    std::unique_ptr<OmniByteRleDecoder> createOmniBooleanRleDecoder(std::unique_ptr<SeekableInputStream> input) {
        OmniBooleanRleDecoder* decoder = new OmniBooleanRleDecoder(std::move(input));
        return std::unique_ptr<OmniByteRleDecoder>(reinterpret_cast<OmniByteRleDecoder*>(decoder));
    }

    std::unique_ptr<OmniByteRleDecoder> createOmniByteRleDecoder(std::unique_ptr<SeekableInputStream> input) {
        return std::unique_ptr<OmniByteRleDecoder>(new OmniByteRleDecoder(std::move(input)));
    }
    
    OmniColumnReader::OmniColumnReader(const Type &type, StripeStreams &stripe)
        : ColumnReader(type, stripe) {
        std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, orc::proto::Stream_Kind_PRESENT, true);
        if (stream.get()) {
            notNullDecoder = std::make_unique<OmniBooleanRleDecoder>(std::move(stream));
        }
    }
    
    uint64_t OmniColumnReader::skip(uint64_t numValues) {
        if (notNullDecoder) {
            // pass through the values that we want to skip and count how many are non-null
            const uint64_t MAX_BUFFER_SIZE = 32768;
            uint64_t bufferSize = std::min(MAX_BUFFER_SIZE, numValues);
            // buffer, 0: null; 1: non-null
            char buffer[MAX_BUFFER_SIZE];
            uint64_t remaining = numValues;
            while (remaining > 0) {
                uint64_t chunkSize = std::min(remaining, bufferSize);
                notNullDecoder->next(buffer, chunkSize, nullptr);
                remaining -= chunkSize;
                // update non-null count
                for (uint64_t i = 0; i < chunkSize; i++) {
                    if (!buffer[i]) {
                        // minus null
                        numValues -= 1;
                    }
                }
            }
        }
        return numValues;
    }
    
    void OmniColumnReader::seekToRowGroup(std::unordered_map<uint64_t, PositionProvider> &positions) {
        if (notNullDecoder) {
            notNullDecoder->seek(positions.at(columnId));
        }
    }
    

    /**
    * Create a reader for the given stripe.
    */
    std::unique_ptr<ColumnReader> omniBuildReader(const Type& type,
                                                 StripeStreams& stripe, common::JulianGregorianRebase *julianPtr) {
        switch (static_cast<int64_t>(type.getKind())) {
            case orc::DATE:
            case orc::INT:
            case orc::LONG:
            case orc::SHORT:
                return std::make_unique<OmniIntegerColumnReader>(type, stripe);
            case orc::BINARY:
            case orc::CHAR:
            case orc::STRING:
            case orc::VARCHAR:
                switch (static_cast<int64_t>(stripe.getEncoding(type.getColumnId()).kind())) {
                    case orc::proto::ColumnEncoding_Kind_DICTIONARY:
                    case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2:
                        return std::make_unique<OmniStringDictionaryColumnReader>(type, stripe);
                    case orc::proto::ColumnEncoding_Kind_DIRECT:
                    case orc::proto::ColumnEncoding_Kind_DIRECT_V2:
                        return std::make_unique<OmniStringDirectColumnReader>(type, stripe);
                    default:
                        throw omniruntime::exception::OmniException(
                            "EXPRESSION_NOT_SUPPORT", "omniBuildReader unhandled string encoding");
                }

            case orc::BOOLEAN:
                return std::make_unique<OmniBooleanColumnReader>(type, stripe);

            case orc::BYTE:
                return std::make_unique<OmniByteColumnReader>(type, stripe);

            case orc::STRUCT:
                return std::make_unique<OmniStructColumnReader>(type, stripe, julianPtr);

            case orc::TIMESTAMP:
                return std::make_unique<OmniTimestampColumnReader>(type, stripe, false, julianPtr);

            case orc::TIMESTAMP_INSTANT:
                return std::make_unique<OmniTimestampColumnReader>(type, stripe, true, julianPtr);

            case orc::DECIMAL:
                // Is this a Hive 0.11 or 0.12 file?
                if (type.getPrecision() == 0) {
                    return std::make_unique<OmniDecimalHive11ColumnReader>(type, stripe);
                } else if (type.getPrecision() <= OmniDecimal64ColumnReader::MAX_PRECISION_64) {
                    return std::make_unique<OmniDecimal64ColumnReader>(type, stripe);
                } else {
                    return std::make_unique<OmniDecimal128ColumnReader>(type, stripe);
                }

            case orc::FLOAT:
            case orc::DOUBLE:
                return std::make_unique<OmniDoubleColumnReader>(type, stripe);

            default:
                throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "omniBuildReader unhandled type");
        }
    }

    inline void readNulls(OmniColumnReader *colReader, uint64_t numValues, uint64_t *incomingNulls,
        uint8_t *nulls) {
        if (colReader->notNullDecoder) {
            colReader->notNullDecoder->nextNulls(reinterpret_cast<char*>(nulls), numValues, incomingNulls);
            // check to see if there are nulls in this batch
        } else if (incomingNulls) {
            // if we don't have a notNull stream, copy the incomingNulls
            // To do finished
            memcpy_s(reinterpret_cast<uint64_t*>(nulls), BitUtil::Nbytes(numValues), incomingNulls,
                BitUtil::Nbytes(numValues));
            return;
        }
    }

    void scaleInt128(orc::Int128& value, uint32_t scale, uint32_t currentScale) {
        if (scale > currentScale) {
            while(scale > currentScale) {
                uint32_t scaleAdjust =
                        std::min(OmniDecimal64ColumnReader::MAX_PRECISION_64,
                                 scale - currentScale);
                value *= OmniDecimal64ColumnReader::POWERS_OF_TEN[scaleAdjust];
                currentScale += scaleAdjust;
            }
        } else if (scale < currentScale) {
            orc::Int128 remainder;
            while(currentScale > scale) {
                uint32_t scaleAdjust =
                        std::min(OmniDecimal64ColumnReader::MAX_PRECISION_64,
                                 currentScale - scale);
                value = value.divide(OmniDecimal64ColumnReader::POWERS_OF_TEN[scaleAdjust],
                                     remainder);
                currentScale -= scaleAdjust;
            }
        }
    }

    void omniUnZigZagInt128(orc::Int128& value) {
        bool needsNegate = value.getLowBits() & 1;
        value >>= 1;
        if (needsNegate) {
            value.negate();
            value -= 1;
        }
    }

    inline void FindLastNotEmpty(const char *chars, long &len)
    {
        while (len > 0 && chars[len - 1] == ' ') {
            len--;
        }
    }

    void omniReadFully(char* buffer, int64_t bufferSize, SeekableInputStream* stream) {
        int64_t posn = 0;
        while (posn < bufferSize) {
            const void* chunk;
            int length;
            if (!stream->Next(&chunk, &length)) {
                throw orc::ParseError("bad read in omniReadFully");
            }
            if (posn + length > bufferSize) {
                throw orc::ParseError("Corrupt dictionary blob in StringDictionaryColumn");
            }
            memcpy_s(buffer + posn, bufferSize - posn, chunk, static_cast<size_t>(length));
            posn += length;
        }
    }

    /**
    * OmniStructColumnReader funcs
    */
    OmniStructColumnReader::OmniStructColumnReader(const Type& type, StripeStreams& stripe,
        common::JulianGregorianRebase *julianPtr): OmniColumnReader(type, stripe) {
        // count the number of selected sub-columns
        const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
        switch (static_cast<int64_t>(stripe.getEncoding(columnId).kind())) {
            case orc::proto::ColumnEncoding_Kind_DIRECT:
                for(unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
                    const Type& child = *type.getSubtype(i);
                    if (selectedColumns[static_cast<uint64_t>(child.getColumnId())]) {
                        children.push_back(omniBuildReader(child, stripe, julianPtr));
                    }
                }
                break;
            case orc::proto::ColumnEncoding_Kind_DIRECT_V2:
            case orc::proto::ColumnEncoding_Kind_DICTIONARY:
            case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2:
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "Unknown encoding for OmniStructColumnReader");
        }
    }

    uint64_t OmniStructColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        for(auto& ptr : children) {
            ptr->skip(numValues);
        }
        return numValues;
    }

    void OmniStructColumnReader::next(void *&batch, uint64_t numValues, char *notNull,
        const orc::Type& baseTp, int* omniTypeId) {
        auto vecs = reinterpret_cast<std::vector<omniruntime::vec::BaseVector*>*>(batch);
        nextInternal<false>(*vecs, numValues, nullptr, baseTp, omniTypeId);
    }

    void OmniStructColumnReader::seekToRowGroup(std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);

        for(auto& ptr : children) {
            ptr->seekToRowGroup(positions);
        }
    }

    template<bool encoded>
    void OmniStructColumnReader::nextInternal(std::vector<omniruntime::vec::BaseVector*> &vecs, uint64_t numValues,
        uint64_t *incomingNulls, const orc::Type& baseTp, int* omniTypeId) {

        if (encoded) {
            std::string message("OmniStructColumnReader::nextInternal encoded is not finished!");
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", message);
        }
        bool hasNull = false;
        auto nulls = std::make_shared<NullsBuffer>(numValues);
        readNulls(this, numValues, incomingNulls, reinterpret_cast<uint8_t*>(nulls->GetNulls()));
        uint64_t i = 0;

        for(auto iter = children.begin(); iter != children.end(); ++iter, ++i) {
            const Type* orcType = baseTp.getSubtype(i);
            omniruntime::type::DataTypeId dataTypeId;
            if (omniTypeId == nullptr) {
                dataTypeId = getDefaultOmniType(orcType);
            } else {
                dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId[i]);
            }
            auto omnivector = omniruntime::reader::makeNewVector(numValues, orcType, dataTypeId);
            reinterpret_cast<OmniColumnReader*>(&(*iter->get()))->next(omnivector.get(), numValues,
                hasNull ? nulls->GetNulls() : nullptr, dataTypeId);
            vecs.push_back(omnivector.release());
        }
    }

    omniruntime::type::DataTypeId OmniStructColumnReader::getDefaultOmniType(const Type* type) {
        constexpr int32_t OMNI_MAX_DECIMAL64_DIGITS = 18;
        switch (type->getKind()) {
            case orc::TypeKind::BOOLEAN:
                return omniruntime::type::OMNI_BOOLEAN;
            case orc::TypeKind::SHORT:
                return omniruntime::type::OMNI_SHORT;
            case orc::TypeKind::DATE:
                //To do check if  the DATE is DATE64 type
                return omniruntime::type::OMNI_DATE32;
            case orc::TypeKind::INT:
                return omniruntime::type::OMNI_INT;
            case orc::TypeKind::LONG:
                return omniruntime::type::OMNI_LONG;
            case orc::TypeKind::TIMESTAMP:
            case orc::TypeKind::TIMESTAMP_INSTANT:
                return omniruntime::type::OMNI_TIMESTAMP;
            case orc::TypeKind::DOUBLE:
                return omniruntime::type::OMNI_DOUBLE;
            case orc::TypeKind::CHAR:
            case orc::TypeKind::STRING:
            case orc::TypeKind::VARCHAR:
                return omniruntime::type::OMNI_VARCHAR;
            case orc::TypeKind::DECIMAL:
                if (type->getPrecision() > OMNI_MAX_DECIMAL64_DIGITS) {
                    return omniruntime::type::OMNI_DECIMAL128;
                } else {
                    return omniruntime::type::OMNI_DECIMAL64;
                }
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "Not Supported Type: " + type->getKind());
        }
    }

    /**
    * all next funcs
    */
    void OmniIntegerColumnReader::next(BaseVector *vec, uint64_t numValues, uint64_t *incomingNulls,
        int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();
        rle->next(vec, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr, omniTypeId);
    }

    void OmniBooleanColumnReader::next(BaseVector *vec, uint64_t numValues, uint64_t *incomingNulls,
        int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();
        if (omniTypeId != omniruntime::type::OMNI_BOOLEAN) {
            throw OmniException("EXPRESSION_NOT_SUPPORT", "Not Supported Type: " + omniTypeId);
        }
        OmniBooleanRleDecoder *boolDecoder = reinterpret_cast<OmniBooleanRleDecoder*>(rle.get());
        boolDecoder->next(vec, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr, omniTypeId);
    }

    void OmniByteColumnReader::next(BaseVector *vec, uint64_t numValues, uint64_t *incomingNulls,
        int omniTypeId) {
        throw OmniException("EXPRESSION_NOT_SUPPORT", "Not Supported yet.");
    }

    void OmniTimestampColumnReader::next(BaseVector *vec, uint64_t numValues,
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();
        auto dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId);
        switch (dataTypeId) {
            case omniruntime::type::OMNI_DATE32: {
                auto intValues = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                    static_cast<omniruntime::vec::Vector<int32_t>*>(vec));
                return nextByType(intValues, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr);
            }
            case omniruntime::type::OMNI_DATE64: {
                auto longValues = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                    static_cast<omniruntime::vec::Vector<int64_t>*>(vec));
                return nextByType(longValues, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr);
            }
            case omniruntime::type::OMNI_TIMESTAMP: {
                auto longValues = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                    static_cast<omniruntime::vec::Vector<int64_t>*>(vec));
                return nextByType(longValues, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr);
            }
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "OmniTimestampColumnReader type not support: " + dataTypeId);
        }
    }

    template <typename T>
    void OmniTimestampColumnReader::nextByType(T *data, uint64_t numValues, uint64_t *nulls) {
        int64_t secsBuffer[numValues];
        secondsRle->next(secsBuffer, numValues, nulls);
        int64_t nanoBuffer[numValues];
        nanoRle->next(nanoBuffer, numValues, nulls);

        // Construct the values
        for(uint64_t i = 0; i < numValues; i++) {
            if (nulls == nullptr || !BitUtil::IsBitSet(nulls, i)) {
                uint64_t zeros = nanoBuffer[i] & 0x7;
                nanoBuffer[i] >>= 3;
                if (zeros != 0) {
                    for (uint64_t j = 0; j <= zeros; ++j) {
                        nanoBuffer[i] *= 10;
                    }
                }
                int64_t writerTime = secsBuffer[i] + epochOffset;
                if (!sameTimezone) {
                    // adjust timestamp value to same wall clock time if writer and reader
                    // time zones have different rules, which is required for Apache Orc.
                    const auto& wv = writerTimezone.getVariant(writerTime);
                    const auto& rv = readerTimezone.getVariant(writerTime);
                    if (!wv.hasSameTzRule(rv)) {
                        // If the timezone adjustment moves the millis across a DST boundary,
                        // we need to reevaluate the offsets.
                        int64_t adjustedTime = writerTime + wv.gmtOffset - rv.gmtOffset;
                        const auto& adjustedReader = readerTimezone.getVariant(adjustedTime);
                        writerTime = writerTime + wv.gmtOffset - adjustedReader.gmtOffset;
                    }
                }
                secsBuffer[i] = writerTime;
                if (secsBuffer[i] < 0 && nanoBuffer[i] > 999999) {
                    secsBuffer[i] -= 1;
                }

                if (julianPtr != nullptr) {
                    data[i] = static_cast<T>(
                            julianPtr->RebaseJulianToGregorianMicros(secsBuffer[i] * 1000000L + nanoBuffer[i] / 1000L));
                } else {
                    data[i] = static_cast<T>(secsBuffer[i] * 1000000L + nanoBuffer[i] / 1000L);
                }
            }
        }
    }

    void OmniDoubleColumnReader::next(BaseVector *vec, uint64_t numValues, uint64_t *incomingNulls,
        int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();
        auto dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId);
        switch (dataTypeId) {
            case omniruntime::type::OMNI_DOUBLE: {
                auto doubleValues = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                    static_cast<omniruntime::vec::Vector<double>*>(vec));
                return nextByType(doubleValues, numValues, hasNull ? reinterpret_cast<uint64_t*>(nulls) : nullptr);
            }
            default:
                throw omniruntime::exception::OmniException(
                    "EXPRESSION_NOT_SUPPORT", "OmniDoubleColumnReader type not support: " + dataTypeId);
        }
    }

    template <typename T>
    void OmniDoubleColumnReader::nextByType(T *data, uint64_t numValues, uint64_t *nulls) {
        if (columnKind == orc::FLOAT) {
            if(nulls) {
                for(size_t i = 0; i < numValues; ++i) {
                    if(!BitUtil::IsBitSet(nulls, i)) {
                        data[i] = static_cast<T>(readFloat());
                    }
                }
            } else {
                for(size_t i = 0; i < numValues; ++i) {
                    data[i] = static_cast<T>(readFloat());
                }
            }
        } else {
            if (nulls) {
                for(size_t i = 0; i < numValues; ++i) {
                    if (!BitUtil::IsBitSet(nulls, i)) {
                        data[i] = static_cast<T>(readDouble());
                    }
                }
            } else {
                for(size_t i = 0; i < numValues; ++i) {
                    data[i] = static_cast<T>(readDouble());
                }
            }
        }
    }

    void OmniStringDictionaryColumnReader::next(BaseVector *vec, uint64_t numValues, 
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();

        char *blob = dictionary->dictionaryBlob.data();
        int64_t *dictionaryOffsets = dictionary->dictionaryOffset.data();
        auto nullsTrans = reinterpret_cast<uint64_t*>(nulls);
        int64_t outputLengths[numValues];
        rle->next(outputLengths, numValues, nullsTrans);
        uint64_t dictionaryCount = dictionary->dictionaryOffset.size() - 1;

        auto varcharVector = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::LargeStringContainer<std::string_view>>*>(vec);
        if (hasNull) {
            for(uint64_t i=0; i < numValues; ++i) {
                if (!BitUtil::IsBitSet(nullsTrans, i)) {
                    int64_t entry = outputLengths[i];
                    if (entry < 0 || static_cast<uint64_t>(entry) >= dictionaryCount ) {
                        throw orc::ParseError("Entry index out of range in StringDictionaryColumn");
                    }

                    //求出长度，如果为char，则需要去除最后的空格
                    auto len = dictionaryOffsets[entry+1] -
                               dictionaryOffsets[entry];
                    char* ptr = blob + dictionaryOffsets[entry];
                    if (isChar) {
                        FindLastNotEmpty(ptr, len);
                    }
                    auto data = std::string_view(ptr, len);
                    varcharVector->SetValue(i, data);
                } else {
                    varcharVector->SetNull(i);
                }
            }
        } else {
            for(uint64_t i=0; i < numValues; ++i) {
                int64_t entry = outputLengths[i];
                if (entry < 0 || static_cast<uint64_t>(entry) >= dictionaryCount) {
                    throw orc::ParseError("Entry index out of range in StringDictionaryColumn");
                }

                //求出长度，如果为char，则需要去除最后的空格
                auto len = dictionaryOffsets[entry+1] -
                           dictionaryOffsets[entry];
                char* ptr = blob + dictionaryOffsets[entry];
                if (isChar) {
                    FindLastNotEmpty(ptr, len);
                }
                auto data = std::string_view(ptr, len);
                varcharVector->SetValue(i, data);
            }
        }
    }

    void OmniStringDirectColumnReader::next(BaseVector *vec, uint64_t numValues, 
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();

        int64_t lengthPtr[numValues];
        auto nullsTrans = reinterpret_cast<uint64_t*>(nulls);
        // read the length vector
        lengthRle->next(lengthPtr, numValues, nullsTrans);

        // figure out the total length of data we need from the blob stream
        const size_t totalLength = computeSize(lengthPtr, nullsTrans, numValues);

        // Load data from the blob stream into our buffer until we have enough
        // to get the rest directly out of the stream's buffer.
        size_t bytesBuffered = 0;
        char ptr[totalLength];
        while (bytesBuffered + lastBufferLength < totalLength) {
            memcpy_s(ptr + bytesBuffered, totalLength - bytesBuffered, lastBuffer, lastBufferLength);
            bytesBuffered += lastBufferLength;
            const void* readBuffer;
            int readLength;
            if (!blobStream->Next(&readBuffer, &readLength)) {
                throw orc::ParseError("failed to read in OmniStringDirectColumnReader.next");
            }
            lastBuffer = static_cast<const char*>(readBuffer);
            lastBufferLength = static_cast<size_t>(readLength);
        }

        if (bytesBuffered < totalLength) {
            size_t moreBytes = totalLength - bytesBuffered;
            memcpy_s(ptr + bytesBuffered, moreBytes, lastBuffer, moreBytes);
            lastBuffer += moreBytes;
            lastBufferLength -= moreBytes;
        }

        auto varcharVector = reinterpret_cast<omniruntime::vec::Vector<
            omniruntime::vec::LargeStringContainer<std::string_view>>*>(vec);
        size_t filledSlots = 0;
        char* tempPtr = ptr;
        if (hasNull) {
            while (filledSlots < numValues) {
                if (!BitUtil::IsBitSet(nullsTrans, filledSlots)) {
                    //求出长度，如果为char，则需要去除最后的空格
                    auto len = lengthPtr[filledSlots];
                    if (isChar) {
                        FindLastNotEmpty(tempPtr, len);
                    }
                    auto data = std::string_view(tempPtr, len);
                    varcharVector->SetValue(filledSlots, data);

                    tempPtr += lengthPtr[filledSlots];
                } else {
                    varcharVector->SetNull(filledSlots);
                }
                filledSlots += 1;
            }
        } else {
            while (filledSlots < numValues) {
                //求出长度，如果为char，则需要去除最后的空格
                auto len = lengthPtr[filledSlots];
                if (isChar) {
                    FindLastNotEmpty(tempPtr, len);
                }
                auto data = std::string_view(tempPtr, len);
                varcharVector->SetValue(filledSlots, data);

                tempPtr += lengthPtr[filledSlots];
                filledSlots += 1;
            }
        }
    }

    void OmniDecimal64ColumnReader::next(BaseVector *vec, uint64_t numValues, 
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();

        // read the next group of scales
        auto nonNullNums = numValues - vec->GetNullCount();
        int64_t scaleBuffer[nonNullNums];
        // read dense scales
        scaleDecoder->next(scaleBuffer, nonNullNums, nullptr);
        auto nullsTrans = reinterpret_cast<uint64_t*>(nulls);
        if (hasNull) {
            auto vector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec);

            int64_t values[nonNullNums];
            ReadValuesBatch(values, nonNullNums);
            UnZigZagBatchHEFs8p2(reinterpret_cast<uint64_t*>(values), nonNullNums);
            UnScaleBatch(values, scaleBuffer, nonNullNums);

            int scaleIndex = 0;
            for(size_t i = 0; i < numValues; ++i) {
                if (!BitUtil::IsBitSet(nullsTrans, i)) {
                    vector->SetValue(i, values[scaleIndex++]);
                }
            }
        } else {
            // special case for non null
            auto values = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                static_cast<omniruntime::vec::Vector<int64_t>*>(vec));
            ReadValuesBatch(values, nonNullNums);
            UnZigZagBatchHEFs8p2(reinterpret_cast<uint64_t*>(values), nonNullNums);
            UnScaleBatch(values, scaleBuffer, nonNullNums);
        }
    }

    void OmniDecimal128ColumnReader::next(BaseVector *vec, uint64_t numValues, 
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();

        // read the next group of scales
        int64_t scaleBuffer[numValues];
        auto nullsTrans = reinterpret_cast<uint64_t*>(nulls);
        scaleDecoder->next(scaleBuffer, numValues, nullsTrans);

        auto vector = reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(vec);
        if (hasNull) {
            for(size_t i = 0; i < numValues; ++i) {
                if (!BitUtil::IsBitSet(nullsTrans, i)) {
                    orc::Int128 value = 0;
                    readInt128(value, static_cast<int32_t>(scaleBuffer[i]));
                    __int128_t dst = value.getHighBits();
                    dst <<= 64;
                    dst |= value.getLowBits();
                    vector->SetValue(i, omniruntime::type::Decimal128(dst));
                }
            }
        } else {
            for(size_t i = 0; i < numValues; ++i) {
                orc::Int128 value = 0;
                readInt128(value, static_cast<int32_t>(scaleBuffer[i]));
                __int128_t dst = value.getHighBits();
                dst <<= 64;
                dst |= value.getLowBits();
                vector->SetValue(i, omniruntime::type::Decimal128(dst));
            }
        }
    }

	void OmniDecimalHive11ColumnReader::next(BaseVector *vec, uint64_t numValues, 
        uint64_t *incomingNulls, int omniTypeId) {
        auto nulls = omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vec);
        readNulls(this, numValues, incomingNulls, nulls);
        bool hasNull = vec->HasNull();
        // read the next group of scales
        int64_t scaleBuffer[numValues];
        auto nullsTrans = reinterpret_cast<uint64_t*>(nulls);
        scaleDecoder->next(scaleBuffer, numValues, nullsTrans);

        auto vector = reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(vec);
       	if (hasNull) {
       		for (size_t i = 0; i < numValues; ++i) {
       			if (!BitUtil::IsBitSet(nullsTrans, i)) {
       				orc::Int128 value = 0;
       				if (!readInt128(value, static_cast<int32_t>(scaleBuffer[i]))) {
       					if (throwOnOverflow) {
       						throw orc::ParseError("Hive 0.11 decimal was more than 38 digits.");
       					} else {
       						*errorStream << "Warning: "
       									 << "Hive 0.11 decimal with more than 38 digits "
       									 << "replaced by NULL. \n";
       						vector->SetNull(i);
       					}
       				} else {
						__int128_t dst = value.getHighBits();
						dst <<= 64;
						dst |= value.getLowBits();
						vector->SetValue(i, omniruntime::type::Decimal128(dst));
       				}
       			}
       		}
       	} else {
       		for (size_t i = 0; i < numValues; ++i) {
       			orc::Int128 value = 0;
       			if (!readInt128(value, static_cast<int32_t>(scaleBuffer[i]))) {
					if (throwOnOverflow) {
						throw orc::ParseError("Hive 0.11 decimal was more than 38 digits.");
					} else {
						*errorStream << "Warning: "
									 << "Hive 0.11 decimal with more than 38 digits "
									 << "replaced by NULL. \n";
						vector->SetNull(i);
					}
       			} else {
					__int128_t dst = value.getHighBits();
					dst <<= 64;
					dst |= value.getLowBits();
					vector->SetValue(i, omniruntime::type::Decimal128(dst));
       			}
       		}
       	}
	}

    OmniIntegerColumnReader::OmniIntegerColumnReader(const Type& type,  StripeStreams& stripe)
        : OmniColumnReader(type, stripe) {
        RleVersion vers = omniConvertRleVersion(stripe.getEncoding(columnId).kind());
        std::unique_ptr<SeekableInputStream> stream = stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (stream == nullptr)
            throw OmniException("EXPRESSION_NOT_SUPPORT", "DATA stream not found in Integer column");
        rle = createOmniRleDecoder(std::move(stream), true, vers, memoryPool);
    }

    OmniIntegerColumnReader::~OmniIntegerColumnReader() {
        // PASS
    }

    uint64_t OmniIntegerColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        rle->skip(numValues);
        return numValues;
    }

    void OmniIntegerColumnReader::seekToRowGroup(
            std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(columnId));
    }

    const uint32_t OmniDecimal64ColumnReader::MAX_PRECISION_64;
    const uint32_t OmniDecimal64ColumnReader::MAX_PRECISION_128;
    const int64_t OmniDecimal64ColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1] =
            {1,
             10,
             100,
             1000,
             10000,
             100000,
             1000000,
             10000000,
             100000000,
             1000000000,
             10000000000,
             100000000000,
             1000000000000,
             10000000000000,
             100000000000000,
             1000000000000000,
             10000000000000000,
             100000000000000000,
             1000000000000000000};

    OmniDecimal64ColumnReader::OmniDecimal64ColumnReader(const Type& type,
                                                StripeStreams& stripe
    ): OmniColumnReader(type, stripe) {
        scale = static_cast<int32_t>(type.getScale());
        precision = static_cast<int32_t>(type.getPrecision());
        valueStream = stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (valueStream == nullptr)
            throw orc::ParseError("DATA stream not found in Decimal64Column");
        buffer = nullptr;
        bufferEnd = nullptr;
        RleVersion vers = omniConvertRleVersion(stripe.getEncoding(columnId).kind());
        std::unique_ptr<SeekableInputStream> stream =
                stripe.getStream(columnId, orc::proto::Stream_Kind_SECONDARY, true);
        if (stream == nullptr)
            throw orc::ParseError("SECONDARY stream not found in Decimal64Column");
        scaleDecoder = createOmniRleDecoder(std::move(stream), true, vers, memoryPool);
    }

    OmniDecimal64ColumnReader::~OmniDecimal64ColumnReader() {
        // PASS
    }

    uint64_t OmniDecimal64ColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        uint64_t skipped = 0;
        while (skipped < numValues) {
            readBuffer();
            if (!(0x80 & *(buffer++))) {
                skipped += 1;
            }
        }
        scaleDecoder->skip(numValues);
        return numValues;
    }

    void OmniDecimal64ColumnReader::seekToRowGroup(
            std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        valueStream->seek(positions.at(columnId));
        scaleDecoder->seek(positions.at(columnId));
        // clear buffer state after seek
        buffer = nullptr;
        bufferEnd = nullptr;
    }

    OmniDecimal128ColumnReader::OmniDecimal128ColumnReader
            (const Type& type,
             StripeStreams& stripe
            ): OmniDecimal64ColumnReader(type, stripe) {
        // PASS
    }

    OmniDecimal128ColumnReader::~OmniDecimal128ColumnReader() {
        // PASS
    }

    void OmniDecimal128ColumnReader::readInt128(orc::Int128& value, int32_t currentScale) {
        value = 0;
        orc::Int128 work;
        uint32_t offset = 0;
        while (true) {
            readBuffer();
            unsigned char ch = static_cast<unsigned char>(*(buffer++));
            work = ch & 0x7f;
            work <<= offset;
            value |= work;
            offset += 7;
            if (!(ch & 0x80)) {
                break;
            }
        }
        omniUnZigZagInt128(value);
        scaleInt128(value, static_cast<uint32_t>(scale),
                    static_cast<uint32_t>(currentScale));
    }

    OmniDecimalHive11ColumnReader::OmniDecimalHive11ColumnReader(const Type& type, StripeStreams& stripe)
    	: OmniDecimal64ColumnReader(type, stripe) {
    	scale = stripe.getForcedScaleOnHive11Decimal();
    	throwOnOverflow = stripe.getThrowOnHive11DecimalOverflow();
    	errorStream = stripe.getErrorStream();
    }

    OmniDecimalHive11ColumnReader::~OmniDecimalHive11ColumnReader() {
    	// PASS
    }

	bool OmniDecimalHive11ColumnReader::readInt128(orc::Int128& value, int32_t currentScale) {
		// -/+ 99999999999999999999999999999999999999
		static const orc::Int128 MIN_VALUE(-0x4b3b4ca85a86c47b, 0xf675ddc000000001);
		static const orc::Int128 MAX_VALUE( 0x4b3b4ca85a86c47a, 0x098a223fffffffff);

		value = 0;
		orc::Int128 work;
		uint32_t offset = 0;
		bool result = true;
		while (true) {
			readBuffer();
			unsigned char ch = static_cast<unsigned char>(*(buffer++));
			work = ch & 0x7f;
			if (offset > 128 || (offset == 126 && work > 3)) {
				result = false;
			}
			work <<= offset;
			value |= work;
			offset += 7;
			if (!(ch & 0x80)) {
				break;
			}
		}

		if (!result) {
			return result;
		}
		omniUnZigZagInt128(value);
		scaleInt128(value, static_cast<uint32_t>(scale),
					static_cast<uint32_t>(currentScale));
		return value >= MIN_VALUE && value <= MAX_VALUE;
	}

    OmniTimestampColumnReader::OmniTimestampColumnReader(const Type& type,
                                                         StripeStreams& stripe,
                                                         bool isInstantType,
                                                         common::JulianGregorianRebase *julianPtr
    ): OmniColumnReader(type, stripe),
        writerTimezone(isInstantType ?
                       orc::getTimezoneByName("GMT") :
                       stripe.getWriterTimezone()),
        readerTimezone(isInstantType ?
                       orc::getTimezoneByName("GMT") :
                       (julianPtr == nullptr ? orc::getLocalTimezone() : orc::getTimezoneByName(julianPtr->GetTz()))),
        epochOffset(writerTimezone.getEpoch()),
        sameTimezone(writerTimezone.getEpoch() == readerTimezone.getEpoch()),
        julianPtr(julianPtr) {
        RleVersion vers = omniConvertRleVersion(stripe.getEncoding(columnId).kind());
        std::unique_ptr<SeekableInputStream> stream =
                stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (stream == nullptr)
            throw orc::ParseError("DATA stream not found in Timestamp column");
        secondsRle = createOmniRleDecoder(std::move(stream), true, vers, memoryPool);
        stream = stripe.getStream(columnId, orc::proto::Stream_Kind_SECONDARY, true);
        if (stream == nullptr)
            throw orc::ParseError("SECONDARY stream not found in Timestamp column");
        nanoRle = createOmniRleDecoder(std::move(stream), false, vers, memoryPool);
    }

    OmniTimestampColumnReader::~OmniTimestampColumnReader() {
        // PASS
    }

    uint64_t OmniTimestampColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        secondsRle->skip(numValues);
        nanoRle->skip(numValues);
        return numValues;
    }

    void OmniTimestampColumnReader::seekToRowGroup(
            std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        secondsRle->seek(positions.at(columnId));
        nanoRle->seek(positions.at(columnId));
    }

    OmniStringDirectColumnReader::OmniStringDirectColumnReader(const Type& type, StripeStreams& stripe)
                                                                : OmniColumnReader(type, stripe) {
        RleVersion rleVersion = omniConvertRleVersion(stripe.getEncoding(columnId).kind());
        std::unique_ptr<SeekableInputStream> stream =
                stripe.getStream(columnId, orc::proto::Stream_Kind_LENGTH, true);
        if (stream == nullptr)
            throw orc::ParseError("LENGTH stream not found in StringDirectColumn");
        lengthRle = createOmniRleDecoder(
                std::move(stream), false, rleVersion, memoryPool);
        blobStream = stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (blobStream == nullptr)
            throw orc::ParseError("DATA stream not found in StringDirectColumn");
        lastBuffer = nullptr;
        lastBufferLength = 0;
        if (type.getKind() == orc::TypeKind::CHAR) {
            isChar = true;
        }
    }

    OmniStringDirectColumnReader::~OmniStringDirectColumnReader() {
        // PASS
    }

    uint64_t OmniStringDirectColumnReader::skip(uint64_t numValues) {
        const size_t BUFFER_SIZE = 1024;
        numValues = OmniColumnReader::skip(numValues);
        int64_t buffer[BUFFER_SIZE];
        uint64_t done = 0;
        size_t totalBytes = 0;
        // read the lengths. so we know how many bytes to skip
        while (done < numValues) {
            uint64_t step = std::min(BUFFER_SIZE,
                                     static_cast<size_t>(numValues - done));
            lengthRle->next(buffer, step, nullptr);
            totalBytes += computeSize(buffer, nullptr, step);
            done += step;
        }
        if (totalBytes <= lastBufferLength) {
            // subtract the needed bytes from the ones left over
            lastBufferLength -= totalBytes;
            lastBuffer += totalBytes;
        } else {
            // move the stream forward after accounting for the buffered bytes
            totalBytes -= lastBufferLength;
            const size_t cap = static_cast<size_t>(std::numeric_limits<int>::max());
            while (totalBytes != 0) {
                size_t step = totalBytes > cap ? cap : totalBytes;
                blobStream->Skip(static_cast<int>(step));
                totalBytes -= step;
            }
            lastBufferLength = 0;
            lastBuffer = nullptr;
        }
        return numValues;
    }

    size_t OmniStringDirectColumnReader::computeSize(const int64_t* lengths, uint64_t *nulls,
        uint64_t numValues) {
        size_t totalLength = 0;
        if (nulls) {
            for(size_t i = 0; i < numValues; ++i) {
                if (!BitUtil::IsBitSet(nulls, i)) {
                    totalLength += static_cast<size_t>(lengths[i]);
                }
            }
        } else {
            for(size_t i = 0; i < numValues; ++i) {
                totalLength += static_cast<size_t>(lengths[i]);
            }
        }
        return totalLength;
    }

    void OmniStringDirectColumnReader::seekToRowGroup(
            std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        blobStream->seek(positions.at(columnId));
        lengthRle->seek(positions.at(columnId));
        // clear buffer state after seek
        lastBuffer = nullptr;
        lastBufferLength =0;
    }

    OmniStringDictionaryColumnReader::OmniStringDictionaryColumnReader(const Type& type, StripeStreams& stripe)
        : OmniColumnReader(type, stripe), dictionary(new orc::StringDictionary(stripe.getMemoryPool())) {
        RleVersion rleVersion = omniConvertRleVersion(stripe.getEncoding(columnId)
                                                            .kind());
        uint32_t dictSize = stripe.getEncoding(columnId).dictionarysize();
        std::unique_ptr<SeekableInputStream> stream =
                stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (stream == nullptr) {
            throw orc::ParseError("DATA stream not found in StringDictionaryColumn");
        }
        rle = createOmniRleDecoder(std::move(stream), false, rleVersion, memoryPool);
        stream = stripe.getStream(columnId, orc::proto::Stream_Kind_LENGTH, false);
        if (dictSize > 0 && stream == nullptr) {
            throw orc::ParseError("LENGTH stream not found in StringDictionaryColumn");
        }
        std::unique_ptr<OmniRleDecoderV2> lengthDecoder =
                createOmniRleDecoder(std::move(stream), false, rleVersion, memoryPool);
        dictionary->dictionaryOffset.resize(dictSize + 1);
        int64_t* lengthArray = dictionary->dictionaryOffset.data();
        lengthDecoder->next(lengthArray + 1, dictSize, nullptr);
        lengthArray[0] = 0;
        for(uint32_t i = 1; i < dictSize + 1; ++i) {
            if (lengthArray[i] < 0) {
                throw orc::ParseError("Negative dictionary entry length");
            }
            lengthArray[i] += lengthArray[i - 1];
        }
        int64_t blobSize = lengthArray[dictSize];
        dictionary->dictionaryBlob.resize(static_cast<uint64_t>(blobSize));
        std::unique_ptr<SeekableInputStream> blobStream =
                stripe.getStream(columnId, orc::proto::Stream_Kind_DICTIONARY_DATA, false);
        if (blobSize > 0 && blobStream == nullptr) {
            throw orc::ParseError(
                    "DICTIONARY_DATA stream not found in StringDictionaryColumn");
        }
        omniReadFully(dictionary->dictionaryBlob.data(), blobSize, blobStream.get());
        if (type.getKind() == orc::TypeKind::CHAR) {
            isChar = true;
        }
    }

    OmniStringDictionaryColumnReader::~OmniStringDictionaryColumnReader() {
        // PASS
    }

    uint64_t OmniStringDictionaryColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        rle->skip(numValues);
        return numValues;
    }

    void OmniStringDictionaryColumnReader::seekToRowGroup(
            std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(columnId));
    }

    OmniBooleanColumnReader::OmniBooleanColumnReader(const orc::Type& type, orc::StripeStreams& stripe)
        : OmniColumnReader(type, stripe){
        std::unique_ptr<SeekableInputStream> stream =
            stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (stream == nullptr)
            throw orc::ParseError("DATA stream not found in Boolean column");
        rle = createOmniBooleanRleDecoder(std::move(stream));
    }

    OmniBooleanColumnReader::~OmniBooleanColumnReader() {
        // PASS
    }

    uint64_t OmniBooleanColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        rle->skip(numValues);
        return numValues;
    }


    void OmniBooleanColumnReader::seekToRowGroup(
        std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(columnId));
    }


    OmniByteColumnReader::OmniByteColumnReader(const Type& type, StripeStreams& stripe)
        : OmniColumnReader(type, stripe){
        std::unique_ptr<SeekableInputStream> stream =
            stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (stream == nullptr)
            throw orc::ParseError("DATA stream not found in Byte column");
        rle = createOmniByteRleDecoder(std::move(stream));
    }

    OmniByteColumnReader::~OmniByteColumnReader() {
        // PASS
    }

    uint64_t OmniByteColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);
        rle->skip(numValues);
        return numValues;
    }

    void OmniByteColumnReader::seekToRowGroup(
         std::unordered_map<uint64_t, PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(columnId));
    }


    OmniDoubleColumnReader::OmniDoubleColumnReader(const Type& type,
                                        StripeStreams& stripe
                                        ): OmniColumnReader(type, stripe),
                                           columnKind(type.getKind()),
                                           bytesPerValue((type.getKind() ==
                                                          orc::FLOAT) ? 4 : 8),
                                           bufferPointer(nullptr),
                                           bufferEnd(nullptr) {
        inputStream = stripe.getStream(columnId, orc::proto::Stream_Kind_DATA, true);
        if (inputStream == nullptr)
            throw orc::ParseError("DATA stream not found in Double column");
    }

    OmniDoubleColumnReader::~OmniDoubleColumnReader() {
    // PASS
    }

    uint64_t OmniDoubleColumnReader::skip(uint64_t numValues) {
        numValues = OmniColumnReader::skip(numValues);

        if (static_cast<size_t>(bufferEnd - bufferPointer) >=
            bytesPerValue * numValues) {
            bufferPointer += bytesPerValue * numValues;
        } else {
            size_t sizeToSkip = bytesPerValue * numValues -
                                static_cast<size_t>(bufferEnd - bufferPointer);
            const size_t cap = static_cast<size_t>(std::numeric_limits<int>::max());
            while (sizeToSkip != 0) {
                size_t step = sizeToSkip > cap ? cap : sizeToSkip;
                inputStream->Skip(static_cast<int>(step));
                sizeToSkip -= step;
            }
            bufferEnd = nullptr;
            bufferPointer = nullptr;
        }

        return numValues;
    }

    void OmniDoubleColumnReader::seekToRowGroup(
        std::unordered_map<uint64_t, orc::PositionProvider>& positions) {
        OmniColumnReader::seekToRowGroup(positions);
        inputStream->seek(positions.at(columnId));
        // clear buffer state after seek
        bufferEnd = nullptr;
        bufferPointer = nullptr;
    }
}