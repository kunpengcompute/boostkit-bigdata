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

#include "OmniRLEv2.hh"
#include "OmniColReader.hh"
#include "vector/vector_helper.h"

namespace omniruntime::reader {

    const int MINIMUM_REPEAT = 3;
    const int MAXIMUM_REPEAT = 127 + MINIMUM_REPEAT;

    void OmniBooleanRleDecoder::seek(orc::PositionProvider& location) {
        OmniByteRleDecoder::seek(location);
        uint64_t consumed = location.next();
        remainingBits = 0;
        if (consumed > 8) {
            throw orc::ParseError("bad position");
        }
        if (consumed != 0) {
            remainingBits = 8 - consumed;
            OmniByteRleDecoder::next(&lastByte, 1, nullptr);
        }
    }

    void OmniBooleanRleDecoder::skip(uint64_t numValues) {
        if (numValues <= remainingBits) {
            remainingBits -= numValues;
        } else {
            numValues -= remainingBits;
            uint64_t bytesSkipped = numValues / 8;
            OmniByteRleDecoder::skip(bytesSkipped);
            if (numValues % 8 != 0) {
                OmniByteRleDecoder::next(&lastByte, 1, nullptr);
                remainingBits = 8 - (numValues % 8);
            } else {
                remainingBits = 0;
            }
        }
    }

    void OmniBooleanRleDecoder::next(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
        const orc::Type* baseTp, int omniTypeId) {
        auto dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId);
        std::unique_ptr<omniruntime::vec::BaseVector> tempOmnivec = makeNewVector(numValues, baseTp, dataTypeId);
        auto pushOmniVec = tempOmnivec.get();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                nextByType<omniruntime::type::OMNI_BOOLEAN>(pushOmniVec, numValues, notNull, baseTp, omniTypeId);
                break;
            case omniruntime::type::OMNI_SHORT:
                throw std::runtime_error("OmniBooleanRleDecoder SHORT not finished!!!");
                break;
            case omniruntime::type::OMNI_INT:
                throw std::runtime_error("OmniBooleanRleDecoder INT not finished!!!");
                break;
            case omniruntime::type::OMNI_LONG:
                throw std::runtime_error("OmniBooleanRleDecoder LONG not finished!!!");
                break;
            case omniruntime::type::OMNI_TIMESTAMP:
                throw std::runtime_error("OmniBooleanRleDecoder TIMESTAMP not finished!!!");
                break;
            case omniruntime::type::OMNI_DATE32:
                throw std::runtime_error("OmniBooleanRleDecoder DATE32 not finished!!!");
                break;
            case omniruntime::type::OMNI_DATE64:
                throw std::runtime_error("OmniBooleanRleDecoder DATE64 not finished!!!");
                break;
            case omniruntime::type::OMNI_DOUBLE:
                throw std::runtime_error("OmniBooleanRleDecoder DOUBLE not finished!!!");
                break;
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("OmniBooleanRleDecoder CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("OmniBooleanRleDecoder VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("OmniBooleanRleDecoder DECIMAL64 should not in here!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("OmniBooleanRleDecoder DECIMAL64 should not in here!!!");
            default:
                printf("OmniBooleanRleDecoder switch no process!!!");
        }

        omnivec = tempOmnivec.release();
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    void OmniBooleanRleDecoder::nextByType(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
                                                const orc::Type* baseTp, int omniTypeId) {
        using namespace omniruntime::type;
        using T = typename NativeType<TYPE_ID>::type;
        auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(omnivec);

        // next spot to fill in
        uint64_t position = 0;

        // use up any remaining bits
        if (notNull) {
            while(remainingBits > 0 && position < numValues) {
                if (notNull[position]) {
                    remainingBits -= 1;
                    vec->SetValue(static_cast<int>(position), static_cast<T>((static_cast<unsigned char>(lastByte) >>
                                    remainingBits) & 0x1));
                } else {
                    vec->SetNull(static_cast<int>(position));
                }
                position += 1;
            }
        } else {
            while(remainingBits > 0 && position < numValues) {
                remainingBits -= 1;
                vec->SetValue(static_cast<int>(position++), static_cast<T>((static_cast<unsigned char>(lastByte) >>
                                remainingBits) & 0x1));
            }
        }

        // count the number of nonNulls remaining
        uint64_t nonNulls = numValues - position;
        if (notNull) {
            for(uint64_t i = position; i < numValues; ++i) {
                if (!notNull[i]) {
                    nonNulls -= 1;
                }
            }
        }

        // fill in the remaining values
        if (nonNulls == 0) {
            while (position < numValues) {
                vec->SetNull(static_cast<int>(position++));
            }
        } else if (position < numValues) {
            // read the new bytes into the array
            uint64_t bytesRead = (nonNulls + 7) / 8;
            auto *values = reinterpret_cast<char *>(omniruntime::vec::VectorHelper::UnsafeGetValues(omnivec));
            OmniByteRleDecoder::next(values + position, bytesRead, nullptr);
            lastByte = static_cast<char>(vec->GetValue(position + bytesRead - 1));
            remainingBits = bytesRead * 8 - nonNulls;
            // expand the array backwards so that we don't clobber the data
            uint64_t bitsLeft = bytesRead * 8- remainingBits;
            if (notNull) {
                for (int64_t i =static_cast<int64_t>(numValues) - 1;
                     i >= static_cast<int64_t>(position); --i) {
                    if (notNull[i]) {
                        uint64_t shiftPosn = (-bitsLeft) % 8;
                        auto value = static_cast<uint64_t>(vec->GetValue(position + (bitsLeft - 1) / 8)) >> shiftPosn;
                        vec->SetValue(static_cast<int>(i), static_cast<T>(value & 0x1));
                        bitsLeft -= 1;
                    } else {
                        vec->SetNull(static_cast<int>(i));
                    }
                }
            } else {
                for(int64_t i = static_cast<int64_t>(numValues) - 1;
                    i >= static_cast<int64_t>(position); --i, --bitsLeft) {
                    uint64_t shiftPosn = (-bitsLeft) % 8;
                    auto value = static_cast<uint64_t>(vec->GetValue(position + (bitsLeft - 1) / 8)) >> shiftPosn;
                    vec->SetValue(static_cast<int>(i), static_cast<T>(value & 0x1));
                }
            }
        }
    }

    OmniBooleanRleDecoder::OmniBooleanRleDecoder
                                (std::unique_ptr<orc::SeekableInputStream> input
                                ): OmniByteRleDecoder(std::move(input)) {
        remainingBits = 0;
        lastByte = 0;
    }

    OmniBooleanRleDecoder::~OmniBooleanRleDecoder() {
        //pass
    }

    // OmniBooleanRleDecoder start
    void OmniByteRleDecoder::nextBuffer() {
        int bufferLength;
        const void* bufferPointer;
        bool result = inputStream->Next(&bufferPointer, &bufferLength);
        if (!result) {
            throw orc::ParseError("bad read in nextBuffer");
        }
        bufferStart = static_cast<const char*>(bufferPointer);
        bufferEnd = bufferStart + bufferLength;
    }

    signed char OmniByteRleDecoder::readByte() {
        if (bufferStart == bufferEnd) {
            nextBuffer();
        }
        return *(bufferStart++);
    }

    void OmniByteRleDecoder::readHeader() {
        signed char ch = readByte();
        if (ch < 0) {
            remainingValues = static_cast<size_t>(-ch);
            repeating = false;
        } else {
            remainingValues = static_cast<size_t>(ch) + MINIMUM_REPEAT;
            repeating = true;
            value = readByte();
        }
    }

    OmniByteRleDecoder::OmniByteRleDecoder(std::unique_ptr<orc::SeekableInputStream> input) {
        inputStream = std::move(input);
        repeating = false;
        remainingValues = 0;
        value = 0;
        bufferStart = nullptr;
        bufferEnd = nullptr;
    }

    OmniByteRleDecoder::~OmniByteRleDecoder() {
        //PASS
    }

    void OmniByteRleDecoder::seek(orc::PositionProvider& location) {
        // move the input stream
        inputStream->seek(location);
        // force a re-read from the stream
        bufferEnd = bufferStart;
        // read a new header;
        readHeader();
        // skip ahead the given number of records
        OmniByteRleDecoder::skip(location.next());
    }

    void OmniByteRleDecoder::skip(uint64_t numValues) {
        while (numValues > 0) {
            if (remainingValues == 0) {
                readHeader();
            }
            size_t count = std::min(static_cast<size_t>(numValues), remainingValues);
            remainingValues -= count;
            numValues -= count;
            // for literals we need to skip over count bytes, which may involve
            // reading from the underlying stream
            if (!repeating) {
                size_t consumedBytes = count;
                while (consumedBytes > 0) {
                    if (bufferStart == bufferEnd) {
                        nextBuffer();
                    }
                    size_t skipSize = std::min(static_cast<size_t>(consumedBytes),
                                                static_cast<size_t>(bufferEnd -
                                                                    bufferStart));
                    bufferStart += skipSize;
                    consumedBytes -= skipSize;
                }
            }
        }
    }

    void OmniByteRleDecoder::next(char* data, uint64_t numValues,
                                char* notNull) {
        uint64_t position = 0;
        // skip over null values
        while (notNull && position < numValues && !notNull[position]) {
            position += 1;
        }
        while (position < numValues) {
            // if we are out of values, read more
            if (remainingValues == 0) {
                readHeader();
            }
            // how many do we read out of this block?
            size_t count = std::min(static_cast<size_t>(numValues - position),
                                    remainingValues);
            uint64_t consumed = 0;
            if (repeating) {
                if (notNull) {
                    for(uint64_t i=0; i < count; ++i) {
                        if (notNull[position + i]) {
                            data[position + i] = value;
                            consumed += 1;
                        }
                    }
                } else {
                    memset_s(data + position, count, value, count);
                    consumed = count;
                }
            } else {
                if (notNull) {
                    for(uint64_t i=0; i < count; ++i) {
                        if (notNull[position + i]) {
                            data[position + i] = readByte();
                            consumed += 1;
                        }
                    }
                } else {
                    uint64_t i = 0;
                    while (i < count) {
                        if (bufferStart == bufferEnd) {
                            nextBuffer();
                        }
                        uint64_t copyBytes =
                        std::min(static_cast<uint64_t>(count - i),
                                                    static_cast<uint64_t>(bufferEnd - bufferStart));
                        memcpy_s(data + position + i, copyBytes, bufferStart, copyBytes);
                        bufferStart += copyBytes;
                        i += copyBytes;
                    }
                    consumed = count;
                }
            }
            remainingValues -= consumed;
            position += count;
            // skip over any null values
            while (notNull && position < numValues && !notNull[position]) {
                position += 1;
            }
        }
    }

    void OmniByteRleDecoder::next(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
                                    const orc::Type* baseTp, int omniTypeId) {
        auto dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId);
        std::unique_ptr<omniruntime::vec::BaseVector> tempOmnivec = makeNewVector(numValues, baseTp, dataTypeId);
        auto pushOmniVec = tempOmnivec.get();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                nextByType<omniruntime::type::OMNI_BOOLEAN>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_SHORT:
                nextByType<omniruntime::type::OMNI_SHORT>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_INT:
                nextByType<omniruntime::type::OMNI_INT>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_LONG:
                nextByType<omniruntime::type::OMNI_LONG>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_TIMESTAMP:
                nextByType<omniruntime::type::OMNI_TIMESTAMP>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_DATE32:
                nextByType<omniruntime::type::OMNI_DATE32>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_DATE64:
                nextByType<omniruntime::type::OMNI_DATE64>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_DOUBLE:
                nextByType<omniruntime::type::OMNI_DOUBLE>
                        (pushOmniVec, numValues, notNull, baseTp);
                break;
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("OmniByteRleDecoder CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("OmniByteRleDecoder VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("OmniByteRleDecoder DECIMAL64 not finished!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("OmniByteRleDecoder DECIMAL64 not finished!!!");
            default:
                printf("OmniByteRleDecoder swtich no process!!!");
        }

        omnivec = tempOmnivec.release();
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    void OmniByteRleDecoder::nextByType(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
                    const orc::Type* baseTp) {
        using namespace omniruntime::type;
        using T = typename NativeType<TYPE_ID>::type;
        auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(omnivec);

        uint64_t position = 0;
        // skip over null values
        while (notNull && position < numValues && !notNull[position]) {
                    position += 1;
        }
        while (position < numValues) {
            // if we are out of values, read more
            if (remainingValues == 0) {
                readHeader();
            }
            // how many do we read out of this block?
            size_t count = std::min(static_cast<size_t>(numValues - position),
                                                        remainingValues);
            uint64_t consumed = 0;
            if (repeating) {
                if (notNull) {
                    for(uint64_t i=0; i < count; ++i) {
                        if (notNull[position + i]) {
                            vec->SetValue(static_cast<int>(position + i), static_cast<T>(value));
                            consumed += 1;
                        } else {
                            vec->SetNull(static_cast<int>(position + i));
                        }
                    }
                } else {
                    for (uint64_t i = position; i < position + count; ++i) {
                        vec->SetValue(static_cast<int>(i), static_cast<int>(value));
                    }
                    consumed = count;
                }
            } else {
                if (notNull) {
                    for(uint64_t i = 0; i < count; ++i) {
                        if (notNull[position + i]) {
                            vec->SetValue(static_cast<int>(position + i), static_cast<T>(readByte()));
                            consumed += 1;
                        } else {
                            vec->SetNull(static_cast<int>(position + i));
                        }
                    }
                } else {
                    uint64_t i = 0;
                    while (i < count) {
                        if (bufferStart == bufferEnd) {
                            nextBuffer();
                        }
                        uint64_t copyBytes =
                        std::min(static_cast<uint64_t>(count - i),
                                static_cast<uint64_t>(bufferEnd - bufferStart));
                        vec->SetValues(static_cast<int>(position + i), bufferStart, static_cast<int>(copyBytes));
                        bufferStart += copyBytes;
                        i += copyBytes;
                    }
                    consumed = count;
                }
            }
            remainingValues -= consumed;
            position += count;
            // skip over any null values
            while (notNull && position < numValues && !notNull[position]) {
                position += 1;
            }
        }
   }
   //OmniByteRleDecoder end
}
