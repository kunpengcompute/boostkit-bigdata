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
#include "OrcDecodeUtils.hh"

using omniruntime::vec::NullsHelper;

namespace omniruntime::reader {

    const int MINIMUM_REPEAT = 3;
    const int MAXIMUM_REPEAT = 127 + MINIMUM_REPEAT;
    const uint32_t BITS_OF_BYTE = 8;

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

    void OmniBooleanRleDecoder::nextNulls(std::shared_ptr<NullsHelper> data, uint64_t numValues, 
        std::shared_ptr<NullsHelper> nulls) {
        // next spot to fill in
        uint64_t position = 0;
        // use up any remaining bits
        if (nulls) {
            while (remainingBits > 0 && position < numValues) {
                if (!(*nulls)[position]) {
                    remainingBits -= 1;
                    data->SetNull(position, !((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1));
                } else {
                    data->SetNull(position, true);
                }
                position += 1;
            }
        } else {
            while (remainingBits > 0 && position < numValues) {
                remainingBits -= 1;
                data->SetNull(position, !((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1));
                position += 1;
            }
        }
        // count the number of nulls remaining
        uint64_t nonNulls = numValues - position;
        if (nulls) {
            for (uint64_t i = position; i < numValues; ++i) {
                if ((*nulls)[i]) {
                    nonNulls -= 1;
                }
            }
        }
        // fill in the remaining values
        if (nonNulls == 0) {
            while (position < numValues) {
                data->SetNull(position++, true);
            }
        } else if (position < numValues) {
            // read the new bytes into array
            uint64_t bytesRead = (nonNulls + 7) / 8;
            char bits[bytesRead];
            OmniByteRleDecoder::next(bits, bytesRead, nullptr);
            lastByte = bits[bytesRead - 1];
            remainingBits = bytesRead * 8 - nonNulls;
            // expand the array backwards so that we don't clobber the data
            uint64_t bitsLeft = bytesRead * 8 - remainingBits;
            if (nulls) {
                for (int64_t i = static_cast<int64_t>(numValues) - 1; i >= static_cast<int64_t>(position); --i) {
                    if (!(*nulls)[i]) {
                        uint64_t shiftPosn = (-bitsLeft) % 8;
                        data->SetNull(i, !((bits[(bitsLeft - 1) / 8] >> shiftPosn) & 0x1));
                        bitsLeft -= 1;
                    } else {
                        data->SetNull(i, true);
                    }
                }
            } else {
                for (int32_t i = 0; i < bitsLeft; i++) {
                    uint64_t byteIndex = i / 8;
                    uint64_t bitPosition = 7 - (i % 8);
                    data->SetNull(position + i, !((bits[byteIndex] >> bitPosition) & 0x1));
                }
            }
        }
    }


    void OmniBooleanRleDecoder::next(omniruntime::vec::BaseVector *omnivec, uint64_t numValues, 
        std::shared_ptr<NullsHelper> nulls, int omniTypeId) {
        switch (omniTypeId) {
            case omniruntime::type::OMNI_BOOLEAN: {
                auto boolValues = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(
                    static_cast<omniruntime::vec::Vector<bool>*>(omnivec));
                return next(boolValues, numValues, nulls);
            }
            default:
                throw std::runtime_error("OmniBooleanRleDecoder not support type: " + omniTypeId);
        }
    }

    void OmniBooleanRleDecoder::next(char *data, uint64_t numValues, std::shared_ptr<NullsHelper> nulls) {
        next<char>(data, numValues, nulls);
    }

    template <typename T>
    void OmniBooleanRleDecoder::next(T *data, uint64_t numValues, std::shared_ptr<NullsHelper> nulls) {
        // next spot to fill in
        uint64_t position = 0;

        // use up any remaining bits
        if (nulls) {
            while (remainingBits > 0 && position < numValues) {
                if (!(*nulls)[position]) {
                    remainingBits -= 1;
                    data[position] = static_cast<T>((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1);
                } else {
                    data[position] = 0;
                }
                position += 1;
            }
        } else {
            while (remainingBits > 0 && position < numValues) {
                remainingBits -= 1;
                data[position++] = static_cast<T>((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1);
            }
        }

        // count the number of nonNulls remaining
        uint64_t nonNulls = numValues - position;
        if (nulls) {
            for (uint64_t i = position; i < numValues; ++i) {
                if ((*nulls)[i]) {
                    nonNulls -= 1;
                }
            }
        }

        // fill in the remaining values
        if (nonNulls == 0) {
            while (position < numValues) {
                data[position++] = 0;
            }
        } else if (position < numValues) {
            // read the new bytes into the array
            uint64_t bytesRead = (nonNulls + 7) / 8;
            OmniByteRleDecoder::next(reinterpret_cast<char *>(data + position), bytesRead, nullptr);
            lastByte = data[position + bytesRead - 1];
            remainingBits = bytesRead * 8 - nonNulls;
            // expand the array backwards so that we don't clobber the data
            uint64_t bitsLeft = bytesRead * 8 - remainingBits;
            if (nulls) {
                for (int64_t i = static_cast<int64_t>(numValues) - 1; i >= static_cast<int64_t>(position); --i) {
                    if (!(*nulls)[i]) {
                        uint64_t shiftPosn = (-bitsLeft) % 8;
                        data[i] = static_cast<T>((data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1);
                        bitsLeft -= 1;
                    } else {
                        data[i] = 0;
                    }
                }
            } else {
                for (int64_t i = static_cast<int64_t>(numValues) - 1;
                    i >= static_cast<int64_t>(position); --i, --bitsLeft) {
                    uint64_t shiftPosn = (-bitsLeft) % 8;
                    data[i] = static_cast<T>((data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1);
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

    void OmniByteRleDecoder::next(char* data, uint64_t numValues, char* notNull) {
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

   //OmniByteRleDecoder end
}
