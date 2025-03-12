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
            reversedAndFlipLastByte = bitNotFlip[lastByte];
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
                reversedAndFlipLastByte = bitNotFlip[lastByte];
                remainingBits = 8 - (numValues % 8);
            } else {
                remainingBits = 0;
            }
        }
    }

    void OmniBooleanRleDecoder::nextNulls(char *data, uint64_t numValues, uint64_t *nulls) {
        if (nulls) {
            throw std::runtime_error("Not implemented yet for struct type!");
        }

        uint64_t nonNulls = numValues;

        const uint32_t outputBytes = (numValues + 7) / 8;
        if (nonNulls == 0) {
            ::memset(data, 1, outputBytes);
            return;
        }

        if (remainingBits >= nonNulls) {
            // handle remaining bits, which can cover this round
            data[0] = reversedAndFlipLastByte >> (8 - remainingBits) & 0xff >> (8 - nonNulls);
            remainingBits -= nonNulls;
        } else {
            // put the remaining bits, if any, into previousByte.
            uint8_t previousByte{0};
            if (remainingBits > 0) {
                previousByte = reversedAndFlipLastByte >> (8 - remainingBits);
            }

            // compute byte size that should read
            uint64_t bytesRead = (nonNulls - remainingBits + 7) / 8;
            OmniByteRleDecoder::next(data, bytesRead, nullptr);

            ReverseAndFlipBytes(reinterpret_cast<uint8_t*>(data), bytesRead);
            reversedAndFlipLastByte = data[bytesRead - 1];

            // now shift the data in place
            if (remainingBits > 0 ) {
                uint64_t nonNullDWords = nonNulls / 64;
                for (uint64_t i = 0; i < nonNullDWords; i++) {
                    uint64_t tmp = reinterpret_cast<uint64_t*>(data)[i];
                    reinterpret_cast<uint64_t*>(data)[i] =
                        previousByte | tmp << remainingBits; // previousByte is LSB
                    previousByte = (tmp >> (64 - remainingBits)) & 0xff;
                }

                // shift 8 bits a time for the remaining bits
                const uint64_t nonNullOutputBytes = (nonNulls + 7) / 8;
                for (int32_t i = nonNullDWords * 8; i < nonNullOutputBytes; ++i) {
                    uint8_t tmp = data[i]; // already reversed
                    data[i] = previousByte | tmp << remainingBits; // previousByte is LSB
                    previousByte = tmp >> (8 - remainingBits);
                }
            }
            remainingBits = bytesRead * 8 + remainingBits - nonNulls;
        }

        // clear the most significant bits in the last byte which will be processed in the next round
        data[outputBytes - 1] &= 0xff >> (outputBytes * 8 - numValues);
    }


    void OmniBooleanRleDecoder::next(omniruntime::vec::BaseVector *omnivec, uint64_t numValues, 
        uint64_t *nulls, int omniTypeId) {
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

    void OmniBooleanRleDecoder::next(char *data, uint64_t numValues, uint64_t *nulls) {
        next<char>(data, numValues, nulls);
    }

    template <typename T>
    void OmniBooleanRleDecoder::next(T *data, uint64_t numValues, uint64_t *nulls) {
        // next spot to fill in
        uint64_t position = 0;

        // use up any remaining bits
        if (nulls) {
            while (remainingBits > 0 && position < numValues) {
                if (!BitUtil::IsBitSet(nulls, position)) {
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
                if (BitUtil::IsBitSet(nulls, position)) {
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
                    if (!BitUtil::IsBitSet(nulls, position)) {
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
        reversedAndFlipLastByte = 0;
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
