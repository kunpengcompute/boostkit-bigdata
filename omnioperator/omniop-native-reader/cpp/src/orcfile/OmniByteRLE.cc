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

    inline void SetNull(char *data,uint64_t position, bool isNull) {
        if (isNull) {
            data[position / 8] |= (1<<(position % 8));
        } else {
            data[position / 8] &= ~(1<<(position % 8));
        }
    }


    void OmniBooleanRleDecoder::nextNulls(char *data, uint64_t numValues, uint64_t *nulls) {

        uint64_t position = 0;

        if(nulls) {
            while(remainingBits > 0 && position < numValues) {
                if(!BitUtil::IsBitSet(nulls, position)) {
                    remainingBits -= 1;
                    SetNull(data, position, !((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1));
                } else {
                    SetNull(data, position, true);
                }
                position += 1;
            }
        } else {
            while(remainingBits > 0 && position < numValues) {
                remainingBits -= 1;
                SetNull(data, position, !((static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1));
                position += 1;
            }
        }

        uint64_t nonNulls = numValues - position;
        if(nulls) {
            for(uint64_t i = position; i < numValues; ++i) {
                if(BitUtil::IsBitSet(nulls, i)) {
                    nonNulls -= 1;
                }
            }
        }

        // fill in the remaining values
         if (nonNulls == 0) {
             while (position < numValues) {
                 SetNull(data, position++, true);
             }
         } else if (position < numValues) {
             // read the new bytes into the array
             uint64_t bytesRead = (nonNulls + 7) / 8;
             char bits[bytesRead];
             OmniByteRleDecoder::next(bits, bytesRead, nullptr);
             lastByte = bits[bytesRead - 1];
             remainingBits = bytesRead * 8 - nonNulls;
             // expand the array backwards so that we don't clobber the data
             uint64_t bitsLeft = bytesRead * 8 - remainingBits;
             if (nulls) {
                 for (int64_t i = static_cast<int64_t>(numValues) - 1; i >= static_cast<int64_t>(position); --i) {
                     if (!BitUtil::IsBitSet(nulls, i)) {
                         uint64_t shiftPosn = (-bitsLeft) % 8;
                         SetNull(data, i, !((bits[(bitsLeft -1)/8] >> shiftPosn) & 0x1));
                         bitsLeft -= 1;
                     } else {
                         SetNull(data, i, true);
                     }
                 }
             } else {
                 for (uint64_t i = 0; i < bitsLeft; i++) {
                    uint64_t byteIndex = i/8;
                    uint64_t bitPosition = 7 - (i%8);
                    SetNull(data, position + i, !((bits[byteIndex] >> bitPosition) & 0x1));
                 }
             }
         }
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
                if (BitUtil::IsBitSet(nulls, i)) {
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
                    if (!BitUtil::IsBitSet(nulls, i)) {
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

   void OmniByteRleDecoder::nextBatch(char *data, uint64_t numValues, uint64_t *nulls) {
         uint64_t position = 0;
         // skip over null values
         while (nulls && position < numValues && BitUtil::IsBitSet(nulls, position)) {
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
                 if (nulls) {
                     for(uint64_t i=0; i < count; ++i) {
                         if (!BitUtil::IsBitSet(nulls, position + i)) {
                             data[position + i] = value;
                             consumed += 1;
                         }
                     }
                 } else {
                     memset_s(data + position, count, value, count);
                     consumed = count;
                 }
             } else {
                 if (nulls) {
                     for(uint64_t i=0; i < count; ++i) {
                         if (!BitUtil::IsBitSet(nulls, position + i)) {
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
             while (nulls && position < numValues && BitUtil::IsBitSet(nulls, position)) {
                 position += 1;
             }
         }
     }
   //OmniByteRleDecoder end
}

namespace omniruntime::writer {
      const int BYTE_RLE_MINIMUM_REPEAT = 3;
      const int BYTE_RLE_MAXIMUM_REPEAT = 127 + BYTE_RLE_MINIMUM_REPEAT;
      const int BYTE_RLE_MAX_LITERAL_SIZE = 128;
    /* omni encoder start */
   OmniByteRleEncoder::OmniByteRleEncoder(std::unique_ptr<orc::BufferedOutputStream> output)
    : outputStream(std::move(output)) {
        literals = new char[BYTE_RLE_MAX_LITERAL_SIZE ];
        numLiterals = 0;
        tailRunLength = 0;
        repeat = false;
        bufferPosition = 0;
        bufferLength = 0;
        buffer = nullptr;
   }

   OmniByteRleEncoder::~OmniByteRleEncoder() {
    // PASS
    delete [] literals;
   }


  void OmniByteRleEncoder::writeByte(char c) {
    if (bufferPosition == bufferLength) {
      int addedSize = 0;
      if (!outputStream->Next(reinterpret_cast<void **>(&buffer), &addedSize)) {
        throw std::bad_alloc();
      }
      bufferPosition = 0;
      bufferLength = addedSize;
    }
    buffer[bufferPosition++] = c;
  }

  void OmniByteRleEncoder::add(const char* data, uint64_t numValues, const char* notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        write(data[i]);
      }
    }
  }

  void OmniByteRleEncoder::add(const int8_t* data,
                               uint64_t offset,
                               uint64_t numValues,
                               omniruntime::vec::NullsBuffer* nullsBuffer) {
      for (uint64_t i = 0; i < numValues; ++i) {
          if (!nullsBuffer || !nullsBuffer->IsNull(offset + i)) {
              write(static_cast<char>(data[i]));
          }
      }
  }

  void OmniByteRleEncoder::add(ByteDictVector* data,
                               uint64_t offset,
                               uint64_t numValues) {
      for (uint64_t i = 0; i < numValues; ++i) {
          uint64_t rowIdx = offset + i;
          if (!data->IsNull(rowIdx)) {
              write(static_cast<char>(data->GetValue(rowIdx)));
          }
      }
  }

    void OmniByteRleEncoder::writeValues() {
    if (numLiterals != 0) {
      if (repeat) {
        writeByte(
            static_cast<char>(numLiterals - static_cast<int>(BYTE_RLE_MINIMUM_REPEAT)));
        writeByte(literals[0]);
      } else {
        writeByte(static_cast<char>(-numLiterals));
        for (int i = 0; i < numLiterals; ++i) {
          writeByte(literals[i]);
        }
      }
      repeat = false;
      tailRunLength = 0;
      numLiterals = 0;
    }
  }

  uint64_t OmniByteRleEncoder::flush() {
    writeValues();
    outputStream->BackUp(bufferLength - bufferPosition);
    uint64_t dataSize = outputStream->flush();
    bufferLength = bufferPosition = 0;
    return dataSize;
  }

  void OmniByteRleEncoder::write(char value) {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      if (value == literals[0]) {
        numLiterals += 1;
        if (numLiterals == BYTE_RLE_MAXIMUM_REPEAT) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
    } else {
      if (value == literals[numLiterals - 1]) {
        tailRunLength += 1;
      } else {
        tailRunLength = 1;
      }
      if (tailRunLength == BYTE_RLE_MINIMUM_REPEAT ) {
        if (numLiterals + 1 == BYTE_RLE_MINIMUM_REPEAT ) {
          repeat = true;
          numLiterals += 1;
        } else {
          numLiterals -= static_cast<int>(BYTE_RLE_MINIMUM_REPEAT  - 1);
          writeValues();
          literals[0] = value;
          repeat = true;
          numLiterals = BYTE_RLE_MINIMUM_REPEAT ;
        }
      } else {
        literals[numLiterals++] = value;
        if (numLiterals == BYTE_RLE_MAX_LITERAL_SIZE ) {
          writeValues();
        }
      }
    }
  }

  uint64_t OmniByteRleEncoder::getBufferSize() const {
    return outputStream->getSize();
  }

  void OmniByteRleEncoder::recordPosition(orc::PositionRecorder *recorder) const {
    uint64_t flushedSize = outputStream->getSize();
    uint64_t unflushedSize = static_cast<uint64_t>(bufferPosition);
    if (outputStream->isCompressed()) {
      // start of the compression chunk in the stream
      recorder->add(flushedSize);
      // number of decompressed bytes that need to be consumed
      recorder->add(unflushedSize);
    } else {
      flushedSize -= static_cast<uint64_t>(bufferLength);
      // byte offset of the RLE run’s start location
      recorder->add(flushedSize + unflushedSize);
    }
    recorder->add(static_cast<uint64_t>(numLiterals));
  }

  std::unique_ptr<orc::ByteRleEncoder> createOmniByteRleEncoder
                              (std::unique_ptr<orc::BufferedOutputStream> output) {
    return std::unique_ptr<orc::ByteRleEncoder>(new OmniByteRleEncoder(std::move(output)));
  }


  OmniBooleanRleEncoder::OmniBooleanRleEncoder(std::unique_ptr<orc::BufferedOutputStream> output)
    : OmniByteRleEncoder(std::move(output)) {
    bitsRemained = 8;
    current = static_cast<char>(0);
  }

  void OmniBooleanRleEncoder::add(
                                  const char* data,
                                  uint64_t numValues,
                                  const char* notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (bitsRemained == 0) {
        write(current);
        current = static_cast<char>(0);
        bitsRemained = 8;
      }

      if (!notNull || notNull[i]) {
        if (!data || data[i]) {
          current =
            static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
        }
        --bitsRemained;
      }
    }
    if (bitsRemained == 0) {
      write(current);
      current = static_cast<char>(0);
      bitsRemained = 8;
    }
  }

  void OmniBooleanRleEncoder::add(BoolDictVector *vec, uint64_t offset, uint64_t numValues) {
    bool hasNull = vec->HasNull();
    uint64_t curPos  = offset;
    for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
      if (bitsRemained == 0) {
        write(current);
        current = static_cast<char>(0);
        bitsRemained = 8;
      }
      if (!hasNull || !vec->IsNull(curPos)) {
        if (vec->GetValue(curPos)) {
          current =
            static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
        }
        --bitsRemained;
      }
    }
    if (bitsRemained == 0) {
      write(current);
      current = static_cast<char>(0);
      bitsRemained = 8;
    }
  }

  void OmniBooleanRleEncoder::add(
          omniruntime::vec::NullsBuffer *nullsBuffer,
          omniruntime::vec::NullsBuffer *pNullsBuffer,
          uint64_t offset,
          uint64_t numValues) {

      if (numValues == 0) return;

      const bool pHasNull = (pNullsBuffer != nullptr && pNullsBuffer->HasNull());
      const bool cHasNull = (nullsBuffer != nullptr && nullsBuffer->HasNull());

      if (!pHasNull) {
          // path 1: pNullsBuffer doesn't have null, nullsBuffer doesn't have null
          if (!cHasNull) {
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (bitsRemained == 0) {
                      write(current);
                      current = 0;
                      bitsRemained = 8;
                  }
                  current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
                  --bitsRemained;
              }
          } else {
              // path 2: pNullsBuffer doesn't have null, nullsBuffer has null
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (bitsRemained == 0) {
                      write(current);
                      current = 0;
                      bitsRemained = 8;
                  }
                  if (!nullsBuffer->IsNull(offset + i)) {
                      current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
                  }
                  --bitsRemained;
              }
          }
      } else {
          // path 3: pNullsBuffer has null, nullsBuffer doesn't have null
          if (!cHasNull) {
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (bitsRemained == 0) {
                      write(current);
                      current = 0;
                      bitsRemained = 8;
                  }
                  if (!pNullsBuffer->IsNull(offset + i)) {
                      current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
                  }
                  --bitsRemained;
              }
          } else {
              // path 4: pNullsBuffer has null, nullsBuffer has nulls
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (bitsRemained == 0) {
                      write(current);
                      current = 0;
                      bitsRemained = 8;
                  }
                  if (!pNullsBuffer->IsNull(offset + i) && !nullsBuffer->IsNull(offset + i)) {
                      current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
                  }
                  --bitsRemained;
              }
          }
      }

      if (bitsRemained == 0) {
          write(current);
          current = static_cast<char>(0);
          bitsRemained = 8;
      }
  }

  void OmniBooleanRleEncoder::add(
          const bool* data,
          uint64_t offset,
          uint64_t numValues,
          omniruntime::vec::NullsBuffer* nullsBuffer) {
      if (numValues == 0) return;

      const bool hasNull = (nullsBuffer != nullptr && nullsBuffer->HasNull());

      if (!hasNull) {
          for (uint64_t i = 0; i < numValues; ++i) {
              if (bitsRemained == 0) {
                  write(current);
                  current = 0;
                  bitsRemained = 8;
              }
              if (data[i]) {
                  current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
              }
              --bitsRemained;
          }
      } else {
          for (uint64_t i = 0; i < numValues; ++i) {
              if (bitsRemained == 0) {
                  write(current);
                  current = 0;
                  bitsRemained = 8;
              }
              if (!nullsBuffer->IsNull(offset + i)) {
                  if (data[i]) {
                      current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
                  }
                  --bitsRemained;
              }
          }
      }

      if (bitsRemained == 0) {
          write(current);
          current = static_cast<char>(0);
          bitsRemained = 8;
      }
  }

  uint64_t OmniBooleanRleEncoder::flush() {
    if (bitsRemained != 8) {
      write(current);
    }
    bitsRemained = 8;
    current = static_cast<char>(0);
    return OmniByteRleEncoder::flush();
  }

  void OmniBooleanRleEncoder::recordPosition(orc::PositionRecorder* recorder) const {
    OmniByteRleEncoder::recordPosition(recorder);
    recorder->add(static_cast<uint64_t>(8 - bitsRemained));
  }


   std::unique_ptr<orc::ByteRleEncoder> createOmniBooleanRleEncoder
        (std::unique_ptr<orc::BufferedOutputStream> output)
   {
      orc::ByteRleEncoder* encoder =
         new OmniBooleanRleEncoder(std::move(output)) ;
            return std::unique_ptr<orc::ByteRleEncoder>(encoder);
   }
   /* omni encoder end  */
}