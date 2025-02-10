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
#include "orc/RLEV2Util.hh"

using omniruntime::vec::VectorBatch;

namespace omniruntime::reader {
    std::unique_ptr<omniruntime::vec::BaseVector> makeFixedLengthVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                return std::make_unique<omniruntime::vec::Vector<bool>>(numValues);
            case omniruntime::type::OMNI_SHORT:
                return std::make_unique<omniruntime::vec::Vector<int16_t>>(numValues);
            case omniruntime::type::OMNI_INT:
                return std::make_unique<omniruntime::vec::Vector<int32_t>>(numValues);
            case omniruntime::type::OMNI_LONG:
                return std::make_unique<omniruntime::vec::Vector<int64_t>>(numValues);
            case omniruntime::type::OMNI_DATE32:
                return std::make_unique<omniruntime::vec::Vector<int32_t>>(numValues);
            case omniruntime::type::OMNI_DATE64:
                return std::make_unique<omniruntime::vec::Vector<int64_t>>(numValues);
            default:
                throw std::runtime_error("MakeFixedLengthVector Not support for this type: " + dataTypeId);
        }
    }

    std::unique_ptr<omniruntime::vec::BaseVector> makeDoubleVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_DOUBLE:
            	return std::make_unique<omniruntime::vec::Vector<double>>(numValues);
            default:
                throw std::runtime_error("MakeDoubleVector Not support double vector for this type: " + dataTypeId);
        }
    }

    std::unique_ptr<omniruntime::vec::BaseVector> makeDecimalVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_DECIMAL64:
                return std::make_unique<omniruntime::vec::Vector<int64_t>>(numValues);
            case omniruntime::type::OMNI_DECIMAL128:
                return std::make_unique<omniruntime::vec::Vector<omniruntime::vec::Decimal128>>(numValues);
            default:
                throw std::runtime_error("makeDecimalVector Not support vector for this type: " + dataTypeId);
        }
    }

    std::unique_ptr<omniruntime::vec::BaseVector> makeVarcharVector(uint64_t numValues,
        omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_CHAR:
            case omniruntime::type::OMNI_VARCHAR:
                return std::make_unique<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>>>(numValues);
            default:
                throw std::runtime_error("MakeVarcharVector Not support vector for this type: " + dataTypeId);
        }
    }

    std::unique_ptr<omniruntime::vec::BaseVector> makeNewVector(uint64_t numValues, const orc::Type* baseTp,
        omniruntime::type::DataTypeId dataTypeId) {
        switch (baseTp->getKind()) {
            case orc::TypeKind::BOOLEAN:
            case orc::TypeKind::SHORT:
            case orc::TypeKind::DATE:
            case orc::TypeKind::INT:
            case orc::TypeKind::TIMESTAMP:
            case orc::TypeKind::TIMESTAMP_INSTANT:
            case orc::TypeKind::LONG:
                return makeFixedLengthVector(numValues, dataTypeId);
            case orc::TypeKind::DOUBLE:
                return makeDoubleVector(numValues, dataTypeId);
            case orc::TypeKind::CHAR:
            case orc::TypeKind::STRING:
            case orc::TypeKind::VARCHAR:
                return makeVarcharVector(numValues, dataTypeId);
            case orc::TypeKind::DECIMAL:
                return makeDecimalVector(numValues, dataTypeId);
            default: {
                throw std::runtime_error("Not support For This ORC Type: " + baseTp->getKind());
            }
        }
    }

    void OmniRleDecoderV2::next(omniruntime::vec::BaseVector*& omnivec, uint64_t numValues, char* notNull,
                                const orc::Type* baseTp, int omniTypeId) {
        uint64_t nRead = 0;

        auto dataTypeId = static_cast<omniruntime::type::DataTypeId>(omniTypeId);
        std::unique_ptr<omniruntime::vec::BaseVector> tempOmnivec = makeNewVector(numValues, baseTp, dataTypeId);
        auto pushOmniVec = tempOmnivec.get();

        while (nRead < numValues) {
            // SKip any nulls before attempting to read first byte.
            while (notNull && !notNull[nRead]) {
                tempOmnivec->SetNull(nRead);
                if (++nRead == numValues) {
                    omnivec = tempOmnivec.release();
                    return; //ended with null values
                }
            }

            if (runRead == runLength) {
                resetRun();
                firstByte = readByte();
            }

            uint64_t offset = nRead, length = numValues - nRead;

            orc::EncodingType enc = static_cast<orc::EncodingType>((firstByte >> 6) & 0x03);
                switch (static_cast<int64_t>(enc)) {
                case orc::SHORT_REPEAT:
                    nRead += nextShortRepeatsByType(pushOmniVec, offset, length, notNull, dataTypeId);
                    break;
                case orc::DIRECT:
                    nRead += nextDirect(pushOmniVec, offset, length, notNull, dataTypeId);
                    break;
                case orc::PATCHED_BASE:
                    nRead += nextPatchedByType(pushOmniVec, offset, length, notNull, dataTypeId);
                    break;
                case orc::DELTA:
                    nRead += nextDeltaByType(pushOmniVec, offset, length, notNull, dataTypeId);
                    break;
                default:
                    throw orc::ParseError("unknown encoding");
            }
        }

        omnivec = tempOmnivec.release();
    }

    uint64_t OmniRleDecoderV2::nextDirect(omniruntime::vec::BaseVector*& OmniVec, 
                                      uint64_t offset, uint64_t numValues, const char* const notNull,
                                      omniruntime::type::DataTypeId dataTypeId) {
        if (runRead == runLength) {
            // extract the number of fixed bits
            unsigned char fbo = (firstByte >> 1) & 0x1f;
            uint32_t bitSize = orc::decodeBitWidth(fbo);

            // extract the run length
            runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
            runLength |= readByte();
            // runs are one off
            runLength += 1;
            runRead = 0;

            readLongs(literals.data(), 0, runLength, bitSize);

            if (isSigned) {
                for (uint64_t i = 0; i < runLength; ++i) {
                    literals[i] = orc::unZigZag(static_cast<uint64_t>(literals[i]));
                }
            }
        }

        return copyDataFromBufferByType(OmniVec, offset, numValues, notNull, dataTypeId);
    }

    uint64_t OmniRleDecoderV2::nextShortRepeatsByType(omniruntime::vec::BaseVector*& OmniVec,
                                          uint64_t offset, uint64_t numValues, const char* const notNull,
                                          omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                return nextShortRepeats<omniruntime::type::OMNI_BOOLEAN>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_SHORT:
                return nextShortRepeats<omniruntime::type::OMNI_SHORT>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_INT:
                return nextShortRepeats<omniruntime::type::OMNI_INT>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_LONG:
                return nextShortRepeatsLongType<omniruntime::type::OMNI_LONG>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_DATE32:
                return nextShortRepeats<omniruntime::type::OMNI_DATE32>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_DATE64:
                return nextShortRepeatsLongType<omniruntime::type::OMNI_DATE64>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_DOUBLE:
                return nextShortRepeats<omniruntime::type::OMNI_DOUBLE>
                        (OmniVec, offset, numValues, notNull);
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("nextShortRepeats_type CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("nextShortRepeats_type VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("nextShortRepeats_type DECIMAL64 should not in here!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("nextShortRepeats_type DECIMAL128 should not in here!!!");
            default:
                printf("nextShortRepeats_type switch no process!!!");
        }

        return 0;
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    uint64_t OmniRleDecoderV2::nextShortRepeats(omniruntime::vec::BaseVector*& OmniVec,
                                          uint64_t offset, uint64_t numValues, const char* const notNull) {
        using namespace omniruntime::type;
        using T = typename NativeType<TYPE_ID>::type;
        auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(OmniVec);

        if (runRead == runLength) {
            // extract the number of fixed bytes
            uint64_t byteSize = (firstByte >> 3) & 0x07;
            byteSize += 1;

            runLength = firstByte & 0x07;
            // run lengths values are stored only after MIN_REPEAT value is met
            runLength += MIN_REPEAT;
            runRead = 0;

            // read the repeated value which is store using fixed bytes
            literals[0] = readLongBE(byteSize);

            if (isSigned) {
                literals[0] = orc::unZigZag(static_cast<uint64_t>(literals[0]));
            }
        }

        uint64_t nRead = std::min(runLength - runRead, numValues);

        if (notNull) {
            for(uint64_t pos = offset; pos < offset + nRead; ++pos) {
                if (notNull[pos]) {
                    vec->SetValue(static_cast<int>(pos), static_cast<T>(literals[0]));
                    ++runRead;
                } else {
                    vec->SetNull(static_cast<int>(pos));
                }
            }
        } else {
            for(uint64_t pos = offset; pos < offset + nRead; ++pos) {
                vec->SetValue(static_cast<int>(pos), static_cast<T>(literals[0]));
                ++runRead;
            }
        }

        return nRead;
    }

	template <omniruntime::type::DataTypeId TYPE_ID>
	uint64_t OmniRleDecoderV2::nextShortRepeatsLongType(omniruntime::vec::BaseVector*& OmniVec,
										  uint64_t offset, uint64_t numValues, const char* const notNull) {
		using namespace omniruntime::type;
		using T = typename NativeType<TYPE_ID>::type;
		auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(OmniVec);

		if (runRead == runLength) {
			// extract the number of fixed bytes
			uint64_t byteSize = (firstByte >> 3) & 0x07;
			byteSize += 1;

			runLength = firstByte & 0x07;
			// run lengths values are stored only after MIN_REPEAT value is met
			runLength += MIN_REPEAT;
			runRead = 0;

			// read the repeated value which is store using fixed bytes
			literals[0] = readLongBE(byteSize);

			if (isSigned) {
				literals[0] = orc::unZigZag(static_cast<uint64_t>(literals[0]));
			}
		}

		uint64_t nRead = std::min(runLength - runRead, numValues);

		if (notNull) {
			for(uint64_t pos = offset; pos < offset + nRead; ++pos) {
				if (notNull[pos]) {
					vec->SetValue(pos, static_cast<T>(literals[0]));
					++runRead;
				} else {
					vec->SetNull(pos);
				}
			}
		} else {
			int64_t values[nRead];
			std::fill(values, values + nRead, literals[0]);
			vec->SetValues(offset, values, nRead);
			runRead += nRead;
		}

		return nRead;
	}


    uint64_t OmniRleDecoderV2::nextPatchedByType(omniruntime::vec::BaseVector*& OmniVec,
                                          uint64_t offset, uint64_t numValues, const char* const notNull,
                                          omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                return nextPatched<omniruntime::type::OMNI_BOOLEAN>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_SHORT:
                return nextPatched<omniruntime::type::OMNI_SHORT>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_INT:
                return nextPatched<omniruntime::type::OMNI_INT>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_LONG:
                return nextPatched<omniruntime::type::OMNI_LONG>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DATE32:
                return nextPatched<omniruntime::type::OMNI_DATE32>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DATE64:
                return nextPatched<omniruntime::type::OMNI_DATE64>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DOUBLE:
                return nextPatched<omniruntime::type::OMNI_DOUBLE>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("nextPatched_type CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("nextPatched_type VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("nextPatched_type DECIMAL64 should not in here!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("nextPatched_type DECIMAL128 should not in here!!!");
            default:
                printf("nextPatched_type switch no process!!!");
        }

        return 0;
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    uint64_t OmniRleDecoderV2::nextPatched(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                           uint64_t numValues, const char* const notNull,
                                           omniruntime::type::DataTypeId dataTypeId) {
        if (runRead == runLength) {
            // extract the number of fixed bits
            unsigned char fbo = (firstByte >> 1) & 0x1f;
            uint32_t bitSize = orc::decodeBitWidth(fbo);

            // extract the run length
            runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
            runLength |= readByte();
            // runs are one off
            runLength += 1;
            runRead = 0;

            // extract the number of bytes occupied by base
            uint64_t thirdByte = readByte();
            uint64_t byteSize = (thirdByte >> 5) & 0x07;
            // base width is one off
            byteSize += 1;

            // extract patch width
            uint32_t pwo = thirdByte & 0x1f;
            uint32_t patchBitSize = orc::decodeBitWidth(pwo);

            // read fourth byte and extract patch gap width
            uint64_t fourthByte = readByte();
            uint32_t pgw = (fourthByte >> 5) & 0x07;
            // patch gap width is one off
            pgw += 1;

            // extract the length of the patch list
            size_t pl = fourthByte & 0x1f;
            if (pl == 0) {
                throw orc::ParseError("Corrupt PATCHED_BASE encoded data (pl==0)!");
            }

            // read the next base width number of bytes to extract base value
            int64_t base = readLongBE(byteSize);
            int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
            // if mask of base value is 1 then base is negative value else positive
            if ((base & mask) != 0) {
                base = base & ~mask;
                base = -base;
            }

            readLongs(literals.data(), 0, runLength, bitSize);
            // any remaining bits are thrown out
            resetReadLongs();

            // TODO: something more efficient than resize
            unpackedPatch.resize(pl);
            // TODO: Skip corrupt?
            //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
            if ((patchBitSize + pgw) > 64) {
                throw orc::ParseError("Corrupt PATCHED_BASE encoded data "
                                 "(patchBitSize + pgw > 64)!");
            }
            uint32_t cfb = orc::getClosestFixedBits(patchBitSize + pgw);
            readLongs(unpackedPatch.data(), 0, pl, cfb);
            // any remaining bits are thrown out
            resetReadLongs();

            // apply the patch directly when decoding the packed data
            int64_t patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

            int64_t gap = 0;
            int64_t patch = 0;
            uint64_t patchIdx = 0;
            adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch, &patchIdx);

            for (uint64_t i = 0; i < runLength; ++i) {
                if (static_cast<int64_t>(i) != gap) {
                    // no patching required. add base to unpacked value to get final value
                    literals[i] += base;
                } else {
                    // extract the patch value
                    int64_t patchedVal = literals[i] | (patch << bitSize);

                    // add base to patched value
                    literals[i] = base + patchedVal;

                    // increment the patch to point to next entry in patch list
                    ++patchIdx;

                    if (patchIdx < unpackedPatch.size()) {
                        adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch,
                                        &patchIdx);

                        // next gap is relative to the current gap
                        gap += i;
                    }
                }
            }
        }

        return copyDataFromBufferByType(OmniVec, offset, numValues, notNull, dataTypeId);
    }

    uint64_t OmniRleDecoderV2::nextDeltaByType(omniruntime::vec::BaseVector*& OmniVec,
                                         uint64_t offset, uint64_t numValues, const char* const notNull,
                                         omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                return nextDelta<omniruntime::type::OMNI_BOOLEAN>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_SHORT:
                return nextDelta<omniruntime::type::OMNI_SHORT>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_INT:
                return nextDelta<omniruntime::type::OMNI_INT>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_LONG:
                return nextDelta<omniruntime::type::OMNI_LONG>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DATE32:
                return nextDelta<omniruntime::type::OMNI_DATE32>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DATE64:
                return nextDelta<omniruntime::type::OMNI_DATE64>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_DOUBLE:
                return nextDelta<omniruntime::type::OMNI_DOUBLE>
                        (OmniVec, offset, numValues, notNull, dataTypeId);
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("nextShortRepeats_type CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("nextShortRepeats_type VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("nextShortRepeats_type DECIMAL64 should not in here!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("nextShortRepeats_type DECIMAL128 should not in here!!!");
            default:
                printf("nextShortRepeats_type switch no process!!!");
        }

        return 0;
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    uint64_t OmniRleDecoderV2::nextDelta(omniruntime::vec::BaseVector*& OmniVec,
                                                uint64_t offset, uint64_t numValues, const char* const notNull,
                                                omniruntime::type::DataTypeId dataTypeId) {
        if (runRead == runLength) {
            // extract the number of fixed bits
            unsigned char fbo = (firstByte >> 1) &0x1f;
            uint32_t bitSize;
            if (fbo != 0) {
                bitSize = orc::decodeBitWidth(fbo);
            } else {
                bitSize = 0;
            }

            // extract the run length
            runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
            runLength |= readByte();
            ++runLength; // account for first value
            runRead = 0;

            int64_t prevValue;
            // read the first value stored as vint
            if (isSigned) {
                prevValue = readVslong();
            } else {
                prevValue = static_cast<int64_t>(readVulong());
            }

            literals[0] = prevValue;

            // read the fixed delta value stored as vint (deltas can be negative even
            // if all number are positive)
            int64_t deltaBase = readVslong();

            if (bitSize == 0) {
                // add fixed deltas to adjacent values
                for (uint64_t i = 1; i < runLength; ++i) {
                    literals[i] = literals[i - 1] + deltaBase;
                }
            } else {
                prevValue = literals[1] = prevValue + deltaBase;
                if (runLength < 2) {
                    std::stringstream ss;
                    ss << "Illegal run length for delta encoding: " << runLength;
                    throw orc::ParseError(ss.str());
                }
                // write the unpacked values, add it to previous value and store final
                // value to result buffer. if the delta base value is negative then it
                // is a decreasing sequence else an increasing sequence.
                // read deltas using the literals buffer.
                readLongs(literals.data(), 2, runLength - 2, bitSize);

                if (deltaBase < 0) {
                    for (uint64_t i = 2; i < runLength; ++i) {
                        prevValue = literals[i] = prevValue - literals[i];
                    }
                } else {
                    for (uint64_t i = 2; i < runLength; ++i) {
                        prevValue = literals[i] = prevValue + literals[i];
                    }
                }
            }
        }

        return copyDataFromBufferByType(OmniVec, offset, numValues, notNull, dataTypeId);
    }

    void OmniRleDecoderV2::readLongs(int64_t *data, uint64_t offset, uint64_t len, uint64_t fbs) {
        switch (fbs) {
            case 4:
                return unrolledUnpack4(data, offset, len);
            case 8:
                return unrolledUnpack8(data, offset, len);
            case 16:
                return unrolledUnpack16(data, offset, len);
            case 24:
                return unrolledUnpack24(data, offset, len);
            case 32:
                return unrolledUnpack32(data, offset, len);
            case 40:
                return unrolledUnpack40(data, offset, len);
            case 48:
                return unrolledUnpack48(data, offset, len);
            case 56:
                return unrolledUnpack56(data, offset, len);
            case 64:
                return unrolledUnpack64(data, offset, len);
            default:
                // Fallback to the default implementation for deprecated bit size.
                return plainUnpackLongs(data, offset, len, fbs);
        }
    }

    void OmniRleDecoderV2::plainUnpackLongs(int64_t *data, uint64_t offset, uint64_t len, uint64_t fbs) {
        for (uint64_t i = offset; i < (offset + len); i++) {
            uint64_t result = 0;
            uint64_t bitsLeftToRead = fbs;
            while (bitsLeftToRead > bitsLeft) {
                result <<= bitsLeft;
                result |= curByte & ((1 << bitsLeft) - 1);
                bitsLeftToRead -= bitsLeft;
                curByte = readByte();
                bitsLeft = 8;
            }

            // handle the left over bits
            if (bitsLeftToRead > 0) {
                result <<= bitsLeftToRead;
                bitsLeft -= static_cast<uint32_t>(bitsLeftToRead);
                result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
            }

            data[i] = static_cast<int64_t>(result);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack64(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 8;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint64_t b0, b1, b2, b3, b4, b5, b6, b7;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                b3 = static_cast<uint32_t>(*(buffer + 3));
                b4 = static_cast<uint32_t>(*(buffer + 4));
                b5 = static_cast<uint32_t>(*(buffer + 5));
                b6 = static_cast<uint32_t>(*(buffer + 6));
                b7 = static_cast<uint32_t>(*(buffer + 7));
                buffer += 8;
                data[curIdx++] = ((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();
            b3 = readByte();
            b4 = readByte();
            b5 = readByte();
            b6 = readByte();
            b7 = readByte();
            data[curIdx++] = ((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack56(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 7;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint64_t b0, b1, b2, b3, b4, b5, b6;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                b3 = static_cast<uint32_t>(*(buffer + 3));
                b4 = static_cast<uint32_t>(*(buffer + 4));
                b5 = static_cast<uint32_t>(*(buffer + 5));
                b6 = static_cast<uint32_t>(*(buffer + 6));
                buffer += 7;
                data[curIdx++] = ((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) | (b4 << 16) | (b5 << 8) | b6);
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();
            b3 = readByte();
            b4 = readByte();
            b5 = readByte();
            b6 = readByte();
            data[curIdx++] = ((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) | (b4 << 16) | (b5 << 8) | b6);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack48(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 6;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint64_t b0, b1, b2, b3, b4, b5;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                b3 = static_cast<uint32_t>(*(buffer + 3));
                b4 = static_cast<uint32_t>(*(buffer + 4));
                b5 = static_cast<uint32_t>(*(buffer + 5));
                buffer += 6;
                data[curIdx++] = ((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5);
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();
            b3 = readByte();
            b4 = readByte();
            b5 = readByte();
            data[curIdx++] = ((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack40(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 5;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint64_t b0, b1, b2, b3, b4;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                b3 = static_cast<uint32_t>(*(buffer + 3));
                b4 = static_cast<uint32_t>(*(buffer + 4));
                buffer += 5;
                data[curIdx++] = ((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();
            b3 = readByte();
            b4 = readByte();
            data[curIdx++] = ((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack32(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 4;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint32_t b0, b1, b2, b3;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                b3 = static_cast<uint32_t>(*(buffer + 3));
                buffer += 4;
                data[curIdx++] = ((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();
            b3 = readByte();

            data[curIdx++] = ((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack24(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 3;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint32_t b0, b1, b2;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint32_t>(*buffer);
                b1 = static_cast<uint32_t>(*(buffer + 1));
                b2 = static_cast<uint32_t>(*(buffer + 2));
                buffer += 3;
                data[curIdx++] = ((b0 << 16) | (b1 << 8) | b2);
            }
            bufferStart += bufferNum * 3;
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();
            b2 = readByte();

            data[curIdx++] = ((b0 << 16) | (b1 << 8) | b2);
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack16(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = (bufferEnd - bufferStart) / 2;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            uint16_t b0, b1;
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                b0 = static_cast<uint16_t>(*buffer);
                b1 = static_cast<uint16_t>(*(buffer + 1));
                buffer += 2;
                data[curIdx++] = (b0 << 8) | b1;
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
            b0 = readByte();
            b1 = readByte();

            data[curIdx++] = (b0 << 8) | b1;
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack8(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Exhaust the buffer
            int64_t bufferNum = bufferEnd - bufferStart;
            bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
            // Avoid updating 'bufferStart' inside the loop.
            const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            for (int i = 0; i < bufferNum; ++i) {
                data[curIdx++] = *buffer++;
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;
            // readByte() will update 'bufferStart' and 'bufferEnd'.
            data[curIdx++] = readByte();
        }

        return;
    }

    void OmniRleDecoderV2::unrolledUnpack4(int64_t *data, uint64_t offset, uint64_t len) {
        uint64_t curIdx = offset;
        while (curIdx < offset + len) {
            // Make sure bitsLeft is 0 before the loop. bitsLeft can only be 0, 4, or 8.
            while (bitsLeft > 0 && curIdx < offset + len) {
                bitsLeft -= 4;
                data[curIdx++] = (curByte >> bitsLeft) & 15;
            }
            if (curIdx == offset + len) return;

            // Exhaust the buffer
            uint64_t numGroups = (offset + len - curIdx) / 2;
            numGroups = std::min(numGroups, static_cast<uint64_t>(bufferEnd - bufferStart));
            // Avoid updating 'bufferStart' inside the loop.
            const auto *buffer = reinterpret_cast<const unsigned char*>(bufferStart);
            uint32_t localByte;
            for (uint64_t i = 0; i < numGroups; ++i) {
                localByte = *buffer++;
                data[curIdx] = (localByte >> 4) & 15;
                data[curIdx + 1] = localByte & 15;
                curIdx += 2;
            }
            bufferStart = reinterpret_cast<const char*>(buffer);
            if (curIdx == offset + len) return;

            // readByte() will update 'bufferStart' and 'bufferEnd'
            curByte = readByte();
            bitsLeft = 8;
        }

        return;
    }

    uint64_t OmniRleDecoderV2::copyDataFromBufferByType(omniruntime::vec::BaseVector*& tempOmnivec,  uint64_t offset,
                                                   uint64_t numValues, const char* notNull,
                                                   omniruntime::type::DataTypeId dataTypeId) {
        switch (dataTypeId) {
            case omniruntime::type::OMNI_BOOLEAN:
                return copyDataFromBuffer<omniruntime::type::OMNI_BOOLEAN>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_SHORT:
                return copyDataFromBuffer<omniruntime::type::OMNI_SHORT>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_INT:
                return copyDataFromBuffer<omniruntime::type::OMNI_INT>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_LONG:
                return copyDataFromBufferTo64bit<omniruntime::type::OMNI_LONG>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_DATE32:
                return copyDataFromBuffer<omniruntime::type::OMNI_DATE32>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_DATE64:
                return copyDataFromBufferTo64bit<omniruntime::type::OMNI_DATE64>(tempOmnivec, offset, numValues,
                	   	   notNull);
            case omniruntime::type::OMNI_DOUBLE:
                return copyDataFromBuffer<omniruntime::type::OMNI_DOUBLE>(tempOmnivec, offset, numValues, notNull);
            case omniruntime::type::OMNI_CHAR:
                throw std::runtime_error("copyDataFromBuffer_type CHAR not finished!!!");
            case omniruntime::type::OMNI_VARCHAR:
                throw std::runtime_error("copyDataFromBuffer_type VARCHAR not finished!!!");
            case omniruntime::type::OMNI_DECIMAL64:
                throw std::runtime_error("copyDataFromBuffer_type DECIMAL64 should not in here!!!");
            case omniruntime::type::OMNI_DECIMAL128:
                throw std::runtime_error("copyDataFromBuffer_type DECIMAL128 should not in here!!!");
            default:
                printf("copyDataFromBuffer switch no process!!!");
        }

        return 0;
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    uint64_t OmniRleDecoderV2::copyDataFromBuffer(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                               uint64_t numValues, const char* notNull) {
        using namespace omniruntime::type;
        using T = typename NativeType<TYPE_ID>::type;
        auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(OmniVec);
        uint64_t nRead = std::min(runLength - runRead, numValues);
        if (notNull) {
            for (uint64_t i = offset; i < (offset + nRead); ++i) {
                if (notNull[i]) {
                    vec->SetValue(static_cast<int>(i), static_cast<T>(literals[runRead++]));
                } else {
                    vec->SetNull(static_cast<int>(i));
                }
            }
        } else {
            for (uint64_t i = offset; i < (offset + nRead); ++i) {
                vec->SetValue(static_cast<int>(i), static_cast<T>(literals[runRead++]));
            }
        }
        return nRead;
    }

    template <omniruntime::type::DataTypeId TYPE_ID>
    uint64_t OmniRleDecoderV2::copyDataFromBufferTo64bit(omniruntime::vec::BaseVector*& OmniVec, uint64_t offset,
                                              uint64_t numValues, const char* notNull) {
        using namespace omniruntime::type;
        using T = typename NativeType<TYPE_ID>::type;
        auto vec = reinterpret_cast<omniruntime::vec::Vector<T>*>(OmniVec);
        uint64_t nRead = std::min(runLength - runRead, numValues);
        if (notNull) {
            for (uint64_t i = offset; i < (offset + nRead); ++i) {
                if (notNull[i]) {
                    vec->SetValue(static_cast<int>(i), static_cast<T>(literals[runRead++]));
                } else {
                    vec->SetNull(static_cast<int>(i));
                }
            }
        } else {
            vec->SetValues(static_cast<int>(offset), literals.data() + runRead, static_cast<int>(nRead));
            runRead += nRead;
        }
        return nRead;
    }
}