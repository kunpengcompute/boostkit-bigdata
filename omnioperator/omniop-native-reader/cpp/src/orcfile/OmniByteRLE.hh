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
        void nextNulls(std::shared_ptr<omniruntime::vec::NullsHelper> data, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        /**
        * Read a number of values into the batch by nulls.
        */
        void next(omniruntime::vec::BaseVector *omnivec, uint64_t numValues, 
            std::shared_ptr<omniruntime::vec::NullsHelper> nulls, int omniTypeId);

        template <typename T>
        void next(T *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

        void next(char *data, uint64_t numValues, std::shared_ptr<omniruntime::vec::NullsHelper> nulls);

    protected:
        size_t remainingBits;
        char lastByte;
    };
}
#endif
