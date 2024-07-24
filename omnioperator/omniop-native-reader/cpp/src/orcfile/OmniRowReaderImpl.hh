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

#ifndef OMNI_ROW_READER_IMPL_HH
#define OMNI_ROW_READER_IMPL_HH

#include "orc/RowReader/Reader.hh"
#include <vector/vector_common.h>

namespace omniruntime::reader {
    class OmniReaderImpl : public orc::ReaderImpl {
    public:
    OmniReaderImpl(std::shared_ptr<orc::FileContents> _contents, const orc::ReaderOptions& opts,
                   uint64_t fileLength, uint64_t postscriptLength)
                   : orc::ReaderImpl(_contents, opts, _fileLength, _postscriptLength) {}

    std::unique_ptr<orc::RowReader> createRowReader() const override;

    std::unique_ptr<orc::RowReader> createRowReader(const orc::RowReaderOptions& options) const override;
    };

    class OmniRowReaderImpl : public orc::RowReaderImpl {
    public:
        OmniRowReaderImpl(std::shared_ptr<orc::FileContents> contents, const orc::RowReaderOptions& options) : orc::RowReaderImpl(contents, options) {}
        /**
        * direct read VectorBatch in next
        * @param omniVecBatch the VectorBatch to push
        * @param omniTypeId the omniTypeId to push
        */
        bool next(omniruntime::vec::VectorBatch*& omniVecBatch, int* omniTypeId, bool is_row, std::shared_ptr<void>& rVec,
            std::vector<int32_t>& rowVecColumns);

        void startNextStripes() override;
    };

    std::unique_ptr<orc::Reader> omniCreateReader(std::unique_ptr<orc::InputStream> stream,
                                            const orc::ReaderOptions& options);
}

#endif