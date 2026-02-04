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

#include "OmniColumnWriter.hh"
#include "ByteRLE.hh"
#include "RLE.hh"
#include "Statistics.hh"
#include "Timezone.hh"
#include "orc/Int128.hh"
#include "orc/Writer.hh"
#include "OmniRLE.hh"


namespace omniruntime::writer {

  orc::proto::ColumnEncoding_Kind RleVersionMapper(orc::RleVersion rleVersion)
  {
    switch (rleVersion)
    {
      case orc::RleVersion_1:
        return orc::proto::ColumnEncoding_Kind_DIRECT;
      case orc::RleVersion_2:
        return orc::proto::ColumnEncoding_Kind_DIRECT_V2;
      default:
        throw orc::InvalidArgument("Invalid param");
    }
  }

  OmniColumnWriter::OmniColumnWriter(
                             const orc::Type& type,
                             const orc::StreamsFactory& factory,
                             const orc::WriterOptions& options) :
                                columnId(type.getColumnId()),
                                colIndexStatistics(),
                                colStripeStatistics(),
                                colFileStatistics(),
                                enableIndex(options.getEnableIndex()),
                                rowIndex(),
                                rowIndexEntry(),
                                rowIndexPosition(),
                                enableBloomFilter(false),
                                memPool(*options.getMemoryPool()),
                                indexStream(),
                                bloomFilterStream() {

    std::unique_ptr<orc::BufferedOutputStream> presentStream =
        factory.createStream(orc::proto::Stream_Kind_PRESENT);
    notNullEncoder = createOmniBooleanRleEncoder(std::move(presentStream));

    colIndexStatistics = orc::createColumnStatistics(type);
    colStripeStatistics = orc::createColumnStatistics(type);
    colFileStatistics = orc::createColumnStatistics(type);

    if (enableIndex) {
      rowIndex = std::unique_ptr<orc::proto::RowIndex>(new orc::proto::RowIndex());
      rowIndexEntry =
        std::unique_ptr<orc::proto::RowIndexEntry>(new orc::proto::RowIndexEntry());
      rowIndexPosition = std::unique_ptr<orc::RowIndexPositionRecorder>(
                     new orc::RowIndexPositionRecorder(*rowIndexEntry));
      indexStream =
        factory.createStream(orc::proto::Stream_Kind_ROW_INDEX);

      // BloomFilters for non-UTF8 strings and non-UTC timestamps are not supported
      if (options.isColumnUseBloomFilter(columnId)
          && options.getBloomFilterVersion() == orc::BloomFilterVersion::UTF8) {
        enableBloomFilter = true;
        bloomFilter.reset(new orc::BloomFilterImpl(
          options.getRowIndexStride(), options.getBloomFilterFPP()));
        bloomFilterIndex.reset(new orc::proto::BloomFilterIndex());
        bloomFilterStream = factory.createStream(orc::proto::Stream_Kind_BLOOM_FILTER_UTF8);
      }
    }
  }

  OmniColumnWriter::~OmniColumnWriter() {
    // PASS
  }

  void OmniColumnWriter::addNulls(omniruntime::vec::NullsBuffer *nullsBuffer,
    omniruntime::vec::NullsBuffer *pNullsBuffer,
    uint64_t offset,
    uint64_t numValues) {
        static_cast<OmniBooleanRleEncoder*>(notNullEncoder.get())->add(nullsBuffer, pNullsBuffer, offset, numValues);
    }



  void OmniColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
    orc::proto::Stream stream;
    stream.set_kind(orc::proto::Stream_Kind_PRESENT);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(notNullEncoder->flush());
    streams.push_back(stream);
  }

  uint64_t OmniColumnWriter::getEstimatedSize() const {
    return notNullEncoder->getBufferSize();
  }

  void OmniColumnWriter::getStripeStatistics(
    std::vector<orc::proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colStripeStatistics.get());
  }

  void OmniColumnWriter::mergeStripeStatsIntoFileStats() {
    colFileStatistics->merge(*colStripeStatistics);
    colStripeStatistics->reset();
  }

  void OmniColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    colStripeStatistics->merge(*colIndexStatistics);
    colIndexStatistics->reset();
  }

  void OmniColumnWriter::getFileStatistics(
    std::vector<orc::proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colFileStatistics.get());
  }

  void OmniColumnWriter::createRowIndexEntry() {
    orc::proto::ColumnStatistics *indexStats = rowIndexEntry->mutable_statistics();
    colIndexStatistics->toProtoBuf(*indexStats);

    *rowIndex->add_entry() = *rowIndexEntry;

    rowIndexEntry->clear_positions();
    rowIndexEntry->clear_statistics();

    colStripeStatistics->merge(*colIndexStatistics);
    colIndexStatistics->reset();

    addBloomFilterEntry();

    recordPosition();
  }

  void OmniColumnWriter::addBloomFilterEntry() {
    if (enableBloomFilter) {
      orc::BloomFilterUTF8Utils::serialize(*bloomFilter, *bloomFilterIndex->add_bloomfilter());
      bloomFilter->reset();
    }
  }

  void OmniColumnWriter::writeIndex(std::vector<orc::proto::Stream> &streams) const {
    // write row index to output stream
    rowIndex->SerializeToZeroCopyStream(indexStream.get());

    // construct row index stream
    orc::proto::Stream stream;
    stream.set_kind(orc::proto::Stream_Kind_ROW_INDEX);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(indexStream->flush());
    streams.push_back(stream);

    // write BLOOM_FILTER_UTF8 stream
    if (enableBloomFilter) {
      if (!bloomFilterIndex->SerializeToZeroCopyStream(bloomFilterStream.get())) {
        throw std::logic_error("Failed to write bloom filter stream.");
      }
      stream.set_kind(orc::proto::Stream_Kind_BLOOM_FILTER_UTF8);
      stream.set_column(static_cast<uint32_t>(columnId));
      stream.set_length(bloomFilterStream->flush());
      streams.push_back(stream);
    }
  }

  void OmniColumnWriter::recordPosition() const {
    notNullEncoder->recordPosition(rowIndexPosition.get());
  }

  void OmniColumnWriter::reset() {
    if (enableIndex) {
      // clear row index
      rowIndex->clear_entry();
      rowIndexEntry->clear_positions();
      rowIndexEntry->clear_statistics();

      // write current positions
      recordPosition();
    }

    if (enableBloomFilter) {
      bloomFilter->reset();
      bloomFilterIndex->clear_bloomfilter();
    }
  }

  void OmniColumnWriter::writeDictionary() {
    // PASS
  }

    /**
   * @brief ORC StructColumnWriter handles nested STRUCT type data with multiple child columns.
   *        Recursively writes child columns using appropriate writers and manages struct-level metadata.
   *        Supports null value handling and statistics collection for complex nested structures.
   */
  class OmniStructColumnWriter : public OmniColumnWriter {
  public:
    OmniStructColumnWriter(
                       const orc::Type& type,
                       const orc::StreamsFactory& factory,
                       const orc::WriterOptions& options);

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
        omniruntime::vec::NullsBuffer *pNullsBuffer,
        uint64_t offset,
        uint64_t numValues) override;

    virtual void flush(std::vector<orc::proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;
    virtual void getColumnEncoding(
      std::vector<orc::proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(
      std::vector<orc::proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(
      std::vector<orc::proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(
      std::vector<orc::proto::Stream> &streams) const override;

    virtual void writeDictionary() override;

    virtual void reset() override;

  private:
    std::vector<std::unique_ptr<OmniColumnWriter>> children;
  };

  OmniStructColumnWriter::OmniStructColumnWriter(
                                         const orc::Type& type,
                                         const orc::StreamsFactory& factory,
                                         const orc::WriterOptions& options) :
                                         OmniColumnWriter(type, factory, options) {
    for(unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
      const orc::Type& child = *type.getSubtype(i);
      children.push_back(buildOmniWriter(child, factory, options));
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  void OmniStructColumnWriter::add(
                               omniruntime::vec::BaseVector *rowBatch,
                               omniruntime::vec::NullsBuffer *pNullsBuffer,
                               uint64_t offset,
                               uint64_t numValues) {
       omniruntime::vec::RowVector *rowVec = dynamic_cast<omniruntime::vec::RowVector *>(rowBatch);
       if (rowVec == nullptr) {
        throw orc::InvalidArgument("Failed to cast to RowVector");
       }
       omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
       OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);
       std::vector<omniruntime::vec::BaseVector *>childrenVec = rowVec->GetRawChildren();
       for (uint32_t i = 0; i < children.size(); ++i) {
        children[i]->add(childrenVec[i], curNullsBuffer, offset, numValues);
       }

       //update stats
       bool hasNull = curNullsBuffer->HasNull();
       if(hasNull) {
        uint64_t count = 0;
        for (uint64_t i = 0; i < numValues; ++i) {
            if(!curNullsBuffer->IsNull(offset + i)) {
                ++count;
            }
        }
        colIndexStatistics->increase(count);
        if (count < numValues) {
            colIndexStatistics->setHasNull(true);
        }
       } else {
        colIndexStatistics->increase(numValues);
       }
  }

  void OmniStructColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
    OmniColumnWriter::flush(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->flush(streams);
    }
  }

  void OmniStructColumnWriter::writeIndex(
                      std::vector<orc::proto::Stream> &streams) const {
    OmniColumnWriter::writeIndex(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeIndex(streams);
    }
  }

  uint64_t OmniStructColumnWriter::getEstimatedSize() const {
    uint64_t size = OmniColumnWriter::getEstimatedSize();
    for (uint32_t i = 0; i < children.size(); ++i) {
      size += children[i]->getEstimatedSize();
    }
    return size;
  }

  void OmniStructColumnWriter::getColumnEncoding(
                      std::vector<orc::proto::ColumnEncoding>& encodings) const {
    orc::proto::ColumnEncoding encoding;
    encoding.set_kind(orc::proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    encodings.push_back(encoding);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getColumnEncoding(encodings);
    }
  }

  void OmniStructColumnWriter::getStripeStatistics(
    std::vector<orc::proto::ColumnStatistics>& stats) const {
    OmniColumnWriter::getStripeStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getStripeStatistics(stats);
    }
  }

  void OmniStructColumnWriter::mergeStripeStatsIntoFileStats() {
    OmniColumnWriter::mergeStripeStatsIntoFileStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeStripeStatsIntoFileStats();
    }
  }

  void OmniStructColumnWriter::getFileStatistics(
    std::vector<orc::proto::ColumnStatistics>& stats) const {
    OmniColumnWriter::getFileStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getFileStatistics(stats);
    }
  }

  void OmniStructColumnWriter::mergeRowGroupStatsIntoStripeStats()  {
    OmniColumnWriter::mergeRowGroupStatsIntoStripeStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void OmniStructColumnWriter::createRowIndexEntry() {
    OmniColumnWriter::createRowIndexEntry();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->createRowIndexEntry();
    }
  }

  void OmniStructColumnWriter::reset() {
    OmniColumnWriter::reset();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->reset();
    }
  }

  void OmniStructColumnWriter::writeDictionary() {
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeDictionary();
    }
  }


    // ORC represents SHORT, INT, and LONG types using a unified LongColumnVector batch,
    // and all are written as int64_t in the ORC writer interface.
    // In Omni, Vector<RAW_TYPE> stores values in their native types (int16_t, int32_t, int64_t),
    // but they are cast to int64_t when passed to the ORC writing API.
    // So we use OmniLongColumnWriter / OmniIntColumnWriter / OmniShortColumnWriter to derive OmniIntegerColumnWriter
    // And override add function for each native type of them.

    class OmniIntegerColumnWriter : public OmniColumnWriter {
    public:
        OmniIntegerColumnWriter(
                const orc::Type &type,
                const orc::StreamsFactory &factory,
                const orc::WriterOptions &options);

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;

        virtual void flush(std::vector<orc::proto::Stream> &streams) override;

        virtual uint64_t getEstimatedSize() const override;

        virtual void getColumnEncoding(
                std::vector<orc::proto::ColumnEncoding> &encodings) const override;

        virtual void recordPosition() const override;

    protected:
        std::unique_ptr<OmniRleEncoder> rleEncoder;

    private:
        orc::RleVersion rleVersion;
    };

    OmniIntegerColumnWriter::OmniIntegerColumnWriter(
            const orc::Type& type,
            const orc::StreamsFactory& factory,
            const orc::WriterOptions& options) :
            OmniColumnWriter(type, factory, options),
            rleVersion(options.getRleVersion()) {
        std::unique_ptr<orc::BufferedOutputStream> dataStream =
                factory.createStream(orc::proto::Stream_Kind_DATA);
        rleEncoder = createOmniRleEncoder(
                std::move(dataStream),
                true,
                rleVersion,
                memPool,
                options.getAlignedBitpacking());

        if (enableIndex) {
            recordPosition();
        }
    }

    void OmniIntegerColumnWriter::add(
            omniruntime::vec::BaseVector* rowBatch,
            omniruntime::vec::NullsBuffer *pNullsBuffer,
            uint64_t offset,
            uint64_t numValues) { }

    void OmniIntegerColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
        OmniColumnWriter::flush(streams);

        orc::proto::Stream stream;
        stream.set_kind(orc::proto::Stream_Kind_DATA);
        stream.set_column(static_cast<uint32_t>(columnId));
        stream.set_length(rleEncoder->flush());
        streams.push_back(stream);
    }

    uint64_t OmniIntegerColumnWriter::getEstimatedSize() const {
        uint64_t size = OmniColumnWriter::getEstimatedSize();
        size += rleEncoder->getBufferSize();
        return size;
    }

    void OmniIntegerColumnWriter::getColumnEncoding(
            std::vector<orc::proto::ColumnEncoding>& encodings) const {
        orc::proto::ColumnEncoding encoding;
        encoding.set_kind(RleVersionMapper(rleVersion));
        encoding.set_dictionarysize(0);
        if (enableBloomFilter) {
            encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
        }
        encodings.push_back(encoding);
    }

    void OmniIntegerColumnWriter::recordPosition() const {
        OmniColumnWriter::recordPosition();
        rleEncoder->recordPosition(rowIndexPosition.get());
    }

    class OmniLongColumnWriter : public OmniIntegerColumnWriter {
    public:
        OmniLongColumnWriter(
                const orc::Type &type,
                const orc::StreamsFactory &factory,
                const orc::WriterOptions &options)
                : OmniIntegerColumnWriter(type, factory, options) {};

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;
    };

void OmniLongColumnWriter::add(
            omniruntime::vec::BaseVector* rowBatch,
            omniruntime::vec::NullsBuffer *pNullsBuffer,
            uint64_t offset,
            uint64_t numValues) {

        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        I64FlatVector *flatVec = nullptr;
        I64DictVector *dictVec = nullptr;
        if(dictEncoding) {
            dictVec = dynamic_cast<I64DictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<I64FlatVector *>(rowBatch);
        }

        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to LongVectorBatch");
        }

        orc::IntegerColumnStatisticsImpl *intStats =
                dynamic_cast<orc::IntegerColumnStatisticsImpl *>(colIndexStatistics.get());
        if (intStats == nullptr) {
            throw orc::InvalidArgument("ERROR : IntegerColumnStatisticsImpl failed !");
        }

        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        const int64_t *data = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;
        if (dictEncoding) {
            rleEncoder->add(dictVec, offset, numValues);
        } else {
            rleEncoder->add(data, offset, numValues, curNullsBuffer);
        }

        // update stats
        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        int64_t curVal = 0;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
            if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
                curVal = dictEncoding ? dictVec->GetValue(curPos) : data[i];
                ++count;
                if (enableBloomFilter) {
                    bloomFilter->addLong(curVal);
                }
                intStats->update(curVal, 1);
            }
        }
        intStats->increase(count);
        if (count < numValues) {
            intStats->setHasNull(true);
        }
    }

    class OmniIntColumnWriter : public OmniIntegerColumnWriter {
    public:
        OmniIntColumnWriter(
                const orc::Type &type,
                const orc::StreamsFactory &factory,
                const orc::WriterOptions &options)
                : OmniIntegerColumnWriter(type, factory, options) {};

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;
    };

   void OmniIntColumnWriter::add(
            omniruntime::vec::BaseVector* rowBatch,
            omniruntime::vec::NullsBuffer *pNullsBuffer,
            uint64_t offset,
            uint64_t numValues) {

        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        I32FlatVector *flatVec = nullptr;
        I32DictVector *dictVec = nullptr;
        if(dictEncoding) {
            dictVec = dynamic_cast<I32DictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<I32FlatVector *>(rowBatch);
        }

        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to IntegerVectorBatch");
        }

        orc::IntegerColumnStatisticsImpl *intStats =
                dynamic_cast<orc::IntegerColumnStatisticsImpl *>(colIndexStatistics.get());
        if (intStats == nullptr) {
            throw orc::InvalidArgument("ERROR : IntegerColumnStatisticsImpl failed !");
        }

        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);
        const int32_t *data = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;
        if (dictEncoding) {
            rleEncoder->add(dictVec, offset, numValues);
        } else {
            rleEncoder->add(data, offset, numValues, curNullsBuffer);
        }

        // update stats
        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        int64_t curVal = 0;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
            if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
                curVal = dictEncoding ? dictVec->GetValue(curPos) : data[i];
                ++count;
                if (enableBloomFilter) {
                    bloomFilter->addLong(curVal);
                }
                intStats->update(curVal, 1);
            }
        }
        intStats->increase(count);
        if (count < numValues) {
            intStats->setHasNull(true);
        }
    }

    class OmniShortColumnWriter : public OmniIntegerColumnWriter {
    public:
        OmniShortColumnWriter(
                const orc::Type &type,
                const orc::StreamsFactory &factory,
                const orc::WriterOptions &options)
                : OmniIntegerColumnWriter(type, factory, options) {};

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;
    };

    void OmniShortColumnWriter::add(
            omniruntime::vec::BaseVector* rowBatch,
            omniruntime::vec::NullsBuffer *pNullsBuffer,
            uint64_t offset,
            uint64_t numValues) {

        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        I16FlatVector *flatVec = nullptr;
        I16DictVector *dictVec = nullptr;
        if(dictEncoding) {
            dictVec = dynamic_cast<I16DictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<I16FlatVector *>(rowBatch);
        }

        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to ShortVectorBatch");
        }

        orc::IntegerColumnStatisticsImpl *intStats =
                dynamic_cast<orc::IntegerColumnStatisticsImpl *>(colIndexStatistics.get());
        if (intStats == nullptr) {
            throw orc::InvalidArgument("ERROR : IntegerColumnStatisticsImpl failed !");
        }

        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        const int16_t *data = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;

        if (dictEncoding) {
            rleEncoder->add(dictVec, offset, numValues);
        } else {
            rleEncoder->add(data, offset, numValues, curNullsBuffer);
        }

        // update stats
        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        int64_t curVal = 0;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
            if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
                curVal = dictEncoding ? dictVec->GetValue(curPos) : data[i];
                ++count;
                if (enableBloomFilter) {
                    bloomFilter->addLong(curVal);
                }
                intStats->update(curVal, 1);
            }
        }
        intStats->increase(count);
        if (count < numValues) {
            intStats->setHasNull(true);
        }
    }

    class OmniByteColumnWriter : public OmniColumnWriter {
    public:
        OmniByteColumnWriter(const orc::Type& type,
                             const orc::StreamsFactory& factory,
                             const orc::WriterOptions& options);

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;

        virtual void flush(std::vector<orc::proto::Stream>& streams) override;

        virtual uint64_t getEstimatedSize() const override;

        virtual void getColumnEncoding(
                std::vector<orc::proto::ColumnEncoding>& encodings) const override;

        virtual void recordPosition() const override;

    private:
        std::unique_ptr<orc::ByteRleEncoder> rleEncoder;
    };

    OmniByteColumnWriter::OmniByteColumnWriter(
            const orc::Type& type,
            const orc::StreamsFactory& factory,
            const orc::WriterOptions& options) :
        OmniColumnWriter(type, factory, options) {
        std::unique_ptr<orc::BufferedOutputStream> dataStream =
                factory.createStream(orc::proto::Stream_Kind_DATA);
        rleEncoder = createOmniByteRleEncoder(std::move(dataStream));

        if (enableIndex) {
            recordPosition();
        }
    }


    void OmniByteColumnWriter::add(omniruntime::vec::BaseVector* rowBatch,
                                   omniruntime::vec::NullsBuffer* pNullsBuffer,
                                   uint64_t offset,
                                   uint64_t numValues) {
        if (numValues == 0) return;

        orc::IntegerColumnStatisticsImpl* intStats =
                dynamic_cast<orc::IntegerColumnStatisticsImpl*>(colIndexStatistics.get());
        if (intStats == nullptr) {
            throw orc::InvalidArgument("OmniByteColumnWriter failed to cast to IntegerColumnStatisticsImpl");
        }

        const bool isDict = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        bool hasNull = curNullsBuffer && curNullsBuffer->HasNull();
        if (!hasNull) {
            curNullsBuffer = nullptr;
        }
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        OmniByteRleEncoder *byteRleEncoder = static_cast<OmniByteRleEncoder *>(rleEncoder.get());
        uint64_t count = 0;

        if (isDict) {
            auto* dictVec = dynamic_cast<ByteDictVector*>(rowBatch);
            if (dictVec == nullptr) throw orc::InvalidArgument("Failed to cast to ByteDictVector");

            byteRleEncoder->add(dictVec, offset, numValues);

            if (hasNull) {
                // 路径 1: 字典编码 + 有空值
                for (uint64_t i = 0; i < numValues; ++i) {
                    uint64_t curPos = offset + i;
                    if (!curNullsBuffer->IsNull(curPos)) {
                        int8_t curVal = dictVec->GetValue(curPos);
                        if (enableBloomFilter) bloomFilter->addLong(curVal);
                        intStats->update(static_cast<int64_t>(curVal), 1);
                        ++count;
                    }
                }
            } else {
                // 路径 2: 字典编码 + 无空值
                for (uint64_t i = 0; i < numValues; ++i) {
                    int8_t curVal = dictVec->GetValue(offset + i);
                    if (enableBloomFilter) bloomFilter->addLong(curVal);
                    intStats->update(static_cast<int64_t>(curVal), 1);
                }
                count = numValues;
            }
        } else {
            auto* flatVec = dynamic_cast<ByteFlatVector*>(rowBatch);
            if (flatVec == nullptr) throw orc::InvalidArgument("Failed to cast to ByteFlatVector");
            int8_t* data = flatVec->GetValuesBuffer() + offset;

            byteRleEncoder->add(data, offset, numValues, curNullsBuffer);

            if (hasNull) {
                // 路径 3: Flat + 有空值
                for (uint64_t i = 0; i < numValues; ++i) {
                    if (!curNullsBuffer->IsNull(offset + i)) {
                        int8_t curVal = data[i];
                        if (enableBloomFilter) bloomFilter->addLong(curVal);
                        intStats->update(static_cast<int64_t>(curVal), 1);
                        ++count;
                    }
                }
            } else {
                // 路径 4: Flat + 无空值
                for (uint64_t i = 0; i < numValues; ++i) {
                    int8_t curVal = data[i];
                    if (enableBloomFilter) bloomFilter->addLong(curVal);
                    intStats->update(static_cast<int64_t>(curVal), 1);
                }
                count = numValues;
            }
        }

        intStats->increase(count);
        if (count < numValues) {
            intStats->setHasNull(true);
        }
    }

    void OmniByteColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
        OmniColumnWriter::flush(streams);

        orc::proto::Stream stream;
        stream.set_kind(orc::proto::Stream_Kind_DATA);
        stream.set_column(static_cast<uint32_t>(columnId));
        stream.set_length(rleEncoder->flush());
        streams.push_back(stream);
    }

    uint64_t OmniByteColumnWriter::getEstimatedSize() const {
        uint64_t size = OmniColumnWriter::getEstimatedSize();
        size += rleEncoder->getBufferSize();
        return size;
    }

    void OmniByteColumnWriter::getColumnEncoding(
            std::vector<orc::proto::ColumnEncoding>& encodings) const {
        orc::proto::ColumnEncoding encoding;
        encoding.set_kind(orc::proto::ColumnEncoding_Kind_DIRECT);
        encoding.set_dictionarysize(0);
        if (enableBloomFilter) {
            encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
        }
        encodings.push_back(encoding);
    }

    void OmniByteColumnWriter::recordPosition() const {
        OmniColumnWriter::recordPosition();
        rleEncoder->recordPosition(rowIndexPosition.get());
    }

    class OmniBooleanColumnWriter : public OmniColumnWriter {
      public:
        OmniBooleanColumnWriter(const orc::Type& type,
                            const orc::StreamsFactory& factory,
                            const orc::WriterOptions& options);

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                         omniruntime::vec::NullsBuffer *pNullsBuffer,
                         uint64_t offset,
                         uint64_t numValues) override;

        virtual void flush(std::vector<orc::proto::Stream>& streams) override;

        virtual uint64_t getEstimatedSize() const override;

        virtual void getColumnEncoding(
            std::vector<orc::proto::ColumnEncoding>& encodings) const override;

        virtual void recordPosition() const override;

      private:
        std::unique_ptr<orc::ByteRleEncoder> rleEncoder;
      };

      OmniBooleanColumnWriter::OmniBooleanColumnWriter(
                               const orc::Type& type,
                               const orc::StreamsFactory& factory,
                               const orc::WriterOptions& options) :
                                   OmniColumnWriter(type, factory, options) {
        std::unique_ptr<orc::BufferedOutputStream> dataStream =
          factory.createStream(orc::proto::Stream_Kind_DATA);
        rleEncoder = createOmniBooleanRleEncoder(std::move(dataStream));

        if (enableIndex) {
          recordPosition();
        }
      }

    void OmniBooleanColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                    omniruntime::vec::NullsBuffer *pNullsBuffer,
                                    uint64_t offset,
                                    uint64_t numValues) {
        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        BoolFlatVector *flatVec = nullptr;
        BoolDictVector *dictVec = nullptr;

        if(dictEncoding) {
            dictVec = dynamic_cast<BoolDictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<BoolFlatVector *>(rowBatch);
        }

        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to BoolVectorBatch");
        }
        orc::BooleanColumnStatisticsImpl* boolStats =
                dynamic_cast<orc::BooleanColumnStatisticsImpl*>(colIndexStatistics.get());
        if (boolStats == nullptr) {
          throw orc::InvalidArgument("Failed to cast to BooleanColumnStatisticsImpl");
        }

        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        bool *data = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;

        OmniBooleanRleEncoder *boolRleEncoder = static_cast<OmniBooleanRleEncoder *>(rleEncoder.get());
        if (dictEncoding) {
            boolRleEncoder->add(dictVec, offset, numValues);
        } else {
            boolRleEncoder->add(data, offset, numValues, curNullsBuffer);
        }

        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        bool curVal = false;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
          if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
            curVal = dictEncoding ? dictVec->GetValue(curPos) : data[i];
            ++count;
            if (enableBloomFilter) {
              bloomFilter->addLong(curVal? 1 : 0);
            }
            boolStats->update(curVal, 1);
          }
        }
        boolStats->increase(count);
        if (count < numValues) {
          boolStats->setHasNull(true);
        }
    }

    void OmniBooleanColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
          OmniColumnWriter::flush(streams);

          orc::proto::Stream stream;
          stream.set_kind(orc::proto::Stream_Kind_DATA);
          stream.set_column(static_cast<uint32_t>(columnId));
          stream.set_length(rleEncoder->flush());
          streams.push_back(stream);
      }

    uint64_t OmniBooleanColumnWriter::getEstimatedSize() const {
          uint64_t size = OmniColumnWriter::getEstimatedSize();
          size += rleEncoder->getBufferSize();
          return size;
      }

    void OmniBooleanColumnWriter::getColumnEncoding(
                         std::vector<orc::proto::ColumnEncoding>& encodings) const {
          orc::proto::ColumnEncoding encoding;
          encoding.set_kind(orc::proto::ColumnEncoding_Kind_DIRECT);
          encoding.set_dictionarysize(0);
          if (enableBloomFilter) {
              encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
          }
          encodings.push_back(encoding);
      }

    void OmniBooleanColumnWriter::recordPosition() const {
          OmniColumnWriter::recordPosition();
          rleEncoder->recordPosition(rowIndexPosition.get());
      }


    class OmniFloatingPointColumnWriter : public OmniColumnWriter {
    public:
        OmniFloatingPointColumnWriter(const orc::Type& type,
                                      const orc::StreamsFactory& factory,
                                      const orc::WriterOptions& options,
                                      bool isFloatType);

        virtual void add(omniruntime::vec::BaseVector* rowBatch,
                       omniruntime::vec::NullsBuffer *pNullsBuffer,
                       uint64_t offset,
                       uint64_t numValues) override;

        virtual void flush(std::vector<orc::proto::Stream>& streams) override;

        virtual uint64_t getEstimatedSize() const override;

        virtual void getColumnEncoding(
            std::vector<orc::proto::ColumnEncoding>& encodings) const override;

        virtual void recordPosition() const override;

    protected:
        bool isFloat;
        std::unique_ptr<orc::AppendOnlyBufferedStream> dataStream;
        orc::DataBuffer<char> buffer;
    };

    OmniFloatingPointColumnWriter::OmniFloatingPointColumnWriter(
                          const orc::Type& type,
                          const orc::StreamsFactory& factory,
                          const orc::WriterOptions& options,
                          bool isFloatType) :
                              OmniColumnWriter(type, factory, options),
                              isFloat(isFloatType),
                              buffer(*options.getMemoryPool()) {
        dataStream.reset(new orc::AppendOnlyBufferedStream(factory.createStream(orc::proto::Stream_Kind_DATA)));
        buffer.resize(isFloat ? 4 : 8);

        if (enableIndex) {
          recordPosition();
        }
    }

    // Floating point types are stored using IEEE 754 floating point bit layout.
    // Float columns use 4 bytes per value and double columns use 8 bytes.
    template <typename FLOAT_TYPE, typename INTEGER_TYPE>
        inline void encodeFloatNum(FLOAT_TYPE input, char* output) {
        INTEGER_TYPE* intBits = reinterpret_cast<INTEGER_TYPE*>(&input);
        for (size_t i = 0; i < sizeof(INTEGER_TYPE); ++i) {
            output[i] = static_cast<char>(((*intBits) >> (8 * i)) & 0xff);
        }
    }

    void OmniFloatingPointColumnWriter::add(omniruntime::vec::BaseVector* rowBatch,
                                            omniruntime::vec::NullsBuffer *pNullsBuffer,
                                            uint64_t offset,
                                            uint64_t numValues) { }

    void OmniFloatingPointColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
        OmniColumnWriter::flush(streams);

        orc::proto::Stream stream;
        stream.set_kind(orc::proto::Stream_Kind_DATA);
        stream.set_column(static_cast<uint32_t>(columnId));
        stream.set_length(dataStream->flush());
        streams.push_back(stream);
    }

    uint64_t OmniFloatingPointColumnWriter::getEstimatedSize() const {
        uint64_t size = OmniColumnWriter::getEstimatedSize();
        size += dataStream->getSize();
        return size;
    }

    void OmniFloatingPointColumnWriter::getColumnEncoding(
                      std::vector<orc::proto::ColumnEncoding>& encodings) const {
        orc::proto::ColumnEncoding encoding;
        encoding.set_kind(orc::proto::ColumnEncoding_Kind_DIRECT);
        encoding.set_dictionarysize(0);
        if (enableBloomFilter) {
            encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
        }
        encodings.push_back(encoding);
    }

    void OmniFloatingPointColumnWriter::recordPosition() const {
        OmniColumnWriter::recordPosition();
        dataStream->recordPosition(rowIndexPosition.get());
    }

    class OmniDoubleColumnWriter : public OmniFloatingPointColumnWriter {
    public:
        OmniDoubleColumnWriter(const orc::Type &type,
                              const orc::StreamsFactory &factory,
                              const orc::WriterOptions &options) :
                OmniFloatingPointColumnWriter(type, factory, options, false){};

        void add(omniruntime::vec::BaseVector *rowBatch,
                 omniruntime::vec::NullsBuffer *pNullsBuffer,
                 uint64_t offset,
                 uint64_t numValues) override;
    };

    void OmniDoubleColumnWriter::add(omniruntime::vec::BaseVector* rowBatch,
                                            omniruntime::vec::NullsBuffer *pNullsBuffer,
                                            uint64_t offset,
                                            uint64_t numValues) {
        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        DoubleFlatVector *flatVec = nullptr;
        DoubleDictVector *dictVec = nullptr;
        if(dictEncoding) {
            dictVec = dynamic_cast<DoubleDictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<DoubleFlatVector *>(rowBatch);
        }
        orc::DoubleColumnStatisticsImpl* doubleStats =
                dynamic_cast<orc::DoubleColumnStatisticsImpl*>(colIndexStatistics.get());
        if (doubleStats == nullptr) {
            throw orc::InvalidArgument("Failed to cast to DoubleColumnStatisticsImpl");
        }
        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to DoubleVectorBatch");
        }
        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        const double *doubleData = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;

        char* data = buffer.data();
        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        double curVal = 0;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
            if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
                curVal = dictEncoding ? dictVec->GetValue(curPos) : doubleData[i];
                encodeFloatNum<double, int64_t>(curVal, data);
                //double columns use 8 bytes
                dataStream->write(data, 8);
                ++count;
                if (enableBloomFilter) {
                    bloomFilter->addDouble(curVal);
                }
                doubleStats->update(curVal);
            }
        }
        doubleStats->increase(count);
        if (count < numValues) {
            doubleStats->setHasNull(true);
        }
    }


    class OmniFloatColumnWriter : public OmniFloatingPointColumnWriter {
    public:
        OmniFloatColumnWriter(const orc::Type &type,
                              const orc::StreamsFactory &factory,
                              const orc::WriterOptions &options) :
                OmniFloatingPointColumnWriter(type, factory, options, true){};

        void add(omniruntime::vec::BaseVector *rowBatch,
                 omniruntime::vec::NullsBuffer *pNullsBuffer,
                 uint64_t offset,
                 uint64_t numValues) override;
    };

    void OmniFloatColumnWriter::add(omniruntime::vec::BaseVector* rowBatch,
                                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                                     uint64_t offset,
                                     uint64_t numValues) {
        bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
        FloatFlatVector *flatVec = nullptr;
        FloatDictVector *dictVec = nullptr;
        if(dictEncoding) {
            dictVec = dynamic_cast<FloatDictVector *>(rowBatch);
        } else {
            flatVec = dynamic_cast<FloatFlatVector *>(rowBatch);
        }

        if (flatVec == nullptr && dictVec == nullptr) {
            throw orc::InvalidArgument("Failed to cast to FloatVectorBatch");
        }
        orc::DoubleColumnStatisticsImpl* doubleStats =
                dynamic_cast<orc::DoubleColumnStatisticsImpl*>(colIndexStatistics.get());
        if (doubleStats == nullptr) {
            throw orc::InvalidArgument("Failed to cast to DoubleColumnStatisticsImpl");
        }

        omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
        OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

        const float *floatData = dictEncoding ? nullptr : flatVec->GetValuesBuffer() + offset;

        char* data = buffer.data();
        bool hasNull = curNullsBuffer->HasNull();
        uint64_t count = 0;
        uint64_t curPos = offset;
        float curVal = 0;
        for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
            if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
                curVal = dictEncoding ? dictVec->GetValue(curPos) : floatData[i];
                encodeFloatNum<float, int32_t>(curVal, data);
                //float columns use 4 bytes
                dataStream->write(data, 4);
                ++count;
                if (enableBloomFilter) {
                    bloomFilter->addDouble(curVal);
                }
                doubleStats->update(curVal);
            }
        }
        doubleStats->increase(count);
        if (count < numValues) {
            doubleStats->setHasNull(true);
        }
    }

class SortedStringDictionary {
  public:
    struct DictEntry {
      DictEntry(const char * str, size_t len):data(str),length(len) {}
      const char * data;
      size_t length;
    };

    SortedStringDictionary():totalLength(0) {}

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char * data, size_t len);

    // write dictionary data & length to output buffer
    void flush(orc::AppendOnlyBufferedStream * dataStream,
               orc::RleEncoder * lengthEncoder) const;

    // reorder input index buffer from insertion order to dictionary order
    void reorder(std::vector<int64_t>& idxBuffer) const;

    // get dict entries in insertion order
    void getEntriesInInsertionOrder(std::vector<const DictEntry *>&) const;

    // return count of entries
    size_t size() const;

    // return total length of strings in the dictioanry
    uint64_t length() const;

    void clear();

  private:
    struct LessThan {
      bool operator()(const DictEntry& left, const DictEntry& right) const {
        int ret = memcmp(left.data, right.data, std::min(left.length, right.length));
        if (ret != 0) {
          return ret < 0;
        }
        return left.length < right.length;
      }
    };

    std::map<DictEntry, size_t, LessThan> dict;
    std::vector<std::vector<char>> data;
    uint64_t totalLength;

    // use friend class here to avoid being bothered by const function calls
    friend class OmniStringColumnWriter;
    friend class OmniCharColumnWriter;
    friend class OmniVarCharColumnWriter;
    // store indexes of insertion order in the dictionary for not-null rows
    std::vector<int64_t> idxInDictBuffer;
  };

  // insert a new string into dictionary, return its insertion order
  size_t SortedStringDictionary::insert(const char * str, size_t len) {
    auto ret = dict.insert({DictEntry(str, len), dict.size()});
    if (ret.second) {
      // make a copy to internal storage
      data.push_back(std::vector<char>(len));
      memcpy(data.back().data(), str, len);
      // update dictionary entry to link pointer to internal storage
      DictEntry * entry = const_cast<DictEntry *>(&(ret.first->first));
      entry->data = data.back().data();
      totalLength += len;
    }
    return ret.first->second;
  }

  // write dictionary data & length to output buffer
  void SortedStringDictionary::flush(orc::AppendOnlyBufferedStream * dataStream,
                               orc::RleEncoder * lengthEncoder) const {
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      dataStream->write(it->first.data, it->first.length);
      lengthEncoder->write(static_cast<int64_t>(it->first.length));
    }
  }

  /**
   * Reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
  void SortedStringDictionary::reorder(std::vector<int64_t>& idxBuffer) const {
    // iterate the dictionary to get mapping from insertion order to value order
    std::vector<size_t> mapping(dict.size());
    size_t dictIdx = 0;
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      mapping[it->second] = dictIdx++;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i) {
      idxBuffer[i] = static_cast<int64_t>(
        mapping[static_cast<size_t>(idxBuffer[i])]);
    }
  }

  // get dict entries in insertion order
  void SortedStringDictionary::getEntriesInInsertionOrder(
                    std::vector<const DictEntry *>& entries) const {
    entries.resize(dict.size());
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      entries[it->second] = &(it->first);
    }
  }

  // return count of entries
  size_t SortedStringDictionary::size() const {
    return dict.size();
  }

  // return total length of strings in the dictioanry
  uint64_t SortedStringDictionary::length() const {
    return totalLength;
  }

  void SortedStringDictionary::clear()  {
    totalLength = 0;
    data.clear();
    dict.clear();
  }

class OmniStringColumnWriter : public OmniColumnWriter {
  public:
    OmniStringColumnWriter(const orc::Type& type,
                       const orc::StreamsFactory& factory,
                       const orc::WriterOptions& options);

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;

    virtual void flush(std::vector<orc::proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(
        std::vector<orc::proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

    virtual void createRowIndexEntry() override;

    virtual void writeDictionary() override;

    virtual void reset() override;

  private:
    /**
     * dictionary related functions
     */
    bool checkDictionaryKeyRatio();
    void createDirectStreams();
    void createDictStreams();
    void deleteDictStreams();
    void fallbackToDirectEncoding();

  protected:
    orc::RleVersion rleVersion;
    bool useCompression;
    const orc::StreamsFactory& streamsFactory;
    bool alignedBitPacking;

    // direct encoding streams
    std::unique_ptr<OmniRleEncoder> directLengthEncoder;
    std::unique_ptr<orc::AppendOnlyBufferedStream> directDataStream;

    // dictionary encoding streams
    std::unique_ptr<OmniRleEncoder> dictDataEncoder;
    std::unique_ptr<OmniRleEncoder> dictLengthEncoder;
    std::unique_ptr<orc::AppendOnlyBufferedStream> dictStream;

    /**
     * dictionary related variables
     */
    SortedStringDictionary dictionary;
    // whether or not dictionary checking is done
    bool doneDictionaryCheck;
    // whether or not it should be used
    bool useDictionary;
    // keys in the dictionary should not exceed this ratio
    double dictSizeThreshold;

    // record start row of each row group; null rows are skipped
    mutable std::vector<size_t> startOfRowGroups;
  };

  OmniStringColumnWriter::OmniStringColumnWriter(
                          const orc::Type& type,
                          const orc::StreamsFactory& factory,
                          const orc::WriterOptions& options) :
                              OmniColumnWriter(type, factory, options),
                              rleVersion(options.getRleVersion()),
                              useCompression(options.getCompression() != orc::CompressionKind_NONE),
                              streamsFactory(factory),
                              alignedBitPacking(options.getAlignedBitpacking()),
                              doneDictionaryCheck(false),
                              useDictionary(options.getEnableDictionary()),
                              dictSizeThreshold(options.getDictionaryKeySizeThreshold()){
    if (type.getKind() == orc::TypeKind::BINARY) {
      useDictionary = false;
      doneDictionaryCheck = true;
    }

    if (useDictionary) {
      createDictStreams();
    } else {
      doneDictionaryCheck = true;
      createDirectStreams();
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  void OmniStringColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                omniruntime::vec::NullsBuffer *pNullsBuffer,
                               uint64_t offset,
                               uint64_t numValues) {
    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    StringFlatVector *flatVec = nullptr;
    StringDictVector *dictVec = nullptr;
    if(dictEncoding) {
        dictVec = dynamic_cast<StringDictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<StringFlatVector *>(rowBatch);
    }
    if (flatVec == nullptr && dictVec == nullptr) {
      throw orc::InvalidArgument("Failed to cast to Vector<String>");
    }

    orc::StringColumnStatisticsImpl* strStats =
        dynamic_cast<orc::StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

    if (!useDictionary){
      if(dictEncoding) {
        directLengthEncoder->add(dictVec, offset, numValues);
      } else {
        directLengthEncoder->add(flatVec, offset, numValues);
      }
    }

    bool hasNull = curNullsBuffer->HasNull();
    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      uint64_t curPos = offset + i;
      if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
        std::string_view sv = dictEncoding ? dictVec->GetValue(curPos) : flatVec->GetValue(curPos);
        const size_t len =  sv.length();
        if (useDictionary) {
          size_t index = dictionary.insert(sv.data(), len);
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(sv.data(), len);
        }
        if (enableBloomFilter) {
          bloomFilter->addBytes(sv.data(), static_cast<int64_t>(len));
        }
        strStats->update(sv.data(), len);
        ++count;
      }
    }
    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

  void OmniStringColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
    OmniColumnWriter::flush(streams);

    if (useDictionary) {
      orc::proto::Stream data;
      data.set_kind(orc::proto::Stream_Kind_DATA);
      data.set_column(static_cast<uint32_t>(columnId));
      data.set_length(dictDataEncoder->flush());
      streams.push_back(data);

      orc::proto::Stream dict;
      dict.set_kind(orc::proto::Stream_Kind_DICTIONARY_DATA);
      dict.set_column(static_cast<uint32_t>(columnId));
      dict.set_length(dictStream->flush());
      streams.push_back(dict);

      orc::proto::Stream length;
      length.set_kind(orc::proto::Stream_Kind_LENGTH);
      length.set_column(static_cast<uint32_t>(columnId));
      length.set_length(dictLengthEncoder->flush());
      streams.push_back(length);
    } else {
      orc::proto::Stream length;
      length.set_kind(orc::proto::Stream_Kind_LENGTH);
      length.set_column(static_cast<uint32_t>(columnId));
      length.set_length(directLengthEncoder->flush());
      streams.push_back(length);

      orc::proto::Stream data;
      data.set_kind(orc::proto::Stream_Kind_DATA);
      data.set_column(static_cast<uint32_t>(columnId));
      data.set_length(directDataStream->flush());
      streams.push_back(data);
    }
  }

  uint64_t OmniStringColumnWriter::getEstimatedSize() const {
    uint64_t size = OmniColumnWriter::getEstimatedSize();
    if (!useDictionary) {
      size += directLengthEncoder->getBufferSize();
      size += directDataStream->getSize();
    } else {
      size += dictionary.length();
      size += dictionary.size() * sizeof(int32_t);
      size += dictionary.idxInDictBuffer.size() * sizeof(int32_t);
      if (useCompression) {
        size /= 3;  // estimated ratio is 3:1
      }
    }
    return size;
  }

  void OmniStringColumnWriter::getColumnEncoding(
    std::vector<orc::proto::ColumnEncoding>& encodings) const {
    orc::proto::ColumnEncoding encoding;
    if (!useDictionary) {
      encoding.set_kind(rleVersion == orc::RleVersion_1 ?
                        orc::proto::ColumnEncoding_Kind_DIRECT :
                        orc::proto::ColumnEncoding_Kind_DIRECT_V2);
    } else {
      encoding.set_kind(rleVersion == orc::RleVersion_1 ?
                        orc::proto::ColumnEncoding_Kind_DICTIONARY :
                        orc::proto::ColumnEncoding_Kind_DICTIONARY_V2);
    }
    encoding.set_dictionarysize(static_cast<uint32_t>(dictionary.size()));
    if (enableBloomFilter) {
      encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void OmniStringColumnWriter::recordPosition() const {
    OmniColumnWriter::recordPosition();
    if (!useDictionary) {
      directDataStream->recordPosition(rowIndexPosition.get());
      directLengthEncoder->recordPosition(rowIndexPosition.get());
    } else {
      if (enableIndex) {
        startOfRowGroups.push_back(dictionary.idxInDictBuffer.size());
      }
    }
  }

  bool OmniStringColumnWriter::checkDictionaryKeyRatio() {
    if (!doneDictionaryCheck) {
      useDictionary = dictionary.size() <= static_cast<size_t>(
        static_cast<double>(dictionary.idxInDictBuffer.size()) * dictSizeThreshold);
      doneDictionaryCheck = true;
    }

    return useDictionary;
  }

  void OmniStringColumnWriter::createRowIndexEntry() {
    if (useDictionary && !doneDictionaryCheck) {
      if (!checkDictionaryKeyRatio()) {
        fallbackToDirectEncoding();
      }
    }
    OmniColumnWriter::createRowIndexEntry();
  }

  void OmniStringColumnWriter::reset() {
    OmniColumnWriter::reset();

    dictionary.clear();
    dictionary.idxInDictBuffer.resize(0);
    startOfRowGroups.clear();
    startOfRowGroups.push_back(0);
  }

  void OmniStringColumnWriter::createDirectStreams() {
    std::unique_ptr<orc::BufferedOutputStream> directLengthStream =
      streamsFactory.createStream(orc::proto::Stream_Kind_LENGTH);
    directLengthEncoder = createOmniRleEncoder(std::move(directLengthStream),
                                           false,
                                           rleVersion,
                                           memPool,
                                           alignedBitPacking);
    directDataStream.reset(new orc::AppendOnlyBufferedStream(
      streamsFactory.createStream(orc::proto::Stream_Kind_DATA)));
  }

  void OmniStringColumnWriter::createDictStreams() {
    std::unique_ptr<orc::BufferedOutputStream> dictDataStream =
      streamsFactory.createStream(orc::proto::Stream_Kind_DATA);
    dictDataEncoder = createOmniRleEncoder(std::move(dictDataStream),
                                       false,
                                       rleVersion,
                                       memPool,
                                       alignedBitPacking);
    std::unique_ptr<orc::BufferedOutputStream> dictLengthStream =
      streamsFactory.createStream(orc::proto::Stream_Kind_LENGTH);
    dictLengthEncoder = createOmniRleEncoder(std::move(dictLengthStream),
                                         false,
                                         rleVersion,
                                         memPool,
                                         alignedBitPacking);
    dictStream.reset(new orc::AppendOnlyBufferedStream(
      streamsFactory.createStream(orc::proto::Stream_Kind_DICTIONARY_DATA)));
  }

  void OmniStringColumnWriter::deleteDictStreams() {
    dictDataEncoder.reset(nullptr);
    dictLengthEncoder.reset(nullptr);
    dictStream.reset(nullptr);

    dictionary.clear();
    dictionary.idxInDictBuffer.clear();
    startOfRowGroups.clear();
  }

  void OmniStringColumnWriter::writeDictionary() {
    if (useDictionary  && !doneDictionaryCheck) {
      // when index is disabled, dictionary check happens while writing 1st stripe
      if (!checkDictionaryKeyRatio()) {
        fallbackToDirectEncoding();
        return;
      }
    }

    if (useDictionary) {
      // flush dictionary data & length streams
      dictionary.flush(dictStream.get(), dictLengthEncoder.get());

      // convert index from insertion order to dictionary order
      dictionary.reorder(dictionary.idxInDictBuffer);

      // write data sequences
      int64_t * data = dictionary.idxInDictBuffer.data();
      if (enableIndex) {
        size_t prevOffset = 0;
        for (size_t i = 0; i < startOfRowGroups.size(); ++i) {
          // write sequences in batch for a row group stride
          size_t offset = startOfRowGroups[i];
          dictDataEncoder->add(data + prevOffset, offset - prevOffset, nullptr);

          // update index positions
          int rowGroupId = static_cast<int>(i);
          orc::proto::RowIndexEntry* indexEntry =
            (rowGroupId < rowIndex->entry_size()) ?
            rowIndex->mutable_entry(rowGroupId) : rowIndexEntry.get();

          // add positions for direct streams
          orc::RowIndexPositionRecorder recorder(*indexEntry);
          dictDataEncoder->recordPosition(&recorder);

          prevOffset = offset;
        }

        dictDataEncoder->add(data + prevOffset,
                             dictionary.idxInDictBuffer.size() - prevOffset,
                             nullptr);
      } else {
        dictDataEncoder->add(data, dictionary.idxInDictBuffer.size(), nullptr);
      }
    }
  }

  void OmniStringColumnWriter::fallbackToDirectEncoding() {
    createDirectStreams();

    if (enableIndex) {
      // fallback happens at the 1st row group;
      // simply complete positions for direct streams
      orc::proto::RowIndexEntry * indexEntry = rowIndexEntry.get();
      orc::RowIndexPositionRecorder recorder(*indexEntry);
      directDataStream->recordPosition(&recorder);
      directLengthEncoder->recordPosition(&recorder);
    }

    // get dictionary entries in insertion order
    std::vector<const SortedStringDictionary::DictEntry *> entries;
    dictionary.getEntriesInInsertionOrder(entries);

    // store each length of the data into a vector
    const SortedStringDictionary::DictEntry * dictEntry = nullptr;
    for (uint64_t i = 0; i != dictionary.idxInDictBuffer.size(); ++i) {
      // write one row data in direct encoding
      dictEntry = entries[static_cast<size_t>(dictionary.idxInDictBuffer[i])];
      directDataStream->write(dictEntry->data, dictEntry->length);
      directLengthEncoder->write(static_cast<int64_t>(dictEntry->length));
    }

    deleteDictStreams();
  }

struct Utf8Utils {
    /**
     * Counts how many utf-8 chars of the input data
     */
    static uint64_t charLength(const char * data, uint64_t length) {
      uint64_t chars = 0;
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
      }
      return chars;
    }

    /**
     * Return the number of bytes required to read at most maxCharLength
     * characters in full from a utf-8 encoded byte array provided
     * by data. This does not validate utf-8 data, but
     * operates correctly on already valid utf-8 data.
     *
     * @param maxCharLength number of characters required
     * @param data the bytes of UTF-8
     * @param length the length of data to truncate
     */
    static uint64_t truncateBytesTo(uint64_t maxCharLength,
                                    const char * data,
                                    uint64_t length) {
      uint64_t chars = 0;
      if (length <= maxCharLength) {
        return length;
      }
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
        if (chars > maxCharLength) {
          return i;
        }
      }
      // everything fits
      return length;
    }

    /**
     * Checks if b is the first byte of a UTF-8 character.
     */
    inline static bool isUtfStartByte(char b) {
      return (b & 0xC0) != 0x80;
    }

    /**
     * Find the start of the last character that ends in the current string.
     * @param text the bytes of the utf-8
     * @param from the first byte location
     * @param until the last byte location
     * @return the index of the last character
    */
    static uint64_t findLastCharacter(const char * text, uint64_t from, uint64_t until) {
      uint64_t posn = until;
      /* we don't expect characters more than 5 bytes */
      while (posn >= from) {
        if (isUtfStartByte(text[posn])) {
          return posn;
        }
        posn -= 1;
      }
      /* beginning of a valid char not found */
      throw std::logic_error(
        "Could not truncate string, beginning of a valid char not found");
    }
  };

class OmniCharColumnWriter : public OmniStringColumnWriter {
  public:
    OmniCharColumnWriter(const orc::Type& type,
                     const orc::StreamsFactory& factory,
                     const orc::WriterOptions& options) :
                         OmniStringColumnWriter(type, factory, options),
                         maxLength(type.getMaximumLength()),
                         padBuffer(*options.getMemoryPool()) {
      // utf-8 is currently 4 bytes long, but it could be up to 6
      padBuffer.resize(maxLength * 6);
    }

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;

  private:
    uint64_t maxLength;
    orc::DataBuffer<char> padBuffer;
  };

  void OmniCharColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                             omniruntime::vec::NullsBuffer *pNullsBuffer,
                             uint64_t offset,
                             uint64_t numValues) {
    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    StringFlatVector *flatVec = nullptr;
    StringDictVector *dictVec = nullptr;
    if(dictEncoding) {
        dictVec = dynamic_cast<StringDictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<StringFlatVector *>(rowBatch);
    }
    if (dictVec == nullptr && flatVec == nullptr) {
      throw orc::InvalidArgument("Failed to cast to CharVectorBatch");
    }

    orc::StringColumnStatisticsImpl* strStats =
        dynamic_cast<orc::StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

    std::unique_ptr<int64_t[]> length = std::make_unique<int64_t[]>(numValues);

    bool hasNull = curNullsBuffer->HasNull();
    uint64_t count = 0;
    uint64_t curPos;
    for (uint64_t i = 0; i < numValues; ++i) {
      curPos = offset + i;
      if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
        const char * charData = nullptr;
        std::string_view sv = dictEncoding? dictVec->GetValue(curPos) : flatVec->GetValue(curPos);
        uint64_t originLength = static_cast<uint64_t>(sv.length());
        uint64_t charLength = Utf8Utils::charLength(sv.data(), originLength);
        if (charLength >= maxLength) {
          charData = sv.data();
          length[i] = static_cast<int64_t>(
            Utf8Utils::truncateBytesTo(maxLength, sv.data(), originLength));
        } else {
          charData = padBuffer.data();
          // the padding is exactly 1 byte per char
          length[i] = length[i] + static_cast<int64_t>(maxLength - charLength);
          memcpy(padBuffer.data(), sv.data(), originLength);
          memset(padBuffer.data() + originLength,
                 ' ',
                 static_cast<size_t>(length[i]) - originLength);
        }

        if (useDictionary) {
          size_t index = dictionary.insert(charData, static_cast<size_t>(length[i]));
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(charData, static_cast<size_t>(length[i]));
        }

        if (enableBloomFilter) {
          bloomFilter->addBytes(sv.data(), length[i]);
        }
        strStats->update(charData, static_cast<size_t>(length[i]));
        ++count;
      }
    }

    if (!useDictionary) {
      directLengthEncoder->add(length.get(), offset, numValues, curNullsBuffer);
    }

    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

class OmniVarCharColumnWriter : public OmniStringColumnWriter {
  public:
    OmniVarCharColumnWriter(const orc::Type& type,
                        const orc::StreamsFactory& factory,
                        const orc::WriterOptions& options) :
                            OmniStringColumnWriter(type, factory, options),
                            maxLength(type.getMaximumLength()) {
      // PASS
    }

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;

  private:
    uint64_t maxLength;
  };

  void OmniVarCharColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                omniruntime::vec::NullsBuffer *pNullsBuffer,
                                uint64_t offset,
                                uint64_t numValues) {
    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    StringFlatVector *flatVec = nullptr;
    StringDictVector *dictVec = nullptr;
    if(dictEncoding) {
        dictVec = dynamic_cast<StringDictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<StringFlatVector *>(rowBatch);
    }
    if (flatVec == nullptr && dictVec == nullptr) {
      throw orc::InvalidArgument("Failed to cast to Vector<String>");
    }

    orc::StringColumnStatisticsImpl* strStats =
        dynamic_cast<orc::StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

    std::unique_ptr<int64_t[]> length = std::make_unique<int64_t[]>(numValues);

    bool hasNull = curNullsBuffer->HasNull();
    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      uint64_t curPos = offset + i;
      if (hasNull || !curNullsBuffer->IsNull(curPos)) {
        std::string_view sv = dictEncoding ? dictVec->GetValue(curPos) : flatVec->GetValue(curPos);
        uint64_t itemLength = Utf8Utils::truncateBytesTo(
          maxLength, sv.data(), static_cast<uint64_t>(sv.length()));
        length[i] = static_cast<int64_t>(itemLength);

        if (useDictionary) {
          size_t index = dictionary.insert(sv.data(), static_cast<size_t>(length[i]));
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(sv.data(), static_cast<size_t>(length[i]));
        }

        if (enableBloomFilter) {
          bloomFilter->addBytes(sv.data(), length[i]);
        }
        strStats->update(sv.data(), static_cast<size_t>(length[i]));
        ++count;
      }
    }

    if (!useDictionary) {
      directLengthEncoder->add(length.get(), offset, numValues, curNullsBuffer);
    }

    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

class OmniDecimal64ColumnWriter : public OmniColumnWriter {
  public:
    static const uint32_t MAX_PRECISION_64 = 18;
    static const uint32_t MAX_PRECISION_128 = 38;

    OmniDecimal64ColumnWriter(const orc::Type& type,
                          const orc::StreamsFactory& factory,
                          const orc::WriterOptions& options);

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;

    virtual void flush(std::vector<orc::proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(
        std::vector<orc::proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

  protected:
    orc::RleVersion rleVersion;
    uint64_t precision;
    uint64_t scale;
    std::unique_ptr<orc::AppendOnlyBufferedStream> valueStream;
    std::unique_ptr<OmniRleEncoder> scaleEncoder;

  private:
    char buffer[10];
  };

  OmniDecimal64ColumnWriter::OmniDecimal64ColumnWriter(
                             const orc::Type& type,
                             const orc::StreamsFactory& factory,
                             const orc::WriterOptions& options) :
                                 OmniColumnWriter(type, factory, options),
                                 rleVersion(options.getRleVersion()),
                                 precision(type.getPrecision()),
                                 scale(type.getScale()) {
    valueStream.reset(new orc::AppendOnlyBufferedStream(
        factory.createStream(orc::proto::Stream_Kind_DATA)));
    std::unique_ptr<orc::BufferedOutputStream> scaleStream =
        factory.createStream(orc::proto::Stream_Kind_SECONDARY);
    scaleEncoder = createOmniRleEncoder(std::move(scaleStream),
                                    true,
                                    rleVersion,
                                    memPool,
                                    options.getAlignedBitpacking());

    if (enableIndex) {
      recordPosition();
    }
  }

void OmniDecimal64ColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                  omniruntime::vec::NullsBuffer *pNullsBuffer,
                                  uint64_t offset,
                                  uint64_t numValues) {

    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    I64FlatVector *flatVec = nullptr;
    I64DictVector *dictVec = nullptr;
    if (dictEncoding) {
        dictVec = dynamic_cast<I64DictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<I64FlatVector *>(rowBatch);
    }
    if (flatVec == nullptr && dictVec == nullptr) {
      throw orc::InvalidArgument("Failed to cast to Decimal64VectorBatch");
    }

    orc::DecimalColumnStatisticsImpl* decStats =
      dynamic_cast<orc::DecimalColumnStatisticsImpl*>(colIndexStatistics.get());
    if (decStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to DecimalColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

    bool hasNull = curNullsBuffer->HasNull();
    uint64_t count = 0;
    uint64_t curPos;
    for (uint64_t i = 0; i < numValues; ++i) {
      curPos = offset + i;
      if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
        int64_t curVal = dictEncoding ? dictVec->GetValue(curPos) : flatVec->GetValue(curPos);
        int64_t val = orc::zigZag(curVal);
        char* data = buffer;
        while (true) {
          if ((val & ~0x7f) == 0) {
            *(data++) = (static_cast<char>(val));
            break;
          } else {
            *(data++) = static_cast<char>(0x80 | (val & 0x7f));
            // cast val to unsigned so as to force 0-fill right shift
            val = (static_cast<uint64_t>(val) >> 7);
          }
        }
        valueStream->write(buffer, static_cast<size_t>(data - buffer));
        ++count;
        if (enableBloomFilter) {
          std::string decimal = orc::Decimal(
            curVal, static_cast<int32_t>(scale)).toString(true);
          bloomFilter->addBytes(
            decimal.c_str(), static_cast<int64_t>(decimal.size()));
        }
        decStats->update(orc::Decimal(curVal, static_cast<int32_t>(scale)));
      }
    }
    decStats->increase(count);
    if (count < numValues) {
      decStats->setHasNull(true);
    }
    std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));
    scaleEncoder->add(scales.data(), offset, numValues, curNullsBuffer);
  }

  void OmniDecimal64ColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
    OmniColumnWriter::flush(streams);

    orc::proto::Stream dataStream;
    dataStream.set_kind(orc::proto::Stream_Kind_DATA);
    dataStream.set_column(static_cast<uint32_t>(columnId));
    dataStream.set_length(valueStream->flush());
    streams.push_back(dataStream);

    orc::proto::Stream secondaryStream;
    secondaryStream.set_kind(orc::proto::Stream_Kind_SECONDARY);
    secondaryStream.set_column(static_cast<uint32_t>(columnId));
    secondaryStream.set_length(scaleEncoder->flush());
    streams.push_back(secondaryStream);
  }

  uint64_t OmniDecimal64ColumnWriter::getEstimatedSize() const {
    uint64_t size = OmniColumnWriter::getEstimatedSize();
    size += valueStream->getSize();
    size += scaleEncoder->getBufferSize();
    return size;
  }

  void OmniDecimal64ColumnWriter::getColumnEncoding(
    std::vector<orc::proto::ColumnEncoding>& encodings) const {
    orc::proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void OmniDecimal64ColumnWriter::recordPosition() const {
    OmniColumnWriter::recordPosition();
    valueStream->recordPosition(rowIndexPosition.get());
    scaleEncoder->recordPosition(rowIndexPosition.get());
  }

class OmniDecimal128ColumnWriter : public OmniDecimal64ColumnWriter {
  public:
    OmniDecimal128ColumnWriter(const orc::Type& type,
                           const orc::StreamsFactory& factory,
                           const orc::WriterOptions& options);

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;

  private:
    char buffer[20];
  };

  OmniDecimal128ColumnWriter::OmniDecimal128ColumnWriter(
                              const orc::Type& type,
                              const orc::StreamsFactory& factory,
                              const orc::WriterOptions& options) :
                                OmniDecimal64ColumnWriter(type, factory, options) {
    // PASS
  }

  // Zigzag encoding moves the sign bit to the least significant bit using the
  // expression (val « 1) ^ (val » 63) and derives its name from the fact that
  // positive and negative numbers alternate once encoded.
  orc::Int128 zigZagInt128(const orc::Int128& value) {
    bool isNegative = value < 0;
    orc::Int128 val = value.abs();
    val <<= 1;
    if (isNegative) {
      val -= 1;
    }
    return val;
  }

  void OmniDecimal128ColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                   omniruntime::vec::NullsBuffer *pNullsBuffer,
                                   uint64_t offset,
                                   uint64_t numValues) {
    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    Decimal128FlatVector *flatVec = nullptr;
    Decimal128DictVector *dictVec = nullptr;
    if (dictEncoding) {
        dictVec = dynamic_cast<Decimal128DictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<Decimal128FlatVector *>(rowBatch);
    }
    if (flatVec == nullptr && dictVec == nullptr) {
      throw orc::InvalidArgument("Failed to cast to Decimal128VectorBatch");
    }

    orc::DecimalColumnStatisticsImpl* decStats =
      dynamic_cast<orc::DecimalColumnStatisticsImpl*>(colIndexStatistics.get());
    if (decStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to DecimalColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

    // The current encoding of decimal columns stores the integer representation
    // of the value as an unbounded length zigzag encoded base 128 varint.
    bool hasNull = curNullsBuffer->HasNull();
    uint64_t count = 0;
    uint64_t curPos;
    for (uint64_t i = 0; i < numValues; ++i) {
      curPos = offset + i;
      if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
        omniruntime::type::Decimal128 dec128Val  = dictEncoding ? dictVec->GetValue(curPos) : flatVec->GetValue(curPos);
        orc::Int128 curVal(dec128Val.HighBits(), dec128Val.LowBits());
        orc::Int128 val = zigZagInt128(curVal);
        char* data = buffer;
        while (true) {
          if ((val & ~0x7f) == 0) {
            *(data++) = (static_cast<char>(val.getLowBits()));
            break;
          } else {
            *(data++) = static_cast<char>(0x80 | (val.getLowBits() & 0x7f));
            val >>= 7;
          }
        }
        valueStream->write(buffer, static_cast<size_t>(data - buffer));

        ++count;
        if (enableBloomFilter) {
          std::string decimal = orc::Decimal(
            curVal, static_cast<int32_t>(scale)).toString(true);
          bloomFilter->addBytes(
            decimal.c_str(), static_cast<int64_t>(decimal.size()));
        }
        decStats->update(orc::Decimal(curVal, static_cast<int32_t>(scale)));
      }
    }
    decStats->increase(count);
    if (count < numValues) {
      decStats->setHasNull(true);
    }
    std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));
    scaleEncoder->add(scales.data(), offset, numValues, curNullsBuffer);
  }

  class OmniBinaryColumnWriter : public OmniStringColumnWriter {
  public:
      OmniBinaryColumnWriter(const orc::Type& type,
                             const orc::StreamsFactory& factory,
                             const orc::WriterOptions& options) :
          OmniStringColumnWriter(type, factory, options) {
          // PASS
      }

      void add(omniruntime::vec::BaseVector *rowBatch,
               omniruntime::vec::NullsBuffer *pNullsBuffer,
               uint64_t offset,
               uint64_t numValues);
  };

  void OmniBinaryColumnWriter::add(
          omniruntime::vec::BaseVector *rowBatch,
          omniruntime::vec::NullsBuffer *pNullsBuffer,
          uint64_t offset,
          uint64_t numValues) {
      if (numValues == 0) return;

      const bool isDict = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
      StringFlatVector *flatVec = nullptr;
      StringDictVector *dictVec = nullptr;

      if (isDict) {
          dictVec = dynamic_cast<StringDictVector *>(rowBatch);
      } else {
          flatVec = dynamic_cast<StringFlatVector *>(rowBatch);
      }

      if (flatVec == nullptr && dictVec == nullptr) {
          throw orc::InvalidArgument("Failed to cast to Vector<String> for Binary column");
      }

      orc::BinaryColumnStatisticsImpl* binStats =
              dynamic_cast<orc::BinaryColumnStatisticsImpl*>(colIndexStatistics.get());
      if (binStats == nullptr) {
          throw orc::InvalidArgument("Failed to cast to BinaryColumnStatisticsImpl");
      }

      omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
      bool hasNull = curNullsBuffer && curNullsBuffer->HasNull();
      if (!hasNull) {
          curNullsBuffer = nullptr;
      }
      OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

      std::unique_ptr<int64_t[]> length = std::make_unique<int64_t[]>(numValues);

      uint64_t count = 0;

      if (isDict) {
          if (hasNull) {
              // [路径 1]: Dictionary + 有空值
              for (uint64_t i = 0; i < numValues; ++i) {
                  uint64_t curPos = offset + i;
                  if (!curNullsBuffer->IsNull(curPos)) {
                      std::string_view sv = dictVec->GetValue(curPos);
                      length[i] = static_cast<int64_t>(sv.length());
                      uint64_t unsignedLength = static_cast<uint64_t>(length[i]);

                      directDataStream->write(sv.data(), unsignedLength);

                      if (enableBloomFilter) {
                          bloomFilter->addBytes(sv.data(), length[i]);
                      }
                      binStats->update(unsignedLength);
                      ++count;
                  }
              }
          } else {
              // [路径 2]: Dictionary + 无空值
              for (uint64_t i = 0; i < numValues; ++i) {
                  std::string_view sv = dictVec->GetValue(offset + i);
                  length[i] = static_cast<int64_t>(sv.length());
                  uint64_t unsignedLength = static_cast<uint64_t>(length[i]);

                  directDataStream->write(sv.data(), unsignedLength);

                  if (enableBloomFilter) {
                      bloomFilter->addBytes(sv.data(), length[i]);
                  }
                  binStats->update(unsignedLength);
              }
              count = numValues;
          }
      } else {
          if (hasNull) {
              // [路径 3]: Flat + 有空值
              for (uint64_t i = 0; i < numValues; ++i) {
                  uint64_t curPos = offset + i;
                  if (!curNullsBuffer->IsNull(curPos)) {
                      std::string_view sv = flatVec->GetValue(curPos);
                      length[i] = static_cast<int64_t>(sv.length());
                      uint64_t unsignedLength = static_cast<uint64_t>(length[i]);

                      directDataStream->write(sv.data(), unsignedLength);

                      if (enableBloomFilter) {
                          bloomFilter->addBytes(sv.data(), length[i]);
                      }
                      binStats->update(unsignedLength);
                      ++count;
                  }
              }
          } else {
              // [路径 4]: Flat + 无空值
              for (uint64_t i = 0; i < numValues; ++i) {
                  std::string_view sv = flatVec->GetValue(offset + i);
                  length[i] = static_cast<int64_t>(sv.length());
                  uint64_t unsignedLength = static_cast<uint64_t>(length[i]);

                  directDataStream->write(sv.data(), unsignedLength);

                  if (enableBloomFilter) {
                      bloomFilter->addBytes(sv.data(), length[i]);
                  }
                  binStats->update(unsignedLength);
              }
              count = numValues;
          }
      }

      directLengthEncoder->add(length.get(), offset, numValues, curNullsBuffer);

      binStats->increase(count);
      if (count < numValues) {
          binStats->setHasNull(true);
      }
  }

  class OmniTimestampColumnWriter : public OmniColumnWriter {
  public:
      OmniTimestampColumnWriter(const orc::Type& type,
                                const orc::StreamsFactory& factory,
                                const orc::WriterOptions& options,
                                bool isInstantType);

      virtual void add(
              omniruntime::vec::BaseVector* rowBatch,
              omniruntime::vec::NullsBuffer *pNullsBuffer,
              uint64_t offset,
              uint64_t numValues);

      virtual void flush(std::vector<orc::proto::Stream>& streams) override;

      virtual uint64_t getEstimatedSize() const override;

      virtual void getColumnEncoding(
              std::vector<orc::proto::ColumnEncoding>& encodings) const override;

      virtual void recordPosition() const override;

  protected:
      std::unique_ptr<OmniRleEncoder> secRleEncoder, nanoRleEncoder;

  private:
      void processSingleValue(int64_t micros, int64_t& outSec, int64_t& outNano,
                              orc::TimestampColumnStatisticsImpl* tsStats);

  private:
      orc::RleVersion rleVersion;
      const orc::Timezone& timezone;
      const bool isUTC;
  };

  OmniTimestampColumnWriter::OmniTimestampColumnWriter(
          const orc::Type& type,
          const orc::StreamsFactory& factory,
          const orc::WriterOptions& options,
          bool isInstantType) :
      OmniColumnWriter(type, factory, options),
      rleVersion(options.getRleVersion()),
      timezone(isInstantType ?
                             orc::getTimezoneByName("GMT") :
                             options.getTimezone()),
      isUTC(isInstantType ||
            options.getTimezoneName() == "GMT") {
      std::unique_ptr<orc::BufferedOutputStream> dataStream =
              factory.createStream(orc::proto::Stream_Kind_DATA);
      std::unique_ptr<orc::BufferedOutputStream> secondaryStream =
              factory.createStream(orc::proto::Stream_Kind_SECONDARY);
      secRleEncoder = createOmniRleEncoder(std::move(dataStream),
                                           true,
                                           rleVersion,
                                           memPool,
                                           options.getAlignedBitpacking());
      nanoRleEncoder = createOmniRleEncoder(std::move(secondaryStream),
                                            false,
                                            rleVersion,
                                            memPool,
                                            options.getAlignedBitpacking());

      if (enableIndex) {
          recordPosition();
      }
  }

  // Because the number of nanoseconds often has a large number of trailing zeros,
  // the number has trailing decimal zero digits removed and the last three bits
  // are used to record how many zeros were removed if the trailing zeros are
  // more than 2. Thus 1000 nanoseconds would be serialized as 0x0a and
  // 100000 would be serialized as 0x0c.
  static int64_t formatNano(int64_t nanos) {
      if (nanos == 0) {
          return 0;
      } else if (nanos % 100 != 0) {
          return (nanos) << 3;
      } else {
          nanos /= 100;
          int64_t trailingZeros = 1;
          while (nanos % 10 == 0 && trailingZeros < 7) {
              nanos /= 10;
              trailingZeros += 1;
          }
          return (nanos) << 3 | trailingZeros;
      }
  }

  void OmniTimestampColumnWriter::processSingleValue(
          int64_t micros,
          int64_t& outSec,
          int64_t& outNano,
          orc::TimestampColumnStatisticsImpl* tsStats) {
      //secs = Seconds since 1970 UTC
      int64_t secs = micros / 1000000;
      if (micros % 1000000 < 0) {
          secs -= 1;
      }
      int64_t nanos = (micros - (secs * 1000000)) * 1000;

      int64_t millsUTC = micros / 1000;
      if (!isUTC) {
          millsUTC = timezone.convertToUTC(millsUTC);
      }

      if (enableBloomFilter) {
          bloomFilter->addLong(millsUTC);
      }

      tsStats->update(millsUTC, static_cast<int32_t>(nanos % 1000000));

      // for timezone =  Asia/Shanghai, variant = timezone.getVariant(secs),
      // variant.gmtOffset = 28800 (8 hours), but here we
      secs -= timezone.getEpoch();

      if (secs < 0 && nanos > 999999) {
          secs += 1;
      }

      outSec = secs;
      outNano = formatNano(nanos);
  }

  void OmniTimestampColumnWriter::add(
          omniruntime::vec::BaseVector* rowBatch,
          omniruntime::vec::NullsBuffer *pNullsBuffer,
          uint64_t offset,
          uint64_t numValues) {
      if (numValues == 0) return;

      orc::TimestampColumnStatisticsImpl* tsStats =
              dynamic_cast<orc::TimestampColumnStatisticsImpl*>(colIndexStatistics.get());
      if (tsStats == nullptr) {
          throw orc::InvalidArgument("ERROR: TimestampColumnStatisticsImpl failed!");
      }

      omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
      bool hasNull = curNullsBuffer && curNullsBuffer->HasNull();
      if (!hasNull) {
          curNullsBuffer = nullptr;
      }
      OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

      std::vector<int64_t> secsBuffer(numValues);
      std::vector<int64_t> nanosBuffer(numValues);

      const bool isDict = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
      uint64_t notNullCount = 0;

      if (isDict) {
          auto* dictVec = dynamic_cast<I64DictVector *>(rowBatch);
          if (dictVec == nullptr) throw orc::InvalidArgument("Failed to cast to I64DictVector");

          if (hasNull) {
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (!curNullsBuffer->IsNull(offset + i)) {
                      processSingleValue(
                              static_cast<int64_t>(dictVec->GetValue(offset + i)),
                              secsBuffer[i], nanosBuffer[i],
                              tsStats
                      );
                      ++notNullCount;
                  }
              }
          } else {
              for (uint64_t i = 0; i < numValues; ++i) {
                  processSingleValue(
                          static_cast<int64_t>(dictVec->GetValue(offset + i)),
                          secsBuffer[i], nanosBuffer[i],
                          tsStats
                  );
              }
              notNullCount = numValues;
          }
      } else {
          auto* flatVec = dynamic_cast<I64FlatVector *>(rowBatch);
          if (flatVec == nullptr) throw orc::InvalidArgument("Failed to cast to I64FlatVector");

          const int64_t *data = flatVec->GetValuesBuffer() + offset;

          if (hasNull) {
              for (uint64_t i = 0; i < numValues; ++i) {
                  if (!curNullsBuffer->IsNull(offset + i)) {
                      processSingleValue(
                              data[i],
                              secsBuffer[i], nanosBuffer[i],
                              tsStats
                      );
                      ++notNullCount;
                  }
              }
          } else {
              for (uint64_t i = 0; i < numValues; ++i) {
                  processSingleValue(
                          data[i],
                          secsBuffer[i], nanosBuffer[i],
                          tsStats
                  );
              }
              notNullCount = numValues;
          }
      }

      secRleEncoder->add(secsBuffer.data(), offset, numValues, curNullsBuffer);
      nanoRleEncoder->add(nanosBuffer.data(), offset, numValues, curNullsBuffer);

      if (notNullCount < numValues) {
          tsStats->setHasNull(true);
      }
      tsStats->increase(notNullCount);
  }

  void OmniTimestampColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
      OmniColumnWriter::flush(streams);

      orc::proto::Stream dataStream;
      dataStream.set_kind(orc::proto::Stream_Kind_DATA);
      dataStream.set_column(static_cast<uint32_t>(columnId));
      dataStream.set_length(secRleEncoder->flush());
      streams.push_back(dataStream);

      orc::proto::Stream secondaryStream;
      secondaryStream.set_kind(orc::proto::Stream_Kind_SECONDARY);
      secondaryStream.set_column(static_cast<uint32_t>(columnId));
      secondaryStream.set_length(nanoRleEncoder->flush());
      streams.push_back(secondaryStream);
  }

  uint64_t OmniTimestampColumnWriter::getEstimatedSize() const {
      uint64_t size = OmniColumnWriter::getEstimatedSize();
      size += secRleEncoder->getBufferSize();
      size += nanoRleEncoder->getBufferSize();
      return size;
  }

  void OmniTimestampColumnWriter::getColumnEncoding(
          std::vector<orc::proto::ColumnEncoding>& encodings) const {
      orc::proto::ColumnEncoding encoding;
      encoding.set_kind(RleVersionMapper(rleVersion));
      encoding.set_dictionarysize(0);
      if (enableBloomFilter) {
          encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
      }
      encodings.push_back(encoding);
  }

  void OmniTimestampColumnWriter::recordPosition() const {
      OmniColumnWriter::recordPosition();
      secRleEncoder->recordPosition(rowIndexPosition.get());
      nanoRleEncoder->recordPosition(rowIndexPosition.get());
  }

class OmniDateColumnWriter : public OmniIntegerColumnWriter {
  public:
    OmniDateColumnWriter(const orc::Type& type,
                     const orc::StreamsFactory& factory,
                     const orc::WriterOptions& options);

    virtual void add(omniruntime::vec::BaseVector *rowBatch,
                     omniruntime::vec::NullsBuffer *pNullsBuffer,
                     uint64_t offset,
                     uint64_t numValues) override;
  };

    OmniDateColumnWriter::OmniDateColumnWriter(
                        const orc::Type &type,
                        const orc::StreamsFactory &factory,
                        const orc::WriterOptions &options) :
                            OmniIntegerColumnWriter(type, factory, options) {
    // PASS
  }

  void OmniDateColumnWriter::add(omniruntime::vec::BaseVector *rowBatch,
                                 omniruntime::vec::NullsBuffer *pNullsBuffer,
                                 uint64_t offset,
                                 uint64_t numValues) {
    if (rowBatch->GetTypeId() == omniruntime::type::DataTypeId::OMNI_DATE64) {
        throw orc::InvalidArgument("ERROR : ORC doesn't support OMNI_DATE64, only support OMNI_DATE32 !");
    }

    bool dictEncoding = rowBatch->GetEncoding() == omniruntime::vec::Encoding::OMNI_DICTIONARY;
    I32FlatVector *flatVec = nullptr;
    I32DictVector *dictVec = nullptr;
    if(dictEncoding) {
        dictVec = dynamic_cast<I32DictVector *>(rowBatch);
    } else {
        flatVec = dynamic_cast<I32FlatVector *>(rowBatch);
    }

    if (flatVec == nullptr && dictVec == nullptr) {
        throw orc::InvalidArgument("Failed to cast to DateVectorBatch");
    }

    orc::DateColumnStatisticsImpl* dateStats =
      dynamic_cast<orc::DateColumnStatisticsImpl*>(colIndexStatistics.get());
    if (dateStats == nullptr) {
      throw orc::InvalidArgument("Failed to cast to DateColumnStatisticsImpl");
    }

    omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
    OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);
    omniruntime::mem::AlignedBuffer<int32_t> dateBuffer;
    int32_t *data = dictEncoding ? dateBuffer.AllocateReuse(numValues, false) : flatVec->GetValuesBuffer() + offset;
    if (dictEncoding) {
        rleEncoder->addDate(dictVec, data, offset, numValues);
    } else {
        rleEncoder->addDate(data, offset, numValues, curNullsBuffer);
    }

    uint64_t count = 0;
    bool hasNull = curNullsBuffer->HasNull();
    uint64_t curPos = offset;
    int32_t curVal = 0;
    for (uint64_t i = 0; i < numValues; ++i, ++curPos) {
      if (!hasNull || !curNullsBuffer->IsNull(curPos)) {
        ++count;
        curVal = data[i];
        dateStats->update(curVal);
        if (enableBloomFilter) {
          bloomFilter->addLong(curVal);
        }
      }
    }
    dateStats->increase(count);
    if (count < numValues) {
      dateStats->setHasNull(true);
    }
  }

  class OmniListColumnWriter : public OmniColumnWriter {
  public:
      OmniListColumnWriter(const orc::Type& type,
                           const orc::StreamsFactory& factory,
                           const orc::WriterOptions& options);
      ~OmniListColumnWriter() override;

      void add(omniruntime::vec::BaseVector *rowBatch,
               omniruntime::vec::NullsBuffer *pNullsBuffer,
               uint64_t offset,
               uint64_t numValues) override;

      virtual void flush(std::vector<orc::proto::Stream>& streams) override;

      virtual uint64_t getEstimatedSize() const override;

      virtual void getColumnEncoding(
              std::vector<orc::proto::ColumnEncoding>& encodings) const override;

      virtual void getStripeStatistics(
              std::vector<orc::proto::ColumnStatistics>& stats) const override;

      virtual void getFileStatistics(
              std::vector<orc::proto::ColumnStatistics>& stats) const override;

      virtual void mergeStripeStatsIntoFileStats() override;

      virtual void mergeRowGroupStatsIntoStripeStats() override;

      virtual void createRowIndexEntry() override;

      virtual void writeIndex(
              std::vector<orc::proto::Stream> &streams) const override;

      virtual void recordPosition() const override;

      virtual void writeDictionary() override;

      virtual void reset() override;

  private:
      std::unique_ptr<OmniRleEncoder> lengthEncoder;
      orc::RleVersion rleVersion;
      std::unique_ptr<OmniColumnWriter> child;
  };

  OmniListColumnWriter::OmniListColumnWriter(const orc::Type& type,
                                             const orc::StreamsFactory& factory,
                                             const orc::WriterOptions& options) :
      OmniColumnWriter(type, factory, options),
      rleVersion(options.getRleVersion()){

      std::unique_ptr<orc::BufferedOutputStream> lengthStream =
              factory.createStream(orc::proto::Stream_Kind_LENGTH);
      lengthEncoder = createOmniRleEncoder(std::move(lengthStream),
                                           false,
                                           rleVersion,
                                           memPool,
                                           options.getAlignedBitpacking());

      if (type.getSubtypeCount() == 1) {
          child = buildOmniWriter(*type.getSubtype(0), factory, options);
      }

      if (enableIndex) {
          recordPosition();
      }
  }

  OmniListColumnWriter::~OmniListColumnWriter() {
      // PASS
  }

  void OmniListColumnWriter::add(
          omniruntime::vec::BaseVector *rowBatch,
          omniruntime::vec::NullsBuffer *pNullsBuffer,
          uint64_t offset,
          uint64_t numValues) {
      if (numValues == 0) return;

      auto *arrayVector = dynamic_cast<omniruntime::vec::ArrayVector *>(rowBatch);
      if (arrayVector == nullptr) {
          throw orc::InvalidArgument("Failed to cast to ArrayVector");
      }

      omniruntime::vec::NullsBuffer *curNullsBuffer = rowBatch->GetNullsBuffer();
      bool hasNull = curNullsBuffer && curNullsBuffer->HasNull();
      if (!hasNull) {
          curNullsBuffer = nullptr;
      }
      OmniColumnWriter::addNulls(curNullsBuffer, pNullsBuffer, offset, numValues);

      const int64_t* offsets = arrayVector->GetOffsets();
      omniruntime::vec::BaseVector* elementVector = arrayVector->GetElementVector().get();

      int64_t elemStartOffset = offsets[offset];
      int64_t totalElemCount = offsets[offset + numValues] - offsets[offset];

      std::vector<int64_t> lengths(numValues);
      uint64_t validCount = 0;

      for (uint64_t i = 0; i < numValues; ++i) {
          uint64_t curIdx = offset + i;
          int64_t len = offsets[curIdx + 1] - offsets[curIdx];
          lengths[i] = len;

          if (!hasNull || !curNullsBuffer->IsNull(curIdx)) {
              validCount++;
              if (enableBloomFilter) {
                  bloomFilter->addLong(len);
              }
          }
      }

      lengthEncoder->add(lengths.data(), offset, numValues, curNullsBuffer);

      if (totalElemCount > 0 && child) {
          child->add(elementVector, nullptr, elemStartOffset, totalElemCount);
      }

      if (enableIndex && colIndexStatistics) {
          colIndexStatistics->increase(validCount);
          if (validCount < numValues) {
              colIndexStatistics->setHasNull(true);
          }
      }
  }

  void OmniListColumnWriter::flush(std::vector<orc::proto::Stream>& streams) {
      OmniColumnWriter::flush(streams);

      orc::proto::Stream stream;
      stream.set_kind(orc::proto::Stream_Kind_LENGTH);
      stream.set_column(static_cast<uint32_t>(columnId));
      stream.set_length(lengthEncoder->flush());
      streams.push_back(stream);

      if (child.get()) {
          child->flush(streams);
      }
  }

  void OmniListColumnWriter::writeIndex(std::vector<orc::proto::Stream> &streams) const {
      OmniColumnWriter::writeIndex(streams);
      if (child.get()) {
          child->writeIndex(streams);
      }
  }

  uint64_t OmniListColumnWriter::getEstimatedSize() const {
      uint64_t size = OmniColumnWriter::getEstimatedSize();
      if (child.get()) {
          size += lengthEncoder->getBufferSize();
          size += child->getEstimatedSize();
      }
      return size;
  }

  void OmniListColumnWriter::getColumnEncoding(
          std::vector<orc::proto::ColumnEncoding>& encodings) const {
      orc::proto::ColumnEncoding encoding;
      encoding.set_kind(RleVersionMapper(rleVersion));
      encoding.set_dictionarysize(0);
      if (enableBloomFilter) {
          encoding.set_bloomencoding(orc::BloomFilterVersion::UTF8);
      }
      encodings.push_back(encoding);
      if (child.get()) {
          child->getColumnEncoding(encodings);
      }
  }

  void OmniListColumnWriter::getStripeStatistics(
          std::vector<orc::proto::ColumnStatistics>& stats) const {
      OmniColumnWriter::getStripeStatistics(stats);
      if (child.get()) {
          child->getStripeStatistics(stats);
      }
  }

  void OmniListColumnWriter::mergeStripeStatsIntoFileStats() {
      OmniColumnWriter::mergeStripeStatsIntoFileStats();
      if (child.get()) {
          child->mergeStripeStatsIntoFileStats();
      }
  }

  void OmniListColumnWriter::getFileStatistics(
          std::vector<orc::proto::ColumnStatistics>& stats) const {
      OmniColumnWriter::getFileStatistics(stats);
      if (child.get()) {
          child->getFileStatistics(stats);
      }
  }

  void OmniListColumnWriter::mergeRowGroupStatsIntoStripeStats()  {
      OmniColumnWriter::mergeRowGroupStatsIntoStripeStats();
      if (child.get()) {
          child->mergeRowGroupStatsIntoStripeStats();
      }
  }

  void OmniListColumnWriter::createRowIndexEntry() {
      OmniColumnWriter::createRowIndexEntry();
      if (child.get()) {
          child->createRowIndexEntry();
      }
  }

  void OmniListColumnWriter::recordPosition() const {
      OmniColumnWriter::recordPosition();
      lengthEncoder->recordPosition(rowIndexPosition.get());
  }

  void OmniListColumnWriter::reset() {
      OmniColumnWriter::reset();
      if (child) {
          child->reset();
      }
  }

  void OmniListColumnWriter::writeDictionary() {
      if (child) {
          child->writeDictionary();
      }
  }

   /* TODO UnionColumnWriter */
//  class UnionColumnWriter : public ColumnWriter {
//  public:
//    UnionColumnWriter(const Type& type,
//                      const StreamsFactory& factory,
//                      const WriterOptions& options);
//
//    virtual void add(ColumnVectorBatch& rowBatch,
//                     uint64_t offset,
//                     uint64_t numValues,
//                     const char* incomingMask) override;
//
//    virtual void flush(std::vector<proto::Stream>& streams) override;
//
//    virtual uint64_t getEstimatedSize() const override;
//
//    virtual void getColumnEncoding(
//      std::vector<proto::ColumnEncoding>& encodings) const override;
//
//    virtual void getStripeStatistics(
//      std::vector<proto::ColumnStatistics>& stats) const override;
//
//    virtual void getFileStatistics(
//      std::vector<proto::ColumnStatistics>& stats) const override;
//
//    virtual void mergeStripeStatsIntoFileStats() override;
//
//    virtual void mergeRowGroupStatsIntoStripeStats() override;
//
//    virtual void createRowIndexEntry() override;
//
//    virtual void writeIndex(
//      std::vector<proto::Stream> &streams) const override;
//
//    virtual void recordPosition() const override;
//
//    virtual void writeDictionary() override;
//
//    virtual void reset() override;
//
//  private:
//    std::unique_ptr<ByteRleEncoder> rleEncoder;
//    std::vector<std::unique_ptr<ColumnWriter>> children;
//  };
//
//  UnionColumnWriter::UnionColumnWriter(const Type& type,
//                                       const StreamsFactory& factory,
//                                       const WriterOptions& options) :
//    ColumnWriter(type, factory, options) {
//
//    std::unique_ptr<BufferedOutputStream> dataStream =
//      factory.createStream(proto::Stream_Kind_DATA);
//    rleEncoder = createByteRleEncoder(std::move(dataStream));
//
//    for (uint64_t i = 0; i != type.getSubtypeCount(); ++i) {
//      children.push_back(buildWriter(*type.getSubtype(i),
//                                     factory,
//                                     options));
//    }
//
//    if (enableIndex) {
//      recordPosition();
//    }
//  }
//
//  void UnionColumnWriter::add(ColumnVectorBatch& rowBatch,
//                              uint64_t offset,
//                              uint64_t numValues,
//                              const char* incomingMask) {
//    UnionVectorBatch* unionBatch = dynamic_cast<UnionVectorBatch*>(&rowBatch);
//    if (unionBatch == nullptr) {
//      throw InvalidArgument("Failed to cast to UnionVectorBatch");
//    }
//
//    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);
//
//    const char* notNull = unionBatch->hasNulls ?
//                          unionBatch->notNull.data() + offset : nullptr;
//    unsigned char * tags = unionBatch->tags.data() + offset;
//    uint64_t * offsets = unionBatch->offsets.data() + offset;
//
//    std::vector<int64_t> childOffset(children.size(), -1);
//    std::vector<uint64_t> childLength(children.size(), 0);
//
//    for (uint64_t i = 0; i != numValues; ++i) {
//      if (childOffset[tags[i]] == -1) {
//        childOffset[tags[i]] = static_cast<int64_t>(offsets[i]);
//      }
//      ++childLength[tags[i]];
//    }
//
//    rleEncoder->add(reinterpret_cast<char*>(tags), numValues, notNull);
//
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      if (childLength[i] > 0) {
//        children[i]->add(*unionBatch->children[i],
//                         static_cast<uint64_t>(childOffset[i]),
//                         childLength[i], nullptr);
//      }
//    }
//
//    // update stats
//    if (enableIndex) {
//      if (!notNull) {
//        colIndexStatistics->increase(numValues);
//      } else {
//        uint64_t count = 0;
//        for (uint64_t i = 0; i < numValues; ++i) {
//          if (notNull[i]) {
//            ++count;
//            if (enableBloomFilter) {
//              bloomFilter->addLong(tags[i]);
//            }
//          }
//        }
//        colIndexStatistics->increase(count);
//        if (count < numValues) {
//          colIndexStatistics->setHasNull(true);
//        }
//      }
//    }
//  }
//
//  void UnionColumnWriter::flush(std::vector<proto::Stream>& streams) {
//    ColumnWriter::flush(streams);
//
//    proto::Stream stream;
//    stream.set_kind(proto::Stream_Kind_DATA);
//    stream.set_column(static_cast<uint32_t>(columnId));
//    stream.set_length(rleEncoder->flush());
//    streams.push_back(stream);
//
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->flush(streams);
//    }
//  }
//
//  void UnionColumnWriter::writeIndex(std::vector<proto::Stream> &streams) const {
//    ColumnWriter::writeIndex(streams);
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->writeIndex(streams);
//    }
//  }
//
//  uint64_t UnionColumnWriter::getEstimatedSize() const {
//    uint64_t size = ColumnWriter::getEstimatedSize();
//    size += rleEncoder->getBufferSize();
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      size += children[i]->getEstimatedSize();
//    }
//    return size;
//  }
//
//  void UnionColumnWriter::getColumnEncoding(
//                     std::vector<proto::ColumnEncoding>& encodings) const {
//    proto::ColumnEncoding encoding;
//    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
//    encoding.set_dictionarysize(0);
//    if (enableBloomFilter) {
//      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
//    }
//    encodings.push_back(encoding);
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->getColumnEncoding(encodings);
//    }
//  }
//
//  void UnionColumnWriter::getStripeStatistics(
//                     std::vector<proto::ColumnStatistics>& stats) const {
//    ColumnWriter::getStripeStatistics(stats);
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->getStripeStatistics(stats);
//    }
//  }
//
//  void UnionColumnWriter::mergeStripeStatsIntoFileStats() {
//    ColumnWriter::mergeStripeStatsIntoFileStats();
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->mergeStripeStatsIntoFileStats();
//    }
//  }
//
//  void UnionColumnWriter::getFileStatistics(
//                     std::vector<proto::ColumnStatistics>& stats) const {
//    ColumnWriter::getFileStatistics(stats);
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->getFileStatistics(stats);
//    }
//  }
//
//  void UnionColumnWriter::mergeRowGroupStatsIntoStripeStats()  {
//    ColumnWriter::mergeRowGroupStatsIntoStripeStats();
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->mergeRowGroupStatsIntoStripeStats();
//    }
//  }
//
//  void UnionColumnWriter::createRowIndexEntry() {
//    ColumnWriter::createRowIndexEntry();
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->createRowIndexEntry();
//    }
//  }
//
//  void UnionColumnWriter::recordPosition() const {
//    ColumnWriter::recordPosition();
//    rleEncoder->recordPosition(rowIndexPosition.get());
//  }
//
//  void UnionColumnWriter::reset() {
//    ColumnWriter::reset();
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->reset();
//    }
//  }
//
//  void UnionColumnWriter::writeDictionary() {
//    for (uint32_t i = 0; i < children.size(); ++i) {
//      children[i]->writeDictionary();
//    }
//  }

  /* TODO UnionColumnWriter */

   /* TODO buildOmniWriter */
  std::unique_ptr<OmniColumnWriter> buildOmniWriter(
                                            const orc::Type& type,
                                            const orc::StreamsFactory& factory,
                                            const orc::WriterOptions& options) {
    switch (static_cast<int64_t>(type.getKind())) {
      case orc::STRUCT:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniStructColumnWriter(
                                 type,
                                 factory,
                                 options));
      case orc::INT:
          return std::unique_ptr<OmniColumnWriter>(
                  new OmniIntColumnWriter(
                          type,
                          factory,
                          options));
      case orc::LONG:
          return std::unique_ptr<OmniColumnWriter>(
                  new OmniLongColumnWriter(
                          type,
                          factory,
                          options));
      case orc::SHORT:
          return std::unique_ptr<OmniColumnWriter>(
                  new OmniShortColumnWriter(
                          type,
                          factory,
                          options));
      case orc::BYTE:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniByteColumnWriter(
                               type,
                               factory,
                               options));
      case orc::BOOLEAN:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniBooleanColumnWriter(
                                  type,
                                  factory,
                                  options));
        case orc::DOUBLE:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniDoubleColumnWriter(
                                 type,
                                 factory,
                                 options));
        case orc::FLOAT:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniFloatColumnWriter(
                                 type,
                                 factory,
                                 options));
        case orc::BINARY:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniBinaryColumnWriter(
                                 type,
                                 factory,
                                 options));
       case orc::STRING:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniStringColumnWriter(
                                 type,
                                 factory,
                                 options));
      case orc::CHAR:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniCharColumnWriter(
                               type,
                               factory,
                               options));
      case orc::VARCHAR:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniVarCharColumnWriter(
                                  type,
                                  factory,
                                  options));
    case orc::DATE:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniDateColumnWriter(
                               type,
                               factory,
                               options));
    case orc::TIMESTAMP:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniTimestampColumnWriter(
                                    type,
                                    factory,
                                    options,
                                    false));
    case orc::TIMESTAMP_INSTANT:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniTimestampColumnWriter(
                                    type,
                                    factory,
                                    options,
                                    true));
        case orc::DECIMAL:
        if (type.getPrecision() <= OmniDecimal64ColumnWriter::MAX_PRECISION_64) {
          return std::unique_ptr<OmniColumnWriter>(
            new OmniDecimal64ColumnWriter(
                                      type,
                                      factory,
                                      options));
        } else if (type.getPrecision() <= OmniDecimal64ColumnWriter::MAX_PRECISION_128) {
          return std::unique_ptr<OmniColumnWriter>(
            new OmniDecimal128ColumnWriter(
                                       type,
                                       factory,
                                       options));
        } else {
          throw orc::NotImplementedYet("Decimal precision more than 38 is not "
                                    "supported");
        }
        case orc::LIST:
        return std::unique_ptr<OmniColumnWriter>(
          new OmniListColumnWriter(
                               type,
                               factory,
                               options));
//      case UNION:
//        return std::unique_ptr<ColumnWriter>(
//          new UnionColumnWriter(
//                                type,
//                                factory,
//                                options));
      default:
        throw orc::NotImplementedYet("Type is not supported yet for creating "
                                  "ColumnWriter.");
    }
  }
}