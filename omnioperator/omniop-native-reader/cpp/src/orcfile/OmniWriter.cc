#include "orc/Common.hh"
#include "orc/OrcFile.hh"
#include "orc/Timezone.hh"
#include "OmniWriter.hh"

#include <memory>

namespace omniruntime::writer {

    OmniWriter::~OmniWriter() {
        // PASS
    }

    class OmniWriterImpl : public OmniWriter {
    private:
        std::unique_ptr<OmniColumnWriter> columnWriter;
        std::unique_ptr<orc::BufferedOutputStream> compressionStream;
        std::unique_ptr<orc::BufferedOutputStream> bufferedStream;
        std::unique_ptr<orc::StreamsFactory> streamsFactory;
        orc::OutputStream* outStream;
        orc::WriterOptions options;
        const orc::Type& type;
        uint64_t stripeRows, totalRows, indexRows;
        uint64_t currentOffset;
        orc::proto::Footer fileFooter;
        orc::proto::PostScript postScript;
        orc::proto::StripeInformation stripeInfo;
        orc::proto::Metadata metadata;

        static const char* magicId;
        static const orc::WriterId writerId;

    public:
        OmniWriterImpl(
                const orc::Type& type,
                orc::OutputStream* stream,
                const orc::WriterOptions& options);


        void add(omniruntime::vec::BaseVector* rowsToAdd, uint64_t startPos, uint64_t endPos) override;

        void close() override;

        void addUserMetadata(const std::string name, const std::string value) override;

    private:
        void init();
        void initStripe();
        void writeStripe();
        void writeMetadata();
        void writeFileFooter();
        void writePostscript();
        void buildFooterType(const orc::Type& t, orc::proto::Footer& footer, uint32_t& index);
        static orc::proto::CompressionKind convertCompressionKind(
                const orc::CompressionKind& kind);
    };

    const char * OmniWriterImpl::magicId = "ORC";

    const orc::WriterId OmniWriterImpl::writerId = orc::WriterId::ORC_CPP_WRITER;

    OmniWriterImpl::OmniWriterImpl(
            const orc::Type& t,
            orc::OutputStream* stream,
            const orc::WriterOptions& opts) :
            outStream(stream),
            options(opts),
            type(t) {
        streamsFactory = createStreamsFactory(options, outStream);
        columnWriter = buildOmniWriter(type, *streamsFactory, options);
        stripeRows = totalRows = indexRows = 0;
        currentOffset = 0;

        // compression stream for stripe footer, file footer and metadata
        compressionStream = orc::createCompressor(
                options.getCompression(),
                outStream,
                options.getCompressionStrategy(),
                1 * 1024 * 1024, // buffer capacity: 1M
                options.getCompressionBlockSize(),
                *options.getMemoryPool());

        // uncompressed stream for post script
        bufferedStream.reset(new orc::BufferedOutputStream(
                *options.getMemoryPool(),
                outStream,
                1024, // buffer capacity: 1024 bytes
                options.getCompressionBlockSize()));

        init();
    }

    void OmniWriterImpl::add(omniruntime::vec::BaseVector* rowsToAdd, uint64_t startPos, uint64_t endPos) {
        auto numElements = endPos - startPos;
        auto omniColumnWriter = dynamic_cast<OmniColumnWriter*>(columnWriter.get());
        if (options.getEnableIndex()) {
            uint64_t pos = 0;
            uint64_t chunkSize = 0;
            uint64_t rowIndexStride = options.getRowIndexStride();

            while (pos < numElements) {
                chunkSize = std::min(numElements - pos,
                                     rowIndexStride - indexRows);

                omniColumnWriter->add(rowsToAdd, nullptr, startPos + pos, chunkSize);

                pos += chunkSize;
                indexRows += chunkSize;
                stripeRows += chunkSize;

                if (indexRows >= rowIndexStride) {
                    columnWriter->createRowIndexEntry();
                    indexRows = 0;
                }
            }
        } else {
            stripeRows += numElements;
            omniColumnWriter->add(rowsToAdd, nullptr, startPos, numElements);
        }

        if (columnWriter->getEstimatedSize() >= options.getStripeSize()) {
            writeStripe();
        }
    }

    void OmniWriterImpl::close() {
        if (stripeRows > 0) {
            writeStripe();
        }
        writeMetadata();
        writeFileFooter();
        writePostscript();
        outStream->close();
    }

    void OmniWriterImpl::addUserMetadata(const std::string name, const std::string value){
        orc::proto::UserMetadataItem* userMetadataItem = fileFooter.add_metadata();
        userMetadataItem->set_name(name);
        userMetadataItem->set_value(value);
    }

    void OmniWriterImpl::init() {
        // Write file header
        const static size_t magicIdLength = strlen(OmniWriterImpl::magicId);
        outStream->write(OmniWriterImpl::magicId, magicIdLength);
        currentOffset += magicIdLength;

        // Initialize file footer
        fileFooter.set_headerlength(currentOffset);
        fileFooter.set_contentlength(0);
        fileFooter.set_numberofrows(0);
        fileFooter.set_rowindexstride(
                static_cast<uint32_t>(options.getRowIndexStride()));
        fileFooter.set_writer(writerId);
        fileFooter.set_softwareversion(ORC_VERSION);

        uint32_t index = 0;
        buildFooterType(type, fileFooter, index);

        // Initialize post script
        postScript.set_footerlength(0);
        postScript.set_compression(
                OmniWriterImpl::convertCompressionKind(options.getCompression()));
        postScript.set_compressionblocksize(options.getCompressionBlockSize());

        postScript.add_version(options.getFileVersion().getMajor());
        postScript.add_version(options.getFileVersion().getMinor());

        postScript.set_writerversion(orc::WriterVersion_ORC_135);
        postScript.set_magic("ORC");

        // Initialize first stripe
        initStripe();
    }

    void OmniWriterImpl::initStripe() {
        stripeInfo.set_offset(currentOffset);
        stripeInfo.set_indexlength(0);
        stripeInfo.set_datalength(0);
        stripeInfo.set_footerlength(0);
        stripeInfo.set_numberofrows(0);

        stripeRows = indexRows = 0;
    }

    void OmniWriterImpl::writeStripe() {
        if (options.getEnableIndex() && indexRows != 0) {
            columnWriter->createRowIndexEntry();
            indexRows = 0;
        } else {
            columnWriter->mergeRowGroupStatsIntoStripeStats();
        }

        // dictionary should be written before any stream is flushed
        columnWriter->writeDictionary();

        std::vector<orc::proto::Stream> streams;
        // write ROW_INDEX streams
        if (options.getEnableIndex()) {
            columnWriter->writeIndex(streams);
        }
        // write streams like PRESENT, DATA, etc.
        columnWriter->flush(streams);

        // generate and write stripe footer
        orc::proto::StripeFooter stripeFooter;
        for (uint32_t i = 0; i < streams.size(); ++i) {
            *stripeFooter.add_streams() = streams[i];
        }

        std::vector<orc::proto::ColumnEncoding> encodings;
        columnWriter->getColumnEncoding(encodings);

        for (uint32_t i = 0; i < encodings.size(); ++i) {
            *stripeFooter.add_columns() = encodings[i];
        }

        stripeFooter.set_writertimezone(options.getTimezoneName());

        // add stripe statistics to metadata
        orc::proto::StripeStatistics* stripeStats = metadata.add_stripestats();
        std::vector<orc::proto::ColumnStatistics> colStats;
        columnWriter->getStripeStatistics(colStats);
        for (uint32_t i = 0; i != colStats.size(); ++i) {
            *stripeStats->add_colstats() = colStats[i];
        }
        // merge stripe stats into file stats and clear stripe stats
        columnWriter->mergeStripeStatsIntoFileStats();

        if (!stripeFooter.SerializeToZeroCopyStream(compressionStream.get())) {
            throw std::logic_error("Failed to write stripe footer.");
        }
        uint64_t footerLength = compressionStream->flush();

        // calculate data length and index length
        uint64_t dataLength = 0;
        uint64_t indexLength = 0;
        for (uint32_t i = 0; i < streams.size(); ++i) {
            if (streams[i].kind() == orc::proto::Stream_Kind_ROW_INDEX ||
                streams[i].kind() == orc::proto::Stream_Kind_BLOOM_FILTER_UTF8) {
                indexLength += streams[i].length();
            } else {
                dataLength += streams[i].length();
            }
        }

        // update stripe info
        stripeInfo.set_indexlength(indexLength);
        stripeInfo.set_datalength(dataLength);
        stripeInfo.set_footerlength(footerLength);
        stripeInfo.set_numberofrows(stripeRows);

        *fileFooter.add_stripes() = stripeInfo;

        currentOffset = currentOffset + indexLength + dataLength + footerLength;
        totalRows += stripeRows;

        columnWriter->reset();

        initStripe();
    }

    void OmniWriterImpl::writeMetadata() {
        if (!metadata.SerializeToZeroCopyStream(compressionStream.get())) {
            throw std::logic_error("Failed to write metadata.");
        }
        postScript.set_metadatalength(compressionStream.get()->flush());
    }

    void OmniWriterImpl::writeFileFooter() {
        fileFooter.set_contentlength(currentOffset - fileFooter.headerlength());
        fileFooter.set_numberofrows(totalRows);

        // update file statistics
        std::vector<orc::proto::ColumnStatistics> colStats;
        columnWriter->getFileStatistics(colStats);
        for (uint32_t i = 0; i != colStats.size(); ++i) {
            *fileFooter.add_statistics() = colStats[i];
        }

        if (!fileFooter.SerializeToZeroCopyStream(compressionStream.get())) {
            throw std::logic_error("Failed to write file footer.");
        }
        postScript.set_footerlength(compressionStream->flush());
    }

    void OmniWriterImpl::writePostscript() {
        if (!postScript.SerializeToZeroCopyStream(bufferedStream.get())) {
            throw std::logic_error("Failed to write post script.");
        }
        unsigned char psLength =
                static_cast<unsigned char>(bufferedStream->flush());
        outStream->write(&psLength, sizeof(unsigned char));
    }

    void OmniWriterImpl::buildFooterType(
            const orc::Type& t,
            orc::proto::Footer& footer,
            uint32_t & index) {
        orc::proto::Type protoType;
        protoType.set_maximumlength(static_cast<uint32_t>(t.getMaximumLength()));
        protoType.set_precision(static_cast<uint32_t>(t.getPrecision()));
        protoType.set_scale(static_cast<uint32_t>(t.getScale()));

        switch (t.getKind()) {
            case orc::BOOLEAN: {
                protoType.set_kind(orc::proto::Type_Kind_BOOLEAN);
                break;
            }
            case orc::BYTE: {
                protoType.set_kind(orc::proto::Type_Kind_BYTE);
                break;
            }
            case orc::SHORT: {
                protoType.set_kind(orc::proto::Type_Kind_SHORT);
                break;
            }
            case orc::INT: {
                protoType.set_kind(orc::proto::Type_Kind_INT);
                break;
            }
            case orc::LONG: {
                protoType.set_kind(orc::proto::Type_Kind_LONG);
                break;
            }
            case orc::FLOAT: {
                protoType.set_kind(orc::proto::Type_Kind_FLOAT);
                break;
            }
            case orc::DOUBLE: {
                protoType.set_kind(orc::proto::Type_Kind_DOUBLE);
                break;
            }
            case orc::STRING: {
                protoType.set_kind(orc::proto::Type_Kind_STRING);
                break;
            }
            case orc::BINARY: {
                protoType.set_kind(orc::proto::Type_Kind_BINARY);
                break;
            }
            case orc::TIMESTAMP: {
                protoType.set_kind(orc::proto::Type_Kind_TIMESTAMP);
                break;
            }
            case orc::TIMESTAMP_INSTANT: {
                protoType.set_kind(orc::proto::Type_Kind_TIMESTAMP_INSTANT);
                break;
            }
            case orc::LIST: {
                protoType.set_kind(orc::proto::Type_Kind_LIST);
                break;
            }
            case orc::MAP: {
                protoType.set_kind(orc::proto::Type_Kind_MAP);
                break;
            }
            case orc::STRUCT: {
                protoType.set_kind(orc::proto::Type_Kind_STRUCT);
                break;
            }
            case orc::UNION: {
                protoType.set_kind(orc::proto::Type_Kind_UNION);
                break;
            }
            case orc::DECIMAL: {
                protoType.set_kind(orc::proto::Type_Kind_DECIMAL);
                break;
            }
            case orc::DATE: {
                protoType.set_kind(orc::proto::Type_Kind_DATE);
                break;
            }
            case orc::VARCHAR: {
                protoType.set_kind(orc::proto::Type_Kind_VARCHAR);
                break;
            }
            case orc::CHAR: {
                protoType.set_kind(orc::proto::Type_Kind_CHAR);
                break;
            }
            default:
                throw std::logic_error("Unknown type.");
        }

        for (auto& key : t.getAttributeKeys()) {
            const auto& value = t.getAttributeValue(key);
            auto protoAttr = protoType.add_attributes();
            protoAttr->set_key(key);
            protoAttr->set_value(value);
        }

        int pos = static_cast<int>(index);
        *footer.add_types() = protoType;

        for (uint64_t i = 0; i < t.getSubtypeCount(); ++i) {
            // only add subtypes' field names if this type is STRUCT
            if (t.getKind() == orc::STRUCT) {
                footer.mutable_types(pos)->add_fieldnames(t.getFieldName(i));
            }
            footer.mutable_types(pos)->add_subtypes(++index);
            buildFooterType(*t.getSubtype(i), footer, index);
        }
    }

    orc::proto::CompressionKind OmniWriterImpl::convertCompressionKind(
            const orc::CompressionKind& kind) {
        return static_cast<orc::proto::CompressionKind>(kind);
    }

    std::unique_ptr<OmniWriter> createOmniWriter(
            const orc::Type& type,
            orc::OutputStream* stream,
            const orc::WriterOptions& options) {
        return std::unique_ptr<OmniWriter>(
                new OmniWriterImpl(
                        type,
                        stream,
                        options));
    }

}