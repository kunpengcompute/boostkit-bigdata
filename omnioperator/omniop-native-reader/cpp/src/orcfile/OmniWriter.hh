#ifndef NATIVE_READER_OMNIWRITER_HH
#define NATIVE_READER_OMNIWRITER_HH

#include "orc/Common.hh"
#include "orc/orc-config.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "vector/vector.h"
#include "OmniColumnWriter.hh"
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace omniruntime::writer {

    class OmniWriter {
    public:
        virtual ~OmniWriter();

        /**
         * Add a row batch into current writer.
         * @param rowsToAdd the row batch data to write.
         */
        virtual void add(omniruntime::vec::BaseVector* rowsToAdd, uint64_t startPos, uint64_t endPos) = 0;

        /**
         * Close the writer and flush any pending data to the output stream.
         */
        virtual void close() = 0;

        /**
         * Add user metadata to the writer.
         */
        virtual void addUserMetadata(const std::string name, const std::string value) = 0;
    };

    std::unique_ptr<OmniWriter> createOmniWriter(const orc::Type& type, orc::OutputStream* stream, const orc::WriterOptions& options);
}
#endif