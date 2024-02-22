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

#include "OrcFileOverride.hh"
#include <sys/types.h>
#include <memory>

#include "filesystem/hdfs_file.h"
#include "filesystem/io_exception.h"

namespace orc {

    using namespace fs;

    class HdfsFileInputStreamOverride : public InputStream {
    private:
        std::string filename_;
        std::unique_ptr <fs::ReadableFile> hdfs_file_;
        uint64_t total_length_;
        const uint64_t READ_SIZE_ = 1024 * 1024; //1 MB

    public:
        HdfsFileInputStreamOverride(const UriInfo &uri, bool notNeedFSCache) {
            this->filename_ = uri.Path();
            std::shared_ptr <HadoopFileSystem> fileSystemPtr = getHdfsFileSystem(uri.Host(), uri.Port(), notNeedFSCache);
            this->hdfs_file_ = std::make_unique<HdfsReadableFile>(fileSystemPtr, this->filename_, 0);

            Status openFileSt = hdfs_file_->OpenFile();
            if (!openFileSt.IsOk()) {
                throw IOException(openFileSt.ToString());
            }

            this->total_length_ = hdfs_file_->GetFileSize();
        }

        ~HdfsFileInputStreamOverride() override {
        }

        /**
         * get the total length of the file in bytes
         */
        uint64_t getLength() const override {
            return total_length_;
        }


        /**
         * get the natural size of reads
         */
        uint64_t getNaturalReadSize() const override {
            return READ_SIZE_;
        }

        /**
         * read length bytes from the file starting at offset into the buffer starting at buf
         * @param buf buffer save data
         * @param length the number of bytes to read
         * @param offset read from
         */
        void read(void *buf,
                  uint64_t length,
                  uint64_t offset) override {

            if (!buf) {
                throw IOException(Status::IOError("Fail to read hdfs file, because read buffer is null").ToString());
            }

            char *buf_ptr = reinterpret_cast<char *>(buf);
            uint64_t total_bytes_read = 0;
            uint64_t last_bytes_read = 0;

            do {
                last_bytes_read = hdfs_file_->ReadAt(buf_ptr, length - total_bytes_read, offset + total_bytes_read);
                if (last_bytes_read < 0) {
                    throw IOException(Status::IOError("Error reading bytes the file").ToString());
                }
                if (last_bytes_read == 0) {
                    break;
                }
                total_bytes_read += last_bytes_read;
                buf_ptr += last_bytes_read;
            } while (total_bytes_read < length);
        }

        const std::string &getName() const override {
            return filename_;
        }
    };

    std::unique_ptr <InputStream> createHdfsFileInputStream(const UriInfo &uri, bool notNeedFSCache) {
        return std::unique_ptr<InputStream>(new HdfsFileInputStreamOverride(uri, notNeedFSCache));
    }
}
