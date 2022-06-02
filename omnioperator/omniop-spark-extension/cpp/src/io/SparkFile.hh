/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#ifndef SPARK_FILE_HH
#define SPARK_FILE_HH

#include <string>
#include <memory>

namespace spark {

  /**
   * An abstract interface for providing readers a stream of bytes.
   */
  class InputStream {
  public:
    virtual ~InputStream();

    /**
     * Get the total length of the file in bytes.
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    virtual uint64_t getNaturalReadSize() const = 0;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    virtual void read(void* buf,
                      uint64_t length,
                      uint64_t offset) = 0;

    /**
     * Get the name of the stream for error messages.
     */
    virtual const std::string& getName() const = 0;
  };

  /**
   * An abstract interface for providing writer a stream of bytes.
   */
  class OutputStream {
  public:
    virtual ~OutputStream();

    /**
     * Get the total length of bytes written.
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be written at once
     */
    virtual uint64_t getNaturalWriteSize() const =0;

    /**
     * Write/Append length bytes pointed by buf to the file stream
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to write.
     */
    virtual void write(const void* buf, size_t length) = 0;

    /**
     * Get the name of the stream for error messages.
     */
    virtual const std::string& getName() const = 0;

    /**
     * Close the stream and flush any pending data to the disk.
     */
    virtual void close() = 0;
  };

  /**
   * Create a stream to a local file
   * @param path the name of the file in the local file system
   */
  std::unique_ptr<InputStream> readFile(const std::string& path);

  /**
   * Create a stream to a local file.
   * @param path the name of the file in the local file system
   */
  std::unique_ptr<InputStream> readLocalFile(const std::string& path);

  /**
   * Create a stream to write to a local file.
   * @param path the name of the file in the local file system
   */
  std::unique_ptr<OutputStream> writeLocalFile(const std::string& path);
}

#endif