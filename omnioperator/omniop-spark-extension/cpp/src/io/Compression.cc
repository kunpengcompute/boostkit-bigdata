/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#include "Adaptor.hh"
#include "Compression.hh"
#include "lz4.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "zlib.h"
#include "zstd.h"

#include "wrap/snappy_wrapper.h"

#ifndef ZSTD_CLEVEL_DEFAULT
#define ZSTD_CLEVEL_DEFAULT 3
#endif

/* These macros are defined in lz4.c */
#ifndef LZ4_ACCELERATION_DEFAULT
#define LZ4_ACCELERATION_DEFAULT 1
#endif

#ifndef LZ4_ACCELERATION_MAX
#define LZ4_ACCELERATION_MAX 65537
#endif

namespace spark {

  class CompressionStreamBase: public BufferedOutputStream {
  public:
    CompressionStreamBase(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool);

    virtual bool Next(void** data, int*size) override = 0;
    virtual void BackUp(int count) override;

    virtual std::string getName() const override = 0;
    virtual uint64_t flush() override;

    virtual bool isCompressed() const override { return true; }
    virtual uint64_t getSize() const override;

  protected:
    void writerHeader(char * buffer, size_t compressedSize, bool original) {
      buffer[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
      buffer[1] = static_cast<char>(compressedSize >> 7);
      buffer[2] = static_cast<char>(compressedSize >> 15);
    }

    // ensure enough room for compression block header
    void ensureHeader();

    // Buffer to hold uncompressed data until user calls Next()
    DataBuffer<unsigned char> rawInputBuffer;

    // Compress level
    int level;

    // Compressed data output buffer
    char * outputBuffer;

    // Size for compressionBuffer
    int bufferSize;

    // Compress output position
    int outputPosition;

    // Compress output buffer size
    int outputSize;
  };

  CompressionStreamBase::CompressionStreamBase(OutputStream * outStream,
                                               int compressionLevel,
                                               uint64_t capacity,
                                               uint64_t blockSize,
                                               MemoryPool& pool) :
                                                BufferedOutputStream(pool,
                                                                     outStream,
                                                                     capacity,
                                                                     blockSize),
                                                rawInputBuffer(pool, blockSize),
                                                level(compressionLevel),
                                                outputBuffer(nullptr),
                                                bufferSize(0),
                                                outputPosition(0),
                                                outputSize(0) {
  // PASS
  }

  void CompressionStreamBase::BackUp(int count) {
    if (count > bufferSize) {
      throw std::logic_error("Can't backup that much!");
    }
    bufferSize -= count;
  }

  uint64_t CompressionStreamBase::flush() {
    void * data;
    int size;
    if (!Next(&data, &size)) {
      throw std::runtime_error("Failed to flush compression buffer.");
    }
    BufferedOutputStream::BackUp(outputSize - outputPosition);
    bufferSize = outputSize = outputPosition = 0;
    return BufferedOutputStream::flush();
  }

  uint64_t CompressionStreamBase::getSize() const {
    return BufferedOutputStream::getSize() -
           static_cast<uint64_t>(outputSize - outputPosition);
  }

  void CompressionStreamBase::ensureHeader() {
    // adjust 3 bytes for the compression header
    if (outputPosition + 3 >= outputSize) {
      int newPosition = outputPosition + 3 - outputSize;
      if (!BufferedOutputStream::Next(
        reinterpret_cast<void **>(&outputBuffer),
        &outputSize)) {
        throw std::runtime_error(
          "Failed to get next output buffer from output stream.");
      }
      outputPosition = newPosition;
    } else {
      outputPosition += 3;
    }
  }

  /**
   * Streaming compression base class
   */
  class CompressionStream: public CompressionStreamBase {
  public:
    CompressionStream(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool);

    virtual bool Next(void** data, int*size) override;
    virtual std::string getName() const override = 0;

  protected:
    // return total compressed size
    virtual uint64_t doStreamingCompression() = 0;
  };

  CompressionStream::CompressionStream(OutputStream * outStream,
                                       int compressionLevel,
                                       uint64_t capacity,
                                       uint64_t blockSize,
                                       MemoryPool& pool) :
                                         CompressionStreamBase(outStream,
                                                               compressionLevel,
                                                               capacity,
                                                               blockSize,
                                                               pool) {
    // PASS
  }

  bool CompressionStream::Next(void** data, int*size) {
    if (bufferSize != 0) {
      ensureHeader();

      uint64_t totalCompressedSize = doStreamingCompression();

      char * header = outputBuffer + outputPosition - totalCompressedSize - 3;
      if (totalCompressedSize >= static_cast<unsigned long>(bufferSize)) {
        writeHeader(header, static_cast<size_t>(bufferSize), true);
        memcpy(
          header + 3,
          rawInputBuffer.data(),
          static_cast<size_t>(bufferSize));

        int backup = static_cast<int>(totalCompressedSize) - bufferSize;
        BufferedOutputStream::BackUp(backup);
        outputPosition -= backup;
        outputSize -= backup;
      } else {
        writeHeader(header, totalCompressedSize, false);
      }
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;

    return true;
  }

  class ZlibCompressionStream: public CompressionStream {
  public:
    ZlibCompressionStream(OutputStream * outStream,
                             int compressionLevel,
                             uint64_t capacity,
                             uint64_t blockSize,
                             MemoryPool& pool);

    virtual ~ZlibCompressionStream() override {
      end();
    }

    virtual std::string getName() const override;

  protected:
    virtual uint64_t doStreamingCompression() override;

  private:
    void init();
    void end();
    z_stream strm;
  };

  ZlibCompressionStream::ZlibCompressionStream(
                        OutputStream * outStream,
                        int compressionLevel,
                        uint64_t capacity,
                        uint64_t blockSize,
                        MemoryPool& pool)
                        : CompressionStream(outStream,
                                            compressionLevel,
                                            capacity,
                                            blockSize,
                                            pool) {
    init();
  }

  uint64_t ZlibCompressionStream::doStreamingCompression() {
    if (deflateReset(&strm) != Z_OK) {
      throw std::runtime_error("Failed to reset inflate.");
    }

    strm.avail_in = static_cast<unsigned int>(bufferSize);
    strm.next_in = rawInputBuffer.data();

    do {
      if (outputPosition >= outputSize) {
        if (!BufferedOutputStream::Next(
          reinterpret_cast<void **>(&outputBuffer),
          &outputSize)) {
          throw std::runtime_error(
            "Failed to get next output buffer from output stream.");
        }
        outputPosition = 0;
      }
      strm.next_out = reinterpret_cast<unsigned char *>
      (outputBuffer + outputPosition);
      strm.avail_out = static_cast<unsigned int>
      (outputSize - outputPosition);

      int ret = deflate(&strm, Z_FINISH);
      outputPosition = outputSize - static_cast<int>(strm.avail_out);

      if (ret == Z_STREAM_END) {
        break;
      } else if (ret == Z_OK) {
        // needs more buffer so will continue the loop
      } else {
        throw std::runtime_error("Failed to deflate input data.");
      }
    } while (strm.avail_out == 0);

    return strm.total_out;
  }

  std::string ZlibCompressionStream::getName() const {
    return "ZlibCompressionStream";
  }

// DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZlibCompressionStream::init() {
    strm.zalloc = nullptr;
    strm.zfree = nullptr;
    strm.opaque = nullptr;
    strm.next_in = nullptr;

    if (deflateInit2(&strm, level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY)
        != Z_OK) {
      throw std::runtime_error("Error while calling deflateInit2() for zlib.");
    }
  }

  void ZlibCompressionStream::end() {
    (void)deflateEnd(&strm);
  }

// DIAGNOSTIC_PUSH

  enum DecompressState { DECOMPRESS_HEADER,
                         DECOMPRESS_START,
                         DECOMPRESS_CONTINUE,
                         DECOMPRESS_ORIGINAL,
                         DECOMPRESS_EOF};

// DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  /**
   * Block compression base class
   */
  class BlockCompressionStream: public CompressionStreamBase {
  public:
    BlockCompressionStream(OutputStream * outStream,
                           int compressionLevel,
                           uint64_t capacity,
                           uint64_t blockSize,
                           MemoryPool& pool)
                           : CompressionStreamBase(outStream,
                                                   compressionLevel,
                                                   capacity,
                                                   blockSize,
                                                   pool)
                           , compressorBuffer(pool) {
      // PASS
    }

    virtual bool Next(void** data, int*size) override;
    virtual std::string getName() const override = 0;

  protected:
    // compresses a block and returns the compressed size
    virtual uint64_t doBlockCompression() = 0;

    // return maximum possible compression size for allocating space for
    // compressorBuffer below
    virtual uint64_t estimateMaxCompressionSize() = 0;

    // should allocate max possible compressed size
    DataBuffer<unsigned char> compressorBuffer;
  };

  bool BlockCompressionStream::Next(void** data, int*size) {
    if (bufferSize != 0) {
      ensureHeader();

      // perform compression
      size_t totalCompressedSize = doBlockCompression();

      const unsigned char * dataToWrite = nullptr;
      int totalSizeToWrite = 0;
      char * header = outputBuffer + outputPosition - 3;

      if (totalCompressedSize >= static_cast<size_t>(bufferSize)) {
        writeHeader(header, static_cast<size_t>(bufferSize), true);
        dataToWrite = rawInputBuffer.data();
        totalSizeToWrite = bufferSize;
      } else {
        writeHeader(header, totalCompressedSize, false);
        dataToWrite = compressorBuffer.data();
        totalSizeToWrite = static_cast<int>(totalCompressedSize);
      }

      char * dst = header + 3;
      while (totalSizeToWrite > 0) {
        if (outputPosition == outputSize) {
          if (!BufferedOutputStream::Next(reinterpret_cast<void **>(&outputBuffer),
                                          &outputSize)) {
            throw std::logic_error(
              "Failed to get next output buffer from output stream.");
          }
          outputPosition = 0;
          dst = outputBuffer;
        } else if (outputPosition > outputSize) {
          // this will unlikely happen, but we have seen a few on zstd v1.1.0
          throw std::logic_error("Write to an out-of-bound place!");
        }

        int sizeToWrite = std::min(totalSizeToWrite, outputSize - outputPosition);
        memcpy(dst, dataToWrite, static_cast<size_t>(sizeToWrite));

        outputPosition += sizeToWrite;
        dataToWrite += sizeToWrite;
        totalSizeToWrite -= sizeToWrite;
        dst += sizeToWrite;
      }
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;
    compressorBuffer.resize(estimateMaxCompressionSize());

    return true;
  }

  /**
   * LZ4 block compression
   */
  class Lz4CompressionSteam: public BlockCompressionStream {
  public:
    Lz4CompressionSteam(OutputStream * outStream,
                        int compressionLevel,
                        uint64_t capacity,
                        uint64_t blockSize,
                        MemoryPool& pool)
                        : BlockCompressionStream(outStream,
                                                 compressionLevel,
                                                 capacity,
                                                 blockSize,
                                                 pool) {
      this->init();
    }

    virtual std::string getName() const override {
      return "Lz4CompressionStream";
    }

    virtual ~Lz4CompressionSteam() override {
      this->end();
    }

  protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return static_cast<uint64_t>(LZ4_compressBound(bufferSize));
    }

  private:
    void init();
    void end();
    LZ4_stream_t *state;
  };

  uint64_t Lz4CompressionSteam::doBlockCompression() {
    int result = LZ4_compress_fast_extState(static_cast<void*>(state),
                                            reinterpret_cast<const char*>(rawInputBuffer.data()),
                                            reinterpret_cast<char*>(compressorBuffer.data()),
                                            bufferSize,
                                            static_cast<int>(compressorBuffer.size()),
                                            level);
    if (result == 0) {
      throw std::runtime_error("Error during block compression using lz4.");
    }
    return static_cast<uint64_t>(result);
  }

  void Lz4CompressionSteam::init() {
    state = LZ4_createStream();
    if (!state) {
      throw std::runtime_error("Error while allocating state for lz4.");
    }
  }

  void Lz4CompressionSteam::end() {
    (void)LZ4_freeStream(state);
    state = nullptr;
  }

  /**
   * Snappy block compression
   */
  class SnappyCompressionStream: public BlockCompressionStream {
  public:
    SnappyCompressionStream(OutputStream * outStream,
                            int compressionLevel,
                            uint64_t capacity,
                            uint64_t blockSize,
                            MemoryPool& pool)
                            : BlockCompressionStream(outStream,
                                                     compressionLevel,
                                                     capacity,
                                                     blockSize,
                                                     pool) {
    }

    virtual std::string getName() const override {
      return "SnappyCompressionStream";
    }

    virtual ~SnappyCompressionStream() override {
      // PASS
    }

  protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return static_cast<uint64_t>
        (snappy::MaxCompressedLength(static_cast<size_t>(bufferSize)));
    }
  };

  uint64_t SnappyCompressionStream::doBlockCompression() {
    size_t compressedLength;
    snappy::RawCompress(reinterpret_cast<const char*>(rawInputBuffer.data()),
                        static_cast<size_t>(bufferSize),
                        reinterpret_cast<char*>(compressorBuffer.data()),
                        &compressedLength);
    return static_cast<uint64_t>(compressedLength);
  }

  /**
   * ZSTD block compression
   */
  class ZSTDCompressionStream: public BlockCompressionStream{
  public:
    ZSTDCompressionStream(OutputStream * outStream,
                          int compressionLevel,
                          uint64_t capacity,
                          uint64_t blockSize,
                          MemoryPool& pool)
                          : BlockCompressionStream(outStream,
                                                   compressionLevel,
                                                   capacity,
                                                   blockSize,
                                                   pool) {
    this->init();
  }

  virtual std::string getName() const override {
    return "ZstdCompressionStream";
  }

  virtual ~ZSTDCompressionStream() override {
    this->end();
  }

  protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return ZSTD_compressBound(static_cast<size_t>(bufferSize));
    }

  private:
    void init();
    void end();
    ZSTD_CCtx *cctx;
  };

  uint64_t ZSTDCompressionStream::doBlockCompression() {
    return ZSTD_compressCCtx(cctx,
                             compressorBuffer.data(),
                             compressorBuffer.size(),
                             rawInputBuffer.data(),
                             static_cast<size_t>(bufferSize),
                             level);
  }

// DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZSTDCompressionStream::init() {

    cctx = ZSTD_createCCtx();
    if (!cctx) {
      throw std::runtime_error("Error while calling ZSTD_createCCtx() for zstd.");
    }
  }


  void ZSTDCompressionStream::end() {
    (void)ZSTD_freeCCtx(cctx);
    cctx = nullptr;
  }

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

// DIAGNOSTIC_PUSH

  std::unique_ptr<BufferedOutputStream>
     createCompressor(
                      CompressionKind kind,
                      OutputStream * outStream,
                      CompressionStrategy strategy,
                      uint64_t bufferCapacity,
                      uint64_t compressionBlockSize,
                      MemoryPool& pool) {
    switch (static_cast<int64_t>(kind)) {
    case CompressionKind_NONE: {
      return std::unique_ptr<BufferedOutputStream>
        (new BufferedOutputStream(
                pool, outStream, bufferCapacity, compressionBlockSize));
    }
    case CompressionKind_ZLIB: {
    int level = (strategy == CompressionStrategy_SPEED) ?
            Z_BEST_SPEED + 1 : Z_DEFAULT_COMPRESSION;
    return std::unique_ptr<BufferedOutputStream>
      (new ZlibCompressionStream(
              outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_ZSTD: {
      int level = (strategy == CompressionStrategy_SPEED) ?
              1 : ZSTD_CLEVEL_DEFAULT;
      return std::unique_ptr<BufferedOutputStream>
        (new ZSTDCompressionStream(
          outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_LZ4: {
      int level = (strategy == CompressionStrategy_SPEED) ?
                  LZ4_ACCELERATION_MAX : LZ4_ACCELERATION_DEFAULT;
      return std::unique_ptr<BufferedOutputStream>
        (new Lz4CompressionSteam(
          outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_SNAPPY: {
      int level = 0;
      return std::unique_ptr<BufferedOutputStream>
        (new SnappyCompressionStream(
          outStream, level, bufferCapacity, compressionBlockSize, pool));
    }
    case CompressionKind_LZO:
    default:
      throw std::logic_error("compression codec not supported");
    }
  }

}