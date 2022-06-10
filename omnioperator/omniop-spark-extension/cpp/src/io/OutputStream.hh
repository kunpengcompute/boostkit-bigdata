/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#ifndef SPARK_OUTPUTSTREAM_HH
#define SPARK_OUTPUTSTREAM_HH

#include "SparkFile.hh"
#include "MemoryPool.hh"
#include "wrap/zero_copy_stream_wrapper.h"

namespace spark {

  /**
   * A subclass of Google's ZeroCopyOutputStream that supports output to memory
   * buffer, and flushing to OutputStream.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf writers.
   */
  class BufferedOutputStream: public google::protobuf::io::ZeroCopyOutputStream {
  private:
    OutputStream * outputStream;
    std::unique_ptr<DataBuffer<char> > dataBuffer;
    uint64_t blockSize;

  public:
    BufferedOutputStream(MemoryPool& pool,
                      OutputStream * outStream,
                      uint64_t capacity,
                      uint64_t block_size);
    virtual ~BufferedOutputStream() override;

    virtual bool Next(void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual bool WriteAliasedRaw(const void * data, int size) override;
    virtual bool AllowsAliasing() const override;

    virtual std::string getName() const;
    virtual uint64_t getSize() const;
    virtual uint64_t flush();
    virtual bool NextNBytes(void** data, int size);

    virtual bool isCompressed() const { return false; }
  };

}

#endif // SPARK_OUTPUTSTREAM_HH
