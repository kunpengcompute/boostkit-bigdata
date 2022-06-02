/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#include "WriterOptions.hh"

#include "ColumnWriter.hh"

namespace spark {
  StreamsFactory::~StreamsFactory() {
    //PASS
  }

  class StreamsFactoryImpl : public StreamsFactory {
  public:
    StreamsFactoryImpl(
                       const WriterOptions& writerOptions,
                       OutputStream* outputStream) :
                       options(writerOptions),
                       outStream(outputStream) {
                       }

    virtual std::unique_ptr<BufferedOutputStream>
                    createStream() const override;
  private:
    const WriterOptions& options;
    OutputStream* outStream;
  };

  std::unique_ptr<BufferedOutputStream> StreamsFactoryImpl::createStream() const {
    return createCompressor(
                            options.getCompression(),
                            outStream,
                            options.getCompressionStrategy(),
                            // BufferedOutputStream initial capacity
                            1 * 1024 * 1024,
                            options.getCompressionBlockSize(),
                            *options.getMemoryPool());
  }

  std::unique_ptr<StreamsFactory> createStreamsFactory(
                                        const WriterOptions& options,
                                        OutputStream* outStream) {
    return std::unique_ptr<StreamsFactory>(
                                   new StreamsFactoryImpl(options, outStream));
  }
}