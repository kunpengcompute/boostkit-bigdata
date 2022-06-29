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