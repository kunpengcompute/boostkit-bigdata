/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#include "Adaptor.hh"
#include "SparkFile.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>

#ifdef _MSC_VER
#include <io.h>
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define stat _stat64
#define fstat _fstat64
#else
#include <unistd.h>
#define O_BINARY 0
#endif

namespace spark {

  class FileInputStream : public InputStream {
  private:
    std::string filename;
    int file;
    uint64_t totalLength;

  public:
    FileInputStream(std::string _filename) {
      filename = _filename;
      file = open(filename.c_str(), O_BINARY | O_RDONLY);
      if (file == -1) {
        throw std::runtime_error("Can't open " + filename);
      }
      struct stat fileStat;
      if (fstat(file, &fileStat) == -1) {
        throw std::runtime_error("Can't stat " + filename);
      }
      totalLength = static_cast<uint64_t>(fileStat.st_size);
    }

    ~FileInputStream() override;

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 128 * 1024;
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {
      if (!buf) {
        throw std::runtime_error("Buffer is null");
      }
      ssize_t bytesRead = pread(file, buf, length, static_cast<off_t>(offset));

      if (bytesRead == -1) {
        throw std::runtime_error("Bad read of " + filename);
      }
      if (static_cast<uint64_t>(bytesRead) != length) {
        throw std::runtime_error("Short read of " + filename);
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  };

  FileInputStream::~FileInputStream() {
    close(file);
  }

  std::unique_ptr<InputStream> readFile(const std::string& path) {
    return spark::readLocalFile(std::string(path));
  }

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }

  OutputStream::~OutputStream() {
    // PASS
  };

  class FileOutputStream : public OutputStream {
  private:
    std::string filename;
    int file;
    uint64_t bytesWritten;
    bool closed;

  public:
    FileOutputStream(std::string _filename) {
      bytesWritten = 0;
      filename = _filename;
      closed = false;
      file = open(
                  filename.c_str(),
                  O_BINARY | O_CREAT | O_WRONLY | O_TRUNC,
                  S_IRUSR | S_IWUSR);
      if (file == -1) {
        throw std::runtime_error("Can't open " + filename);
      }
    }

    ~FileOutputStream() override;

    uint64_t getLength() const override {
      return bytesWritten;
    }

    uint64_t getNaturalWriteSize() const override {
      return 128 * 1024;
    }

    void write(const void* buf, size_t length) override {
      if (closed) {
        throw std::logic_error("Cannot write to closed stream.");
      }
      ssize_t bytesWrite = ::write(file, buf, length);
      if (bytesWrite == -1) {
        throw std::runtime_error("Bad write of " + filename);
      }
      if (static_cast<uint64_t>(bytesWrite) != length) {
        throw std::runtime_error("Short write of " + filename);
      }
      bytesWritten += static_cast<uint64_t>(bytesWrite);
    }

    const std::string& getName() const override {
      return filename;
    }

    void close() override {
      if (!closed) {
        ::close(file);
        closed = true;
      }
    }
  };

  FileOutputStream::~FileOutputStream() {
    if (!closed) {
      ::close(file);
      closed = true;
    }
  }

  std::unique_ptr<OutputStream> writeLocalFile(const std::string& path) {
    return std::unique_ptr<OutputStream>(new FileOutputStream(path));
  }

  InputStream::~InputStream() {
    // PASS
  };
}