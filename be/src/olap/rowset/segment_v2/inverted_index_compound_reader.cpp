// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

#include "cloud/io/file_reader.h"
#include "cloud/io/file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"

#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <assert.h>

#ifdef LUCENE_FS_MMAP
#include "_MMapIndexInput.h"
#endif

#define CL_MAX_PATH 4096
//this is the max filename... for now its just the same,
//but this could change, so we use a different name
#define CL_MAX_NAME CL_MAX_PATH
//this used to be CL_MAX_NAME * 32, but as Alex Hudson points out, this could come to be 128kb.
//the above logic for CL_MAX_NAME should be correct enough to handle all file names
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

#define STRDUP_WtoA(x) CL_NS(util)::Misc::_wideToChar(x)
#define STRDUP_TtoA STRDUP_WtoA

using FileReaderPtr = std::unique_ptr<doris::io::FileReader>;
using FileWriterPtr = std::unique_ptr<doris::io::FileWriter>;

namespace doris {
namespace segment_v2 {

class DorisCompoundReader::ReaderFileEntry : LUCENE_BASE {
public:
    std::string file_name {};
    int64_t offset;
    int64_t length;
    ReaderFileEntry() {
        //file_name = nullptr;
        offset = 0;
        length = 0;
    }
    ~ReaderFileEntry() {}
};

/** Implementation of an IndexInput that reads from a portion of the
 *  compound file.
 */
class CSIndexInput : public lucene::store::BufferedIndexInput {
private:
    CL_NS(store)::IndexInput* base;
    int64_t fileOffset;
    int64_t _length;
    doris::Mutex _this_lock;

protected:
    /** Expert: implements buffer refill.  Reads uint8_ts from the current
    *  position in the input.
    * @param b the array to read uint8_ts into
    * @param length the number of uint8_ts to read
    */
    void readInternal(uint8_t* /*b*/, const int32_t /*len*/);
    void seekInternal(const int64_t /*pos*/) {}

public:
    CSIndexInput(CL_NS(store)::IndexInput* base, const int64_t fileOffset, const int64_t length,
                 const int32_t readBufferSize = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    CSIndexInput(const CSIndexInput& clone);
    ~CSIndexInput();

    /** Closes the stream to further operations. */
    void close();
    lucene::store::IndexInput* clone() const;

    int64_t length() const { return _length; }

    const char* getDirectoryType() const { return DorisCompoundReader::getClassName(); }
    const char* getObjectName() const { return getClassName(); }
    static const char* getClassName() { return "CSIndexInput"; }
};

CSIndexInput::CSIndexInput(CL_NS(store)::IndexInput* base, const int64_t fileOffset,
                           const int64_t length, const int32_t _readBufferSize)
        : BufferedIndexInput(_readBufferSize) {
    this->base = base;
    this->fileOffset = fileOffset;
    this->_length = length;
}

void CSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    // SCOPED_LOCK_MUTEX(base->THIS_LOCK)
    std::lock_guard<doris::Mutex> wlock(((DorisCompoundDirectory::FSIndexInput*)base)->_this_lock);

    int64_t start = getFilePointer();
    if (start + len > _length) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    base->seek(fileOffset + start);
    base->readBytes(b, len, false);
}

CSIndexInput::~CSIndexInput() {}

lucene::store::IndexInput* CSIndexInput::clone() const {
    return _CLNEW CSIndexInput(*this);
}

CSIndexInput::CSIndexInput(const CSIndexInput& clone) : BufferedIndexInput(clone) {
    this->base = clone.base; //no need to clone this..
    this->fileOffset = clone.fileOffset;
    this->_length = clone._length;
}

void CSIndexInput::close() {}

DorisCompoundReader::DorisCompoundReader(lucene::store::Directory* d, const char* name,
                                         int32_t readBufferSize)
        : readBufferSize(readBufferSize),
          dir(d),
          ram_dir(new lucene::store::RAMDirectory()),
          file_name(name),
          stream(nullptr),
          entries(_CLNEW EntriesType(true, true)) {
    bool success = false;
    try {
        stream = dir->openInput(name, readBufferSize);

        int32_t count = stream->readVInt();
        ReaderFileEntry* entry = NULL;
        TCHAR tid[CL_MAX_PATH];
        const int64_t buffer_length = 16384;
        uint8_t buffer[buffer_length];
        for (int32_t i = 0; i < count; i++) {
            entry = _CLNEW ReaderFileEntry();
            stream->readString(tid, CL_MAX_PATH);
            char* aid = STRDUP_TtoA(tid);
            entry->file_name = aid;
            entry->offset = stream->readLong();
            entry->length = stream->readLong();
            entries->put(aid, entry);
            // read header file data
            if (entry->offset < 0) {
                copyFile(entry->file_name.c_str(), entry->length, buffer, buffer_length);
            }
        }

        success = true;
    } catch (...) {
        if (!success && (stream != NULL)) {
            try {
                stream->close();
                _CLDELETE(stream);
            } catch (CLuceneError& err) {
                if (err.number() != CL_ERR_IO) throw err;
            }
        }
    }
}

void DorisCompoundReader::copyFile(const char* file, int64_t file_length, uint8_t* buffer,
                                   int64_t buffer_length) {
    std::unique_ptr<lucene::store::IndexOutput> output(ram_dir->createOutput(file));
    int64_t start_ptr = output->getFilePointer();
    int64_t remainder = file_length;
    int64_t chunk = buffer_length;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, file_length), remainder);
        stream->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, file_name.c_str(), (int)file_length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != file_length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)file_length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    output->close();
}

DorisCompoundReader::~DorisCompoundReader() {
    _CLDELETE(entries);
}

const char* DorisCompoundReader::getClassName() {
    return "DorisCompoundReader";
}
const char* DorisCompoundReader::getObjectName() const {
    return getClassName();
}

bool DorisCompoundReader::list(std::vector<std::string>* names) const {
    for (EntriesType::const_iterator i = entries->begin(); i != entries->end(); i++) {
        names->push_back(i->first);
    }
    return true;
}

bool DorisCompoundReader::fileExists(const char* name) const {
    return entries->exists((char*)name);
}

const char* DorisCompoundReader::getDirName() const {
    return directory.c_str();
}

lucene::store::Directory* DorisCompoundReader::getDirectory() {
    return dir;
}

int64_t DorisCompoundReader::fileModified(const char* name) const {
    return dir->fileModified(name);
}

int64_t DorisCompoundReader::fileLength(const char* name) const {
    ReaderFileEntry* e = entries->get((char*)name);
    if (e == NULL) {
        char buf[CL_MAX_PATH + 30];
        strcpy(buf, "File ");
        strncat(buf, name, CL_MAX_PATH);
        strcat(buf, " does not exist");
        _CLTHROWA(CL_ERR_IO, buf);
    }
    return e->length;
}

bool DorisCompoundReader::openInput(const char* name, lucene::store::IndexInput*& ret,
                                    CLuceneError& error, int32_t bufferSize) {
    if (stream == NULL) {
        error.set(CL_ERR_IO, "Stream closed");
        return false;
    }

    const ReaderFileEntry* entry = entries->get((char*)name);
    if (entry == NULL) {
        char buf[CL_MAX_PATH + 26];
        snprintf(buf, CL_MAX_PATH + 26, "No sub-file with id %s found", name);
        error.set(CL_ERR_IO, buf);
        return false;
    }

    // If file is in RAM, just return.
    if (ram_dir->fileExists(name)) {
        return ram_dir->openInput(name, ret, error, bufferSize);
    }

    if (bufferSize < 1) {
        bufferSize = readBufferSize;
    }

    //auto input_stream = dir->openInput(file_name, bufferSize);
    ret = _CLNEW CSIndexInput(stream, entry->offset, entry->length, bufferSize);
    return true;
}

void DorisCompoundReader::close() {
    // SCOPED_LOCK_MUTEX(THIS_LOCK);
    std::lock_guard<doris::Mutex> wlock(_this_lock);
    if (stream != NULL) {
        entries->clear();
        stream->close();
        _CLDELETE(stream);
    }
    ram_dir->close();
    dir->close();
    _CLDECDELETE(dir);
    _CLDELETE(ram_dir);
}

bool DorisCompoundReader::doDeleteFile(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::doDeleteFile");
}

void DorisCompoundReader::renameFile(const char* from, const char* to) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::renameFile");
}

void DorisCompoundReader::touchFile(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::touchFile");
}

lucene::store::IndexOutput* DorisCompoundReader::createOutput(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "UnsupportedOperationException: DorisCompoundReader::createOutput");
}

std::string DorisCompoundReader::toString() const {
    return std::string("DorisCompoundReader@") + this->directory + std::string("; file_name: ") +
           std::string(file_name);
}

} // namespace segment_v2
} // namespace doris
